mod moderation;

use axum::{
    extract::State,
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
    Json, Router,
};
use futures::stream::{self, StreamExt};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use thiserror::Error;
use tokio::sync::Semaphore;
use tracing::{info, warn};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

use moderation::{ModerationResult, ViolationType};

#[derive(Debug, Error)]
enum AppError {
    #[error("无效请求: {0}")]
    BadRequest(String),
    #[error("内部服务器错误")]
    InternalError,
}

impl IntoResponse for AppError {
    fn into_response(self) -> axum::response::Response {
        let (status, message) = match self {
            AppError::BadRequest(msg) => (StatusCode::BAD_REQUEST, msg),
            AppError::InternalError => (StatusCode::INTERNAL_SERVER_ERROR, self.to_string()),
        };

        let body = Json(ApiResponse::<()> {
            code: status.as_u16(),
            message,
            data: None,
        });

        (status, body).into_response()
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct ApiResponse<T> {
    code: u16,
    message: String,
    data: Option<T>,
}

impl<T> ApiResponse<T> {
    fn success(data: T) -> Self {
        ApiResponse {
            code: 200,
            message: "success".to_string(),
            data: Some(data),
        }
    }

    fn error(code: u16, message: String) -> Self {
        ApiResponse {
            code,
            message,
            data: None,
        }
    }
}

#[derive(Debug, Deserialize)]
struct GroupChatMessage {
    group_id: String,
    user_id: String,
    user_name: Option<String>,
    message_id: Option<String>,
    content: String,
    message_type: Option<String>,
    timestamp: Option<i64>,
}

#[derive(Debug, Serialize)]
struct ModerationResponse {
    group_id: String,
    user_id: String,
    message_id: Option<String>,
    is_violation: bool,
    violations: Vec<ViolationInfo>,
    risk_score: u8,
    suggestion: String,
    processed_at: i64,
}

#[derive(Debug, Serialize)]
struct ViolationInfo {
    violation_type: String,
    violation_type_name: String,
    description: String,
    matched_text: Option<String>,
    severity: String,
    severity_name: String,
}

#[derive(Debug, Deserialize)]
struct BatchModerationRequest {
    messages: Vec<GroupChatMessage>,
}

#[derive(Debug, Serialize)]
struct BatchModerationResponse {
    results: Vec<ModerationResponse>,
    total: usize,
    violation_count: usize,
    duration_ms: u64,
}

#[derive(Debug, Serialize)]
struct HealthResponse {
    status: String,
    version: String,
    timestamp: i64,
}

struct AppState {
    version: String,
    max_concurrency: usize,
    chunk_size: usize,
}

fn get_current_timestamp() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64
}

fn to_violation_info(result: &moderation::ModerationResult) -> Vec<ViolationInfo> {
    result
        .violations
        .iter()
        .map(|v| ViolationInfo {
            violation_type: format!("{:?}", v.violation_type).to_lowercase(),
            violation_type_name: v.violation_type.to_string(),
            description: v.description.clone(),
            matched_text: v.matched_text.clone(),
            severity: format!("{:?}", v.severity).to_lowercase(),
            severity_name: v.severity.to_string(),
        })
        .collect()
}

fn build_moderation_response(msg: &GroupChatMessage, result: &ModerationResult) -> ModerationResponse {
    ModerationResponse {
        group_id: msg.group_id.clone(),
        user_id: msg.user_id.clone(),
        message_id: msg.message_id.clone(),
        is_violation: result.is_violation,
        violations: to_violation_info(result),
        risk_score: result.risk_score,
        suggestion: format!("{:?}", result.suggestion).to_lowercase(),
        processed_at: get_current_timestamp(),
    }
}

async fn health_check(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let response = HealthResponse {
        status: "ok".to_string(),
        version: state.version.clone(),
        timestamp: get_current_timestamp(),
    };
    Json(ApiResponse::success(response))
}

async fn moderate_single(
    Json(msg): Json<GroupChatMessage>,
) -> Result<impl IntoResponse, AppError> {
    if msg.content.is_empty() {
        return Err(AppError::BadRequest("消息内容不能为空".to_string()));
    }
    if msg.group_id.is_empty() {
        return Err(AppError::BadRequest("群ID不能为空".to_string()));
    }
    if msg.user_id.is_empty() {
        return Err(AppError::BadRequest("用户ID不能为空".to_string()));
    }

    info!(
        "收到群聊消息 - 群ID: {}, 用户: {}, 内容长度: {}",
        msg.group_id,
        msg.user_id,
        msg.content.len()
    );

    let result = moderation::moderate_message(&msg.content);

    if result.is_violation {
        warn!(
            "检测到违规消息 - 群ID: {}, 用户: {}, 风险分: {}, 建议: {:?}",
            msg.group_id, msg.user_id, result.risk_score, result.suggestion
        );
    }

    let response = build_moderation_response(&msg, &result);
    Ok(Json(ApiResponse::success(response)))
}

async fn moderate_batch(
    State(state): State<Arc<AppState>>,
    Json(req): Json<BatchModerationRequest>,
) -> Result<impl IntoResponse, AppError> {
    if req.messages.is_empty() {
        return Err(AppError::BadRequest("消息列表不能为空".to_string()));
    }
    if req.messages.len() > 5000 {
        return Err(AppError::BadRequest("单次最多审核5000条消息".to_string()));
    }

    let start = std::time::Instant::now();
    let total_messages = req.messages.len();
    info!("批量审核开始 - 消息数量: {}, 并发度: {}, 分块: {}",
          total_messages, state.max_concurrency, state.chunk_size);

    let semaphore = Arc::new(Semaphore::new(state.max_concurrency));
    let chunk_size = state.chunk_size;

    let messages: Vec<GroupChatMessage> = req.messages
        .into_iter()
        .filter(|m| !m.content.is_empty())
        .collect();

    let valid_count = messages.len();

    let chunks: Vec<Vec<GroupChatMessage>> = messages
        .chunks(chunk_size)
        .map(|c| c.to_vec())
        .collect();

    info!("分块数量: {}, 每块约 {} 条", chunks.len(), chunk_size);

    let semaphore_clone = semaphore.clone();
    let mut result_stream = stream::iter(chunks.into_iter().enumerate())
        .map(|(chunk_idx, chunk)| {
            let permit = semaphore_clone.clone().acquire_owned();
            async move {
                let _permit = permit.await.expect("信号量已关闭");
                let mut chunk_results = Vec::with_capacity(chunk.len());
                for (offset, msg) in chunk.iter().enumerate() {
                    let result = moderation::moderate_message(&msg.content);
                    let global_idx = chunk_idx * chunk_size + offset;
                    chunk_results.push((global_idx, build_moderation_response(msg, &result), result.is_violation));
                }
                chunk_results
            }
        })
        .buffer_unordered(state.max_concurrency);

    let mut indexed_results: Vec<(usize, ModerationResponse, bool)> = Vec::with_capacity(valid_count);
    while let Some(chunk_results) = result_stream.next().await {
        indexed_results.extend(chunk_results);
    }

    indexed_results.sort_by_key(|(idx, _, _)| *idx);

    let mut violation_count = 0;
    let results: Vec<ModerationResponse> = indexed_results
        .into_iter()
        .map(|(_, resp, is_v)| {
            if is_v {
                violation_count += 1;
            }
            resp
        })
        .collect();

    let duration_ms = start.elapsed().as_millis() as u64;

    info!(
        "批量审核完成 - 总数: {}, 有效: {}, 违规: {}, 耗时: {}ms, 平均: {:.2}ms/条",
        total_messages,
        valid_count,
        violation_count,
        duration_ms,
        if valid_count > 0 { duration_ms as f64 / valid_count as f64 } else { 0.0 }
    );

    let response = BatchModerationResponse {
        total: results.len(),
        violation_count,
        results,
        duration_ms,
    };

    Ok(Json(ApiResponse::success(response)))
}

async fn violation_types() -> impl IntoResponse {
    let types = vec![
        serde_json::json!({
            "type": "pornography",
            "name": "色情低俗",
            "description": "包含色情、淫秽、低俗内容"
        }),
        serde_json::json!({
            "type": "violence",
            "name": "暴力血腥",
            "description": "包含暴力、血腥、恐怖内容"
        }),
        serde_json::json!({
            "type": "politics",
            "name": "政治敏感",
            "description": "包含政治敏感内容"
        }),
        serde_json::json!({
            "type": "advertising",
            "name": "广告推广",
            "description": "包含广告、推广、营销内容"
        }),
        serde_json::json!({
            "type": "abuse",
            "name": "辱骂攻击",
            "description": "包含辱骂、人身攻击内容"
        }),
        serde_json::json!({
            "type": "gambling",
            "name": "赌博相关",
            "description": "包含赌博、博彩相关内容"
        }),
        serde_json::json!({
            "type": "fraud",
            "name": "诈骗欺诈",
            "description": "包含诈骗、欺诈、虚假信息"
        }),
        serde_json::json!({
            "type": "other",
            "name": "其他违规",
            "description": "其他违规内容"
        }),
    ];

    Json(ApiResponse::success(types))
}

fn create_router(state: Arc<AppState>) -> Router {
    Router::new()
        .route("/health", get(health_check))
        .route("/api/v1/moderate", post(moderate_single))
        .route("/api/v1/moderate/batch", post(moderate_batch))
        .route("/api/v1/violation-types", get(violation_types))
        .with_state(state)
}

#[tokio::main]
async fn main() {
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "group_chat_moderation=info,tower_http=info,axum=info".into()),
        )
        .with(tracing_subscriber::fmt::layer())
        .init();

    let cpu_cores = num_cpus::get();
    let max_concurrency = std::env::var("MOD_MAX_CONCURRENCY")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or_else(|| (cpu_cores * 8).max(16).min(256));

    let chunk_size = std::env::var("MOD_CHUNK_SIZE")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or_else(|| 32);

    info!("系统配置 - CPU核心: {}, 最大并发: {}, 分块大小: {}", cpu_cores, max_concurrency, chunk_size);

    let state = Arc::new(AppState {
        version: "0.1.0".to_string(),
        max_concurrency,
        chunk_size,
    });

    let app = create_router(state);

    let listener = tokio::net::TcpListener::bind("0.0.0.0:3000")
        .await
        .expect("无法绑定端口");

    info!("群聊消息审核服务启动，监听端口: 3000");
    info!("健康检查: http://localhost:3000/health");
    info!("审核接口: POST http://localhost:3000/api/v1/moderate");
    info!("批量审核: POST http://localhost:3000/api/v1/moderate/batch (支持最多5000条/次，并发处理)");

    axum::serve(listener, app)
        .await
        .expect("服务启动失败");
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{
        body::Body,
        http::{self, Request, StatusCode},
    };
    use http_body_util::BodyExt;
    use tower::ServiceExt;

    fn test_state() -> Arc<AppState> {
        Arc::new(AppState {
            version: "test".to_string(),
            max_concurrency: 8,
            chunk_size: 16,
        })
    }

    #[tokio::test]
    async fn test_health_check() {
        let app = create_router(test_state());

        let response = app
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/health")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);

        let body = response.into_body().collect().await.unwrap().to_bytes();
        let body: ApiResponse<HealthResponse> = serde_json::from_slice(&body).unwrap();
        assert_eq!(body.code, 200);
        assert_eq!(body.data.unwrap().status, "ok");
    }

    #[tokio::test]
    async fn test_moderate_clean_message() {
        let app = create_router(test_state());

        let msg = GroupChatMessage {
            group_id: "group1".to_string(),
            user_id: "user1".to_string(),
            user_name: Some("测试用户".to_string()),
            message_id: Some("msg001".to_string()),
            content: "你好，今天天气真好".to_string(),
            message_type: Some("text".to_string()),
            timestamp: Some(1234567890),
        };

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/moderate")
                    .header(http::header::CONTENT_TYPE, "application/json")
                    .body(Body::from(serde_json::to_string(&msg).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);

        let body = response.into_body().collect().await.unwrap().to_bytes();
        let body: ApiResponse<ModerationResponse> = serde_json::from_slice(&body).unwrap();
        assert_eq!(body.code, 200);
        let data = body.data.unwrap();
        assert!(!data.is_violation);
        assert_eq!(data.risk_score, 0);
        assert_eq!(data.suggestion, "pass");
    }

    #[tokio::test]
    async fn test_moderate_violation_message() {
        let app = create_router(test_state());

        let msg = GroupChatMessage {
            group_id: "group1".to_string(),
            user_id: "user1".to_string(),
            user_name: None,
            message_id: None,
            content: "加微信abc123免费领取".to_string(),
            message_type: None,
            timestamp: None,
        };

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/moderate")
                    .header(http::header::CONTENT_TYPE, "application/json")
                    .body(Body::from(serde_json::to_string(&msg).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);

        let body = response.into_body().collect().await.unwrap().to_bytes();
        let body: ApiResponse<ModerationResponse> = serde_json::from_slice(&body).unwrap();
        let data = body.data.unwrap();
        assert!(data.is_violation);
        assert!(data.risk_score > 0);
    }

    #[tokio::test]
    async fn test_moderate_empty_content() {
        let app = create_router(test_state());

        let msg = GroupChatMessage {
            group_id: "group1".to_string(),
            user_id: "user1".to_string(),
            user_name: None,
            message_id: None,
            content: "".to_string(),
            message_type: None,
            timestamp: None,
        };

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/moderate")
                    .header(http::header::CONTENT_TYPE, "application/json")
                    .body(Body::from(serde_json::to_string(&msg).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn test_batch_moderation_concurrent() {
        let app = create_router(test_state());

        let mut messages = Vec::new();
        for i in 0..100 {
            let content = if i % 5 == 0 {
                format!("加微信user{}免费领取礼品", i)
            } else if i % 7 == 0 {
                format!("傻逼你好{}", i)
            } else {
                format!("正常消息内容 {}", i)
            };
            messages.push(GroupChatMessage {
                group_id: format!("group_{}", i % 3),
                user_id: format!("user_{}", i),
                user_name: None,
                message_id: Some(format!("msg_{}", i)),
                content,
                message_type: None,
                timestamp: None,
            });
        }

        let req = BatchModerationRequest { messages };

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/moderate/batch")
                    .header(http::header::CONTENT_TYPE, "application/json")
                    .body(Body::from(serde_json::to_string(&req).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);

        let body = response.into_body().collect().await.unwrap().to_bytes();
        let body: ApiResponse<BatchModerationResponse> = serde_json::from_slice(&body).unwrap();
        assert_eq!(body.code, 200);
        let data = body.data.unwrap();
        assert_eq!(data.total, 100);
        assert!(data.violation_count > 0);
        assert!(data.duration_ms < 5000);
    }

    #[tokio::test]
    async fn test_batch_moderation_empty_list() {
        let app = create_router(test_state());

        let req = BatchModerationRequest { messages: vec![] };

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/moderate/batch")
                    .header(http::header::CONTENT_TYPE, "application/json")
                    .body(Body::from(serde_json::to_string(&req).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn test_batch_moderation_skips_empty_content() {
        let app = create_router(test_state());

        let messages = vec![
            GroupChatMessage {
                group_id: "g1".to_string(),
                user_id: "u1".to_string(),
                user_name: None,
                message_id: None,
                content: "".to_string(),
                message_type: None,
                timestamp: None,
            },
            GroupChatMessage {
                group_id: "g1".to_string(),
                user_id: "u2".to_string(),
                user_name: None,
                message_id: None,
                content: "正常消息".to_string(),
                message_type: None,
                timestamp: None,
            },
        ];

        let req = BatchModerationRequest { messages };

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/moderate/batch")
                    .header(http::header::CONTENT_TYPE, "application/json")
                    .body(Body::from(serde_json::to_string(&req).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);

        let body = response.into_body().collect().await.unwrap().to_bytes();
        let body: ApiResponse<BatchModerationResponse> = serde_json::from_slice(&body).unwrap();
        let data = body.data.unwrap();
        assert_eq!(data.total, 1);
    }
}
