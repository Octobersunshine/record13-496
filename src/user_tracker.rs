use crate::moderation::ModerationResult;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use tokio::sync::RwLock;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum UserStatus {
    Normal,
    Warned,
    Muted,
    Banned,
}

impl std::fmt::Display for UserStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            UserStatus::Normal => "正常",
            UserStatus::Warned => "警告",
            UserStatus::Muted => "禁言",
            UserStatus::Banned => "封禁",
        };
        write!(f, "{}", s)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ViolationRecord {
    pub message_content: String,
    pub group_id: String,
    pub violation_types: Vec<String>,
    pub risk_score: u8,
    pub suggestion: String,
    pub timestamp: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UserInfo {
    pub user_id: String,
    pub user_name: Option<String>,
    pub status: UserStatus,
    pub violation_count: u32,
    pub total_risk_score: u64,
    pub groups: Vec<String>,
    pub last_violation_at: Option<i64>,
    pub status_updated_at: Option<i64>,
    pub records: Vec<ViolationRecord>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ViolationUserSummary {
    pub user_id: String,
    pub user_name: Option<String>,
    pub status: UserStatus,
    pub status_name: String,
    pub violation_count: u32,
    pub total_risk_score: u64,
    pub groups: Vec<String>,
    pub last_violation_at: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ViolationUserListResponse {
    pub users: Vec<ViolationUserSummary>,
    pub total: usize,
}

#[derive(Debug, Clone, Deserialize)]
pub struct UserActionRequest {
    pub action: UserAction,
    pub reason: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum UserAction {
    Warn,
    Mute,
    Ban,
    Unmute,
    Unban,
    Reset,
}

pub struct UserViolationTracker {
    users: RwLock<HashMap<String, UserInfo>>,
    warning_threshold: u32,
    mute_threshold: u32,
    ban_threshold: u32,
}

impl UserViolationTracker {
    pub fn new(warning_threshold: u32, mute_threshold: u32, ban_threshold: u32) -> Self {
        Self {
            users: RwLock::new(HashMap::new()),
            warning_threshold,
            mute_threshold,
            ban_threshold,
        }
    }

    pub async fn record_violation(
        &self,
        user_id: &str,
        user_name: Option<&str>,
        group_id: &str,
        content: &str,
        result: &ModerationResult,
        timestamp: i64,
    ) -> UserStatus {
        let mut users = self.users.write().await;

        let user = users.entry(user_id.to_string()).or_insert_with(|| UserInfo {
            user_id: user_id.to_string(),
            user_name: user_name.map(|s| s.to_string()),
            status: UserStatus::Normal,
            violation_count: 0,
            total_risk_score: 0,
            groups: Vec::new(),
            last_violation_at: None,
            status_updated_at: None,
            records: Vec::new(),
        });

        if let Some(name) = user_name {
            if user.user_name.is_none() {
                user.user_name = Some(name.to_string());
            }
        }

        if !user.groups.contains(&group_id.to_string()) {
            user.groups.push(group_id.to_string());
        }

        user.violation_count += 1;
        user.total_risk_score += result.risk_score as u64;
        user.last_violation_at = Some(timestamp);

        let record = ViolationRecord {
            message_content: content.to_string(),
            group_id: group_id.to_string(),
            violation_types: result
                .violations
                .iter()
                .map(|v| format!("{:?}", v.violation_type).to_lowercase())
                .collect(),
            risk_score: result.risk_score,
            suggestion: format!("{:?}", result.suggestion).to_lowercase(),
            timestamp,
        };
        user.records.push(record);

        let new_status = self.calculate_status(user.violation_count, user.total_risk_score);
        if new_status != user.status {
            user.status = new_status;
            user.status_updated_at = Some(timestamp);
        }

        user.status
    }

    fn calculate_status(&self, violation_count: u32, total_risk_score: u64) -> UserStatus {
        if violation_count >= self.ban_threshold || total_risk_score >= 300 {
            UserStatus::Banned
        } else if violation_count >= self.mute_threshold || total_risk_score >= 150 {
            UserStatus::Muted
        } else if violation_count >= self.warning_threshold || total_risk_score >= 50 {
            UserStatus::Warned
        } else {
            UserStatus::Normal
        }
    }

    pub async fn get_user(&self, user_id: &str) -> Option<UserInfo> {
        let users = self.users.read().await;
        users.get(user_id).cloned()
    }

    pub async fn get_violation_users(&self, min_violations: u32) -> ViolationUserListResponse {
        let users = self.users.read().await;
        let filtered: Vec<ViolationUserSummary> = users
            .values()
            .filter(|u| u.violation_count >= min_violations)
            .map(|u| ViolationUserSummary {
                user_id: u.user_id.clone(),
                user_name: u.user_name.clone(),
                status: u.status,
                status_name: u.status.to_string(),
                violation_count: u.violation_count,
                total_risk_score: u.total_risk_score,
                groups: u.groups.clone(),
                last_violation_at: u.last_violation_at,
            })
            .collect();

        let total = filtered.len();
        ViolationUserListResponse {
            users: filtered,
            total,
        }
    }

    pub async fn apply_action(
        &self,
        user_id: &str,
        action: UserAction,
        reason: Option<&str>,
        timestamp: i64,
    ) -> Result<UserInfo, String> {
        let mut users = self.users.write().await;

        let user = users
            .get_mut(user_id)
            .ok_or_else(|| format!("用户 {} 不存在", user_id))?;

        match action {
            UserAction::Warn => {
                user.status = UserStatus::Warned;
            }
            UserAction::Mute => {
                user.status = UserStatus::Muted;
            }
            UserAction::Ban => {
                user.status = UserStatus::Banned;
            }
            UserAction::Unmute | UserAction::Unban => {
                user.status = self.calculate_status(user.violation_count, user.total_risk_score);
            }
            UserAction::Reset => {
                user.status = UserStatus::Normal;
                user.violation_count = 0;
                user.total_risk_score = 0;
                user.last_violation_at = None;
                user.records.clear();
            }
        }

        user.status_updated_at = Some(timestamp);

        let _ = reason;

        Ok(user.clone())
    }

    pub async fn get_users_by_status(&self, status: UserStatus) -> Vec<ViolationUserSummary> {
        let users = self.users.read().await;
        users
            .values()
            .filter(|u| u.status == status)
            .map(|u| ViolationUserSummary {
                user_id: u.user_id.clone(),
                user_name: u.user_name.clone(),
                status: u.status,
                status_name: u.status.to_string(),
                violation_count: u.violation_count,
                total_risk_score: u.total_risk_score,
                groups: u.groups.clone(),
                last_violation_at: u.last_violation_at,
            })
            .collect()
    }

    pub async fn get_statistics(&self) -> UserStatistics {
        let users = self.users.read().await;
        let mut stats = UserStatistics::default();
        stats.total_tracked_users = users.len();

        for user in users.values() {
            match user.status {
                UserStatus::Normal => stats.normal_count += 1,
                UserStatus::Warned => stats.warned_count += 1,
                UserStatus::Muted => stats.muted_count += 1,
                UserStatus::Banned => stats.banned_count += 1,
            }
            if user.violation_count > 0 {
                stats.users_with_violations += 1;
            }
            stats.total_violation_records += user.records.len();
        }

        stats
    }
}

#[derive(Debug, Clone, Serialize, Default)]
pub struct UserStatistics {
    pub total_tracked_users: usize,
    pub users_with_violations: usize,
    pub normal_count: usize,
    pub warned_count: usize,
    pub muted_count: usize,
    pub banned_count: usize,
    pub total_violation_records: usize,
}
