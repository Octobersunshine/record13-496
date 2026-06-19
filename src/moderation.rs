use serde::{Deserialize, Serialize};
use std::fmt;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ViolationType {
    Pornography,
    Violence,
    Politics,
    Advertising,
    Abuse,
    Gambling,
    Fraud,
    Other,
}

impl fmt::Display for ViolationType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match self {
            ViolationType::Pornography => "色情低俗",
            ViolationType::Violence => "暴力血腥",
            ViolationType::Politics => "政治敏感",
            ViolationType::Advertising => "广告推广",
            ViolationType::Abuse => "辱骂攻击",
            ViolationType::Gambling => "赌博相关",
            ViolationType::Fraud => "诈骗欺诈",
            ViolationType::Other => "其他违规",
        };
        write!(f, "{}", s)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Violation {
    pub violation_type: ViolationType,
    pub description: String,
    pub matched_text: Option<String>,
    pub severity: Severity,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Severity {
    Low,
    Medium,
    High,
    Critical,
}

impl fmt::Display for Severity {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match self {
            Severity::Low => "低",
            Severity::Medium => "中",
            Severity::High => "高",
            Severity::Critical => "严重",
        };
        write!(f, "{}", s)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ModerationResult {
    pub is_violation: bool,
    pub violations: Vec<Violation>,
    pub risk_score: u8,
    pub suggestion: ModerationSuggestion,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ModerationSuggestion {
    Pass,
    Review,
    Block,
}

struct Rule {
    violation_type: ViolationType,
    patterns: Vec<&'static str>,
    severity: Severity,
    description: &'static str,
}

fn get_rules() -> Vec<Rule> {
    vec![
        Rule {
            violation_type: ViolationType::Pornography,
            patterns: vec![
                "色情", "黄色", "裸聊", "约炮", "一夜情", "性服务",
                "成人影片", "av女优", "苍井空",
            ],
            severity: Severity::High,
            description: "包含色情低俗内容",
        },
        Rule {
            violation_type: ViolationType::Violence,
            patterns: vec![
                "杀人", "放火", "爆炸", "恐怖袭击", "黑社会",
                "砍人", "打群架", "贩毒",
            ],
            severity: Severity::High,
            description: "包含暴力血腥内容",
        },
        Rule {
            violation_type: ViolationType::Politics,
            patterns: vec![
                "法轮功", "台独", "港独", "藏独",
            ],
            severity: Severity::Critical,
            description: "包含政治敏感内容",
        },
        Rule {
            violation_type: ViolationType::Advertising,
            patterns: vec![
                "加微信", "加qq", "扫码进群", "免费领取", "点击链接",
                "www.", ".com", "http://", "https://",
            ],
            severity: Severity::Low,
            description: "包含广告推广内容",
        },
        Rule {
            violation_type: ViolationType::Abuse,
            patterns: vec![
                "傻逼", "草泥马", "操你妈", "白痴", "脑残",
                "废物", "垃圾人", "去死",
            ],
            severity: Severity::Medium,
            description: "包含辱骂攻击内容",
        },
        Rule {
            violation_type: ViolationType::Gambling,
            patterns: vec![
                "赌博", "彩票", "时时彩", "六合彩", "赌球",
                "老虎机", "百家乐", "炸金花",
            ],
            severity: Severity::High,
            description: "包含赌博相关内容",
        },
        Rule {
            violation_type: ViolationType::Fraud,
            patterns: vec![
                "刷单", "兼职日结", "投资回报", "稳赚不赔",
                "中奖", "汇款", "银行账号",
            ],
            severity: Severity::High,
            description: "包含诈骗欺诈内容",
        },
    ]
}

pub fn moderate_message(text: &str) -> ModerationResult {
    let mut violations: Vec<Violation> = Vec::new();
    let text_lower = text.to_lowercase();

    for rule in get_rules() {
        let mut matched: Vec<String> = Vec::new();
        for pattern in &rule.patterns {
            if text_lower.contains(&pattern.to_lowercase()) {
                matched.push(pattern.to_string());
            }
        }

        if !matched.is_empty() {
            violations.push(Violation {
                violation_type: rule.violation_type,
                description: rule.description.to_string(),
                matched_text: Some(matched.join(", ")),
                severity: rule.severity,
            });
        }
    }

    let is_violation = !violations.is_empty();

    let risk_score = if !is_violation {
        0
    } else {
        violations.iter().map(|v| severity_score(v.severity)).sum::<u8>().min(100)
    };

    let suggestion = if !is_violation {
        ModerationSuggestion::Pass
    } else if violations.iter().any(|v| v.severity == Severity::Critical) {
        ModerationSuggestion::Block
    } else if violations.iter().any(|v| v.severity == Severity::High) {
        if risk_score >= 60 {
            ModerationSuggestion::Block
        } else {
            ModerationSuggestion::Review
        }
    } else if risk_score >= 40 {
        ModerationSuggestion::Review
    } else {
        ModerationSuggestion::Pass
    };

    ModerationResult {
        is_violation,
        violations,
        risk_score,
        suggestion,
    }
}

fn severity_score(severity: Severity) -> u8 {
    match severity {
        Severity::Low => 10,
        Severity::Medium => 25,
        Severity::High => 40,
        Severity::Critical => 100,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_clean_message() {
        let result = moderate_message("你好，今天天气真好");
        assert!(!result.is_violation);
        assert_eq!(result.risk_score, 0);
        assert_eq!(result.suggestion, ModerationSuggestion::Pass);
    }

    #[test]
    fn test_advertising_message() {
        let result = moderate_message("加微信abc123免费领取礼品");
        assert!(result.is_violation);
        assert!(result.violations.iter().any(|v| v.violation_type == ViolationType::Advertising));
    }

    #[test]
    fn test_critical_violation() {
        let result = moderate_message("法轮功大法好");
        assert!(result.is_violation);
        assert_eq!(result.suggestion, ModerationSuggestion::Block);
        assert_eq!(result.risk_score, 100);
    }

    #[test]
    fn test_multiple_violations() {
        let result = moderate_message("加微信abc，傻逼玩意");
        assert!(result.is_violation);
        assert!(result.violations.len() >= 2);
    }
}
