use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Message {
    pub id: String,
    pub thread_id: String,
    pub sender: String,
    pub sender_name: String,
    pub text: String,
    pub timestamp: i64,
}

impl Message {
    pub fn new(thread_id: &str, sender: &str, sender_name: &str, text: &str) -> Self {
        Message {
            id: uuid::Uuid::new_v4().to_string(),
            thread_id: thread_id.to_string(),
            sender: sender.to_string(),
            sender_name: sender_name.to_string(),
            text: text.to_string(),
            timestamp: chrono::Utc::now().timestamp(),
        }
    }

    pub fn to_bytes(&self) -> Result<Vec<u8>, serde_json::Error> {
        serde_json::to_vec(self)
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, serde_json::Error> {
        serde_json::from_slice(bytes)
    }
}