use std::sync::Arc;

use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

#[derive(Debug, Clone, Serialize, Deserialize)]
struct User {
    id: u32,
    name: Arc<String>,
    #[serde(default, skip_serializing_if = "Value::is_null")]
    description: Arc<Value>,
}

pub fn main() {
    let u = User {
        id: 1,
        name: Arc::new(String::from("Alice")),
        description: Arc::new(Value::Null),
    };
    let json = serde_json::to_string(&u).unwrap();
    println!("{}", json);
    let value = serde_json::from_str(&json).unwrap();
    let entity = serde_json::from_value::<Arc<User>>(value).unwrap();
    println!("id = {}, name = {}, description = {}", entity.id, entity.name, entity.description);
}