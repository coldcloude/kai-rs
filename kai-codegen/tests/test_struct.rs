use std::collections::HashMap;

pub struct Address {
    pub street: String,
    pub city: String,
    pub zip_code: String,
}

pub struct User {
    pub id: i32,
    pub name: String,
    pub email: Option<String>,
    pub age: Option<u32>,
    pub address: Address,
}

pub struct Product {
    pub product_id: String,
    pub price: f64,
    pub in_stock: bool,
    pub tags: Vec<String>,
    pub description: Option<&'static str>,
    pub metadata: HashMap<String, String>,
}

pub struct Order {
    pub order_id: String,
    pub user: User,
    pub products: Vec<Product>,
    pub total: f64,
    pub status: String,
    pub discounts: Option<HashMap<String, f64>>,
}
