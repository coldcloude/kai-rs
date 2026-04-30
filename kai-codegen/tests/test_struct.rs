pub struct User {
    pub id: i32,
    pub name: String,
    pub email: Option<String>,
    pub age: Option<u32>,
}

pub struct Product {
    pub product_id: String,
    pub price: f64,
    pub in_stock: bool,
    pub tags: Vec<String>,
    pub description: Option<&'static str>,
}
