use std::{rc::Rc, sync::Arc};

pub trait AsStr {
    fn as_string(&self) -> &str;
}

pub trait Document<S: AsStr> {
    fn contents(&self) -> &Vec<S>;
}

impl AsStr for String {
    fn as_string(&self) -> &str {
        self.as_str()
    }
}

impl AsStr for Rc<String> {
    fn as_string(&self) -> &str {
        self.as_str()
    }
}

impl AsStr for Arc<String> {
    fn as_string(&self) -> &str {
        self.as_str()
    }
}

pub struct ToStringDocument {
    value: Vec<String>,
}

impl Document<String> for ToStringDocument {
    fn contents(&self) -> &Vec<String> {
        &self.value
    }
}

pub fn to_document<T: ToString>(value: T) -> ToStringDocument {
    return ToStringDocument {
        value: vec![value.to_string()],
    };
}
