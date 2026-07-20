use async_trait::async_trait;

#[async_trait]
pub trait FileAppendWriter<K, R>: Send + Sync + 'static {
    async fn write(&self, key: &K, records: Vec<R>) -> crate::error::Result<()>;
}

pub trait ErrorHandler<K, R>: Send + Sync + 'static {
    fn on_write_error(&self, key: &K, batch: Vec<R>, error: &crate::error::Error);
}

pub struct NoopErrorHandler;

impl<K, R> ErrorHandler<K, R> for NoopErrorHandler {
    fn on_write_error(&self, _key: &K, _batch: Vec<R>, _error: &crate::error::Error) {}
}

#[cfg(test)]
mod tests {
    use super::*;

    struct TestWriter;
    #[async_trait]
    impl FileAppendWriter<String, String> for TestWriter {
        async fn write(&self, _key: &String, _records: Vec<String>) -> crate::error::Result<()> {
            Ok(())
        }
    }

    struct TestErrorHandler;
    impl ErrorHandler<String, String> for TestErrorHandler {
        fn on_write_error(&self, _key: &String, _batch: Vec<String>, _error: &crate::error::Error) {}
    }

    #[tokio::test]
    async fn test_traits_are_implementable() {
        let writer = TestWriter;
        let handler = TestErrorHandler;
        let _appender: FileObjectAppender<String, String, TestWriter, TestErrorHandler> =
            FileObjectAppender::new(writer, handler, std::time::Duration::from_secs(10), 100);
    }
}
