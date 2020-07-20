use rocket::request;
use rocket::State;
use slog::Logger;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct SyncLogger(pub Arc<Logger>);

impl std::ops::Deref for SyncLogger {
    type Target = Logger;

    fn deref(&self) -> &Logger {
        &*self.0
    }
}

#[rocket::async_trait]
impl<'a, 'r> request::FromRequest<'a, 'r> for SyncLogger {
    type Error = ();

    async fn from_request(req: &'a request::Request<'r>) -> request::Outcome<SyncLogger, ()> {
        let sync_logger = try_outcome!(req.guard::<State<SyncLogger>>().await);
        request::Outcome::Success(sync_logger.clone())
    }
}
