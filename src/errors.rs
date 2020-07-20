#[macro_export]
macro_rules! invalid_data {
    ($($arg:tt)*) => {{
        let res = std::fmt::format(std::format_args!($($arg)*));
        std::io::Error::new(std::io::ErrorKind::InvalidData, res)
    }}
}

#[macro_export]
macro_rules! invalid_data_e {
    ($($arg:tt)*) => { Err(invalid_data!($($arg)*)) };
}

#[macro_export]
macro_rules! invalid_data_ae {
    ($($arg:tt)*) => { Err(ApiError(invalid_data!($($arg)*))) };
}

#[macro_export]
macro_rules! other_e {
    ($($arg:tt)*) => {{
        let res = std::fmt::format(std::format_args!($($arg)*));
        Err(ApiError(std::io::Error::new(std::io::ErrorKind::Other, res)))
    }}
}

use futures::io::ErrorKind;
use rocket::http::{ContentType, Status};
use rocket::response::{self, Responder, Response};
use rocket::Request;
use std::io::Cursor;
use std::result::Result;
use tokio::io;

#[derive(Debug)]
pub struct ApiError(pub io::Error);

pub type ApiResult<T> = Result<T, ApiError>;

impl<'r> Responder<'r, 'static> for ApiError {
    fn respond_to(self, _: &'r Request<'_>) -> response::Result<'static> {
        let descr = format!("Error: {:?}", self.0.to_string());
        Response::build()
            .header(ContentType::Plain)
            .sized_body(descr.len(), Cursor::new(descr))
            .status(match self.0.kind() {
                ErrorKind::InvalidData => Status::BadRequest,
                _ => Status::InternalServerError,
            })
            .ok()
    }
}

impl std::convert::From<io::Error> for ApiError {
    fn from(e: io::Error) -> Self {
        Self(e)
    }
}

impl std::convert::From<flexbuffers::ReaderError> for ApiError {
    fn from(e: flexbuffers::ReaderError) -> Self {
        Self(invalid_data!("{:?}", e))
    }
}

impl std::convert::From<sqlparser::parser::ParserError> for ApiError {
    fn from(e: sqlparser::parser::ParserError) -> Self {
        Self(invalid_data!("{}", e))
    }
}

impl std::convert::From<std::fmt::Error> for ApiError {
    fn from(e: std::fmt::Error) -> Self {
        Self(invalid_data!("{}", e))
    }
}

impl std::convert::From<std::ffi::NulError> for ApiError {
    fn from(e: std::ffi::NulError) -> Self {
        Self(invalid_data!("{}", e))
    }
}

impl std::convert::From<rocket::error::Error> for ApiError {
    fn from(e: rocket::error::Error) -> Self {
        Self(invalid_data!("{}", e))
    }
}

impl<T> std::convert::From<std::sync::PoisonError<T>> for ApiError {
    fn from(e: std::sync::PoisonError<T>) -> Self {
        Self(invalid_data!("{}", e))
    }
}

impl std::convert::From<tokio::task::JoinError> for ApiError {
    fn from(e: tokio::task::JoinError) -> Self {
        Self(invalid_data!("{}", e))
    }
}
