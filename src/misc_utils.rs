use std::path::{Path, PathBuf};

pub fn expand_tilde<P: AsRef<Path>>(path_user_input: P) -> Option<PathBuf> {
    let p = path_user_input.as_ref();
    if !p.starts_with("~") {
        return Some(p.to_path_buf());
    }
    if p == Path::new("~") {
        return dirs::home_dir();
    }
    dirs::home_dir().map(|mut h| {
        if h == Path::new("/") {
            // Corner case: `h` root directory;
            // don't prepend extra `/`, just drop the tilde.
            p.strip_prefix("~").unwrap().to_path_buf()
        } else {
            h.push(p.strip_prefix("~/").unwrap());
            h
        }
    })
}

use bytes::Bytes;
use flexbuffers::VectorReader;
use futures::stream::{Stream, TryStreamExt};
use tokio::io::{AsyncRead, Result};
use tokio_util::codec;

pub fn into_bytes_stream<R>(r: R) -> impl Stream<Item = Result<Bytes>>
where
    R: AsyncRead,
{
    codec::FramedRead::new(r, codec::BytesCodec::new()).map_ok(|bytes| bytes.freeze())
}

pub(crate) fn string_vec_to_flex(data: &Vec<String>) -> Vec<u8> {
    let mut builder = flexbuffers::Builder::default();
    let mut vec = builder.start_vector();
    data.iter().for_each(|s| vec.push(s.as_str()));
    vec.end_vector();
    builder.take_buffer()
}

pub(crate) fn flex_to_string_vec(value: VectorReader) -> Vec<String> {
    value.iter().map(|r| r.as_str().to_owned()).collect()
}
