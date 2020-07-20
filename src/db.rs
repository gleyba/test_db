use flexbuffers::{Reader, VectorReader};
use futures::future::Future;
use libc::{c_int, c_uint};
use mdbx_sys::*;
use rocket::{request, State};
use std::{
    boxed::Box,
    ffi::{CStr, CString},
    io::{Error, ErrorKind},
    marker::Sized,
    ptr,
    sync::Arc,
};

use crate::errors::*;
use crate::misc_utils::*;
use std::marker::PhantomData;

#[derive(Clone)]
pub struct DBHandle(pub Arc<DB>);

impl std::ops::Deref for DBHandle {
    type Target = DB;

    fn deref(&self) -> &DB {
        &*self.0
    }
}

#[rocket::async_trait]
impl<'a, 'r> request::FromRequest<'a, 'r> for DBHandle {
    type Error = ();

    async fn from_request(req: &'a request::Request<'r>) -> request::Outcome<DBHandle, ()> {
        let db_handle = try_outcome!(req.guard::<State<DBHandle>>().await);
        request::Outcome::Success(db_handle.clone())
    }
}

type AsyncMutex<T> = tokio::sync::Mutex<T>;

pub struct DB {
    env: Arc<DBEnv>,
    write_mutex: AsyncMutex<MutableDB>,
}

impl DB {
    pub fn new(path: &str) -> ApiResult<Self> {
        guard!(let Some(path_buf) = expand_tilde(path) else {
            return other_e!("wrong path for db: {}", path);
        });
        guard!(let Some(path_str) = path_buf.to_str() else {
            return other_e!("wrong path for db: {}", path);
        });
        let env = Arc::new(DBEnv::new(path_str)?);
        Ok(Self {
            env: env.clone(),
            write_mutex: AsyncMutex::new(MutableDB::new(env)?),
        })
    }

    pub async fn mutation(&self) -> tokio::sync::MutexGuard<'_, MutableDB> {
        self.write_mutex.lock().await
    }

    pub fn table(&self, table: String) -> Table {
        Table::new(self.env.clone(), table)
    }
}

pub struct MutableDB {
    env: Arc<DBEnv>,
    runtime: Arc<tokio::runtime::Runtime>,
}

impl MutableDB {
    fn new(env: Arc<DBEnv>) -> ApiResult<Self> {
        let runtime = tokio::runtime::Builder::new()
            .threaded_scheduler()
            .core_threads(1)
            .thread_name("db_writer")
            .enable_all()
            .build()?;

        let result = Self {
            env,
            runtime: Arc::new(runtime),
        };
        Ok(result)
    }

    pub async fn mutable_table<R, Fut>(
        &self,
        table: String,
        f: impl FnOnce(WriteTransaction) -> Fut + Send + 'static,
    ) -> ApiResult<R>
    where
        R: Sized + Send + 'static,
        Fut: Future<Output = ApiResult<R>> + Send,
    {
        let env = self.env.clone();
        self.runtime
            .spawn(async move {
                let table = MutableTable::new(env, table);
                let txn = table.open_transaction()?;
                f(txn).await
            })
            .await?
    }
}

pub struct MutableTable {
    env: Arc<DBEnv>,
    name: String,
}

impl MutableTable {
    fn new(env: Arc<DBEnv>, name: String) -> Self {
        Self { env, name }
    }

    pub fn open_transaction(&self) -> ApiResult<WriteTransaction> {
        let txn = Transaction::new(self.env.env, 0, MDBX_CREATE, self.name.clone())?;
        Ok(WriteTransaction::new(txn))
    }
}

pub struct WriteTransaction {
    txn: Transaction,
    last_idx: usize,
}

impl WriteTransaction {
    fn new(txn: Transaction) -> Self {
        Self { txn, last_idx: 0 }
    }

    pub fn drop(&mut self) -> ApiResult<()> {
        unsafe {
            mdbx_check(mdbx_drop(self.txn.txn, self.txn.dbi, 0))?;
        }
        Ok(())
    }

    pub fn commit(&mut self) -> ApiResult<()> {
        unsafe {
            mdbx_check(mdbx_txn_commit(self.txn.txn))?;
        }
        self.txn.txn = ptr::null_mut();
        Ok(())
    }

    pub fn append(&mut self, data: Vec<u8>) -> ApiResult<()> {
        unsafe {
            let key_str = self.last_idx.to_string();
            let mut key = into_val(key_str.as_bytes());
            let mut value = into_val(data.as_ref());
            mdbx_check(mdbx_put(
                self.txn.txn,
                self.txn.dbi,
                &mut key,
                &mut value,
                MDBX_APPEND,
            ))?;
        }
        self.last_idx += 1;
        Ok(())
    }
}

pub struct Table {
    env: Arc<DBEnv>,
    name: String,
}

impl Table {
    fn new(env: Arc<DBEnv>, name: String) -> Self {
        Self { env, name }
    }

    pub fn open_transaction<'ret, 'me: 'ret>(&'me self) -> ApiResult<ReadTransaction<'ret>> {
        let txn = Transaction::new(self.env.env, MDBX_RDONLY, 0, self.name.clone())?;
        Ok(ReadTransaction::new(txn))
    }
}

pub struct ReadTransaction<'de> {
    txn: Transaction,
    _phantom: PhantomData<&'de ()>,
}

impl<'de> ReadTransaction<'de> {
    fn new(txn: Transaction) -> Self {
        Self {
            txn,
            _phantom: PhantomData,
        }
    }

    pub fn cursor_on_start<'ret>(&'de self) -> ApiResult<Cursor<'de>> {
        let mut cursor: *mut MDBX_cursor = ptr::null_mut();
        unsafe { mdbx_check(mdbx_cursor_open(self.txn.txn, self.txn.dbi, &mut cursor))? }
        Cursor::new(cursor, MDBX_cursor_op::MDBX_FIRST)
    }
}

pub struct Cursor<'de> {
    cursor: *mut MDBX_cursor,
    key: MDBX_val,
    data: MDBX_val,
    _phantom: PhantomData<&'de ()>,
}

impl<'de> Cursor<'de> {
    fn new(cursor: *mut MDBX_cursor, op: MDBX_cursor_op) -> ApiResult<Self> {
        let mut result = Self {
            cursor,
            key: MDBX_val {
                iov_base: ptr::null_mut(),
                iov_len: 0,
            },
            data: MDBX_val {
                iov_base: ptr::null_mut(),
                iov_len: 0,
            },
            _phantom: PhantomData,
        };
        result.call_get(op)?;
        Ok(result)
    }

    fn call_get(&mut self, op: MDBX_cursor_op) -> ApiResult<bool> {
        unsafe {
            let err_code = mdbx_cursor_get(self.cursor, &mut self.key, &mut self.data, op);
            if err_code == MDBX_NOTFOUND {
                return Ok(false);
            } else {
                mdbx_check(err_code)?;
            }
        }
        Ok(true)
    }

    pub fn next(&mut self) -> ApiResult<Option<VectorReader<'de>>> {
        if !self.call_get(MDBX_cursor_op::MDBX_NEXT)? {
            return Ok(None);
        }
        Ok(Some(self.data()?))
    }

    #[inline]
    unsafe fn extend_data_lifetime<'a>(data: &'a [u8]) -> &'de [u8] {
        std::mem::transmute(data)
    }

    #[inline]
    pub fn data(&self) -> ApiResult<VectorReader<'de>> {
        if self.data.iov_base.is_null() {
            return invalid_data_ae!("NULL data");
        }

        let result = unsafe {
            let data = from_val(&self.data);
            let data = Self::extend_data_lifetime(data);
            let reader = Reader::get_root(data)?;
            reader.as_vector()
        };

        Ok(result)
    }
}

impl Drop for Cursor<'_> {
    fn drop(&mut self) {
        unsafe {
            mdbx_cursor_close(self.cursor);
        }
    }
}

pub struct Transaction {
    txn: *mut MDBX_txn,
    dbi: MDBX_dbi,
}

unsafe impl Send for Transaction {}
unsafe impl Sync for Transaction {}

impl Transaction {
    fn new(
        env: *mut MDBX_env,
        txn_flags: c_uint,
        dbi_flags: c_uint,
        column: String,
    ) -> ApiResult<Self> {
        let mut txn: *mut MDBX_txn = ptr::null_mut();
        let mut dbi: MDBX_dbi = 0;
        let column = CString::new(column)?;
        unsafe {
            mdbx_check(mdbx_txn_begin(env, ptr::null_mut(), txn_flags, &mut txn))?;
            mdbx_check(mdbx_dbi_open_ex(
                txn,
                column.as_ptr(),
                dbi_flags,
                &mut dbi,
                Some(key_cmp),
                None,
            ))?;
        };
        Ok(Self { txn, dbi })
    }
}

#[no_mangle]
extern "C" fn key_cmp(a: *const MDBX_val, b: *const MDBX_val) -> c_int {
    unsafe {
        let a = String::from_utf8_lossy(from_val_ptr(a))
            .parse::<usize>()
            .unwrap();
        let b = String::from_utf8_lossy(from_val_ptr(b))
            .parse::<usize>()
            .unwrap();
        if a == b {
            0
        } else if a < b {
            -1
        } else {
            1
        }
    }
}

impl Drop for Transaction {
    fn drop(&mut self) {
        unsafe {
            if self.txn != ptr::null_mut() {
                mdbx_txn_abort(self.txn);
            }
        }
    }
}

struct DBEnv {
    env: *mut MDBX_env,
}

unsafe impl Send for DBEnv {}
unsafe impl Sync for DBEnv {}

const MAX_TABLES: MDBX_dbi = 1024;
const OPEN_FLAGS: c_uint = MDBX_COALESCE
    | MDBX_LIFORECLAIM
    | MDBX_NOMETASYNC
    | MDBX_NOTLS
    | MDBX_NOMEMINIT
    | MDBX_EXCLUSIVE;

impl DBEnv {
    pub fn new(path: &str) -> ApiResult<Self> {
        let path = CString::new(path)?;
        let mut env: *mut MDBX_env = ptr::null_mut();
        unsafe {
            mdbx_check(mdbx_env_create(&mut env))?;
            mdbx_check(mdbx_env_set_maxdbs(env, MAX_TABLES))?;
            mdbx_check(mdbx_env_set_geometry(
                env,
                16 * 1024 * 1024,
                -1,
                16 * 1024 * 1024 * 1024,
                16 * 1024 * 1024,
                -1,
                -1,
            ))?;
            mdbx_check(mdbx_env_open(env, path.as_ptr(), OPEN_FLAGS, 0o664))?;
        }
        Ok(Self { env })
    }
}

unsafe fn into_val(value: &[u8]) -> MDBX_val {
    MDBX_val {
        iov_base: value.as_ptr() as *mut libc::c_void,
        iov_len: value.len(),
    }
}

unsafe fn from_val(value: &MDBX_val) -> &[u8] {
    std::slice::from_raw_parts(value.iov_base as *const u8, value.iov_len)
}

unsafe fn from_val_ptr<'a>(value: *const MDBX_val) -> &'a [u8] {
    std::slice::from_raw_parts((*value).iov_base as *const u8, (*value).iov_len)
}

unsafe fn mdbx_check(err_code: c_int) -> ApiResult<()> {
    if err_code == MDBX_SUCCESS {
        Ok(())
    } else {
        let err_desc = CStr::from_ptr(mdbx_strerror(err_code));
        let str = err_desc
            .to_str()
            .map_err(|e| Error::new(ErrorKind::Other, e))?;
        Err(ApiError(Error::new(ErrorKind::Other, str)))
    }
}
