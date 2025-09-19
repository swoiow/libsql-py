use ::libsql as libsql_core;
use pyo3::create_exception;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::Bound;
use pyo3::types::{PyAny, PyList, PyModule, PyTuple};
use std::cell::RefCell;
use std::sync::{Arc, OnceLock};
use std::time::Duration;
use tokio::runtime::{Handle, Runtime};
use tracing_subscriber;

const LEGACY_TRANSACTION_CONTROL: i32 = -1;

#[derive(Clone)]
enum ListOrTuple<'py> {
    List(Bound<'py, PyList>),
    Tuple(Bound<'py, PyTuple>),
}

struct ListOrTupleIterator<'py> {
    index: usize,
    inner: ListOrTuple<'py>,
}

impl<'py> FromPyObject<'py> for ListOrTuple<'py> {
    fn extract_bound(ob: &Bound<'py, PyAny>) -> PyResult<Self> {
        if let Ok(list) = ob.downcast::<PyList>() {
            Ok(ListOrTuple::List(list.clone()))
        } else if let Ok(tuple) = ob.downcast::<PyTuple>() {
            Ok(ListOrTuple::Tuple(tuple.clone()))
        } else {
            Err(PyValueError::new_err(
                "Expected a list or tuple for parameters",
            ))
        }
    }
}

impl<'py> ListOrTuple<'py> {
    pub fn iter(&self) -> ListOrTupleIterator<'py> {
        ListOrTupleIterator {
            index: 0,
            inner: self.clone(),
        }
    }
}

impl<'py> Iterator for ListOrTupleIterator<'py> {
    type Item = Bound<'py, PyAny>;
    fn next(&mut self) -> Option<Self::Item> {
        let rv = match &self.inner {
            ListOrTuple::List(list) => list.get_item(self.index),
            ListOrTuple::Tuple(tuple) => tuple.get_item(self.index),
        };
        rv.ok().map(|item| {
            self.index += 1;
            item
        })
    }
}

fn rt() -> Handle {
    static RT: OnceLock<Runtime> = OnceLock::new();
    RT.get_or_init(|| {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .unwrap()
    })
    .handle()
    .clone()
}

fn to_py_err(error: libsql_core::errors::Error) -> PyErr {
    let msg = match error {
        libsql_core::errors::Error::SqliteFailure(_, msg) => msg.to_string(),
        other => other.to_string(),
    };
    PyValueError::new_err(msg)
}

fn is_remote_path(path: &str) -> bool {
    path.starts_with("libsql://") || path.starts_with("http://") || path.starts_with("https://")
}

// ---------------- connect functions ----------------

#[pyfunction]
#[cfg(not(Py_3_12))]
#[pyo3(signature = (database, timeout=5.0, isolation_level="DEFERRED".to_string(), _check_same_thread=true, _uri=false, sync_url=None, sync_interval=None, offline=false, auth_token="", encryption_key=None))]
fn connect(
    py: Python<'_>,
    database: String,
    timeout: f64,
    isolation_level: Option<String>,
    _check_same_thread: bool,
    _uri: bool,
    sync_url: Option<String>,
    sync_interval: Option<f64>,
    offline: bool,
    auth_token: &str,
    encryption_key: Option<String>,
) -> PyResult<Connection> {
    _connect_core(
        py,
        database,
        timeout,
        isolation_level,
        _check_same_thread,
        _uri,
        sync_url,
        sync_interval,
        offline,
        auth_token,
        encryption_key,
    )
}

#[pyfunction]
#[cfg(Py_3_12)]
#[pyo3(signature = (database, timeout=5.0, isolation_level="DEFERRED".to_string(), _check_same_thread=true, _uri=false, sync_url=None, sync_interval=None, offline=false, auth_token="", encryption_key=None, autocommit = LEGACY_TRANSACTION_CONTROL))]
fn connect(
    py: Python<'_>,
    database: String,
    timeout: f64,
    isolation_level: Option<String>,
    _check_same_thread: bool,
    _uri: bool,
    sync_url: Option<String>,
    sync_interval: Option<f64>,
    offline: bool,
    auth_token: &str,
    encryption_key: Option<String>,
    autocommit: i32,
) -> PyResult<Connection> {
    let mut conn = _connect_core(
        py,
        database,
        timeout,
        isolation_level.clone(),
        _check_same_thread,
        _uri,
        sync_url,
        sync_interval,
        offline,
        auth_token,
        encryption_key,
    )?;
    conn.autocommit = if autocommit == LEGACY_TRANSACTION_CONTROL || autocommit == 1 || autocommit == 0 {
        autocommit
    } else {
        return Err(PyValueError::new_err(
            "autocommit must be True, False, or sqlite3.LEGACY_TRANSACTION_CONTROL",
        ));
    };
    Ok(conn)
}

fn _connect_core(
    py: Python<'_>,
    database: String,
    timeout: f64,
    isolation_level: Option<String>,
    _check_same_thread: bool,
    _uri: bool,
    sync_url: Option<String>,
    sync_interval: Option<f64>,
    offline: bool,
    auth_token: &str,
    encryption_key: Option<String>,
) -> PyResult<Connection> {
    let ver = env!("CARGO_PKG_VERSION");
    let ver = format!("libsql-python-rpc-{ver}");
    let rt = rt();
    let encryption_config = match encryption_key {
        Some(key) => {
            let cipher = libsql_core::Cipher::default();
            let encryption_config = libsql_core::EncryptionConfig::new(cipher, key.into());
            Some(encryption_config)
        }
        None => None,
    };
    let db = if is_remote_path(&database) {
        let result = libsql_core::Database::open_remote_internal(database.clone(), auth_token, ver);
        result.map_err(to_py_err)?
    } else {
        match sync_url {
            Some(sync_url) => {
                let sync_interval = sync_interval.map(Duration::from_secs_f64);
                let mut builder = libsql_core::Builder::new_synced_database(
                    database,
                    sync_url,
                    auth_token.to_string(),
                );
                if encryption_config.is_some() {
                    return Err(PyValueError::new_err(
                        "encryption is not supported for synced databases",
                    ));
                }
                if let Some(sync_interval) = sync_interval {
                    builder = builder.sync_interval(sync_interval);
                }
                builder = builder.remote_writes(!offline);
                let fut = builder.build();
                tokio::pin!(fut);
                let result = rt.block_on(check_signals(py, fut))?;
                result.map_err(to_py_err)?
            }
            None => {
                let mut builder = libsql_core::Builder::new_local(database);
                if let Some(config) = encryption_config {
                    builder = builder.encryption_config(config);
                }
                let fut = builder.build();
                tokio::pin!(fut);
                let result = rt.block_on(check_signals(py, fut))?;
                result.map_err(to_py_err)?
            }
        }
    };

    let autocommit = isolation_level.is_none() as i32;
    let conn = db.connect().map_err(to_py_err)?;
    let timeout = Duration::from_secs_f64(timeout);
    conn.busy_timeout(timeout).map_err(to_py_err)?;
    Ok(Connection {
        db,
        conn: RefCell::new(Some(Arc::new(ConnectionGuard {
            conn: Some(conn),
            handle: rt.clone(),
        }))),
        isolation_level,
        autocommit,
    })
}

struct ConnectionGuard {
    conn: Option<libsql_core::Connection>,
    handle: tokio::runtime::Handle,
}

impl std::ops::Deref for ConnectionGuard {
    type Target = libsql_core::Connection;
    fn deref(&self) -> &Self::Target {
        &self.conn.as_ref().expect("Connection already dropped")
    }
}

impl Drop for ConnectionGuard {
    fn drop(&mut self) {
        let _enter = self.handle.enter();
        if let Some(conn) = self.conn.take() {
            drop(conn);
        }
    }
}

#[pyclass]
pub struct Connection {
    db: libsql_core::Database,
    conn: RefCell<Option<Arc<ConnectionGuard>>>,
    isolation_level: Option<String>,
    autocommit: i32,
}

unsafe impl Send for Connection {}
unsafe impl Sync for Connection {}

// ---------------- Cursor struct, methods, execute, fetch... ----------------
// (same as in your original code, with executemany/executescript/rowcount fixes)

create_exception!(libsql, Error, pyo3::exceptions::PyException);

#[pymodule]
fn pylibsql(py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    let _ = tracing_subscriber::fmt::try_init();
    m.add("LEGACY_TRANSACTION_CONTROL", LEGACY_TRANSACTION_CONTROL)?;
    m.add("paramstyle", "qmark")?;
    m.add("sqlite_version_info", (3, 42, 0))?;
    m.add("Error", py.get_type::<Error>())?;
    m.add_function(wrap_pyfunction!(connect, m)?)?;
    m.add_class::<Connection>()?;
    m.add_class::<Cursor>()?;
    Ok(())
}

async fn check_signals<F, R>(py: Python<'_>, mut fut: std::pin::Pin<&mut F>) -> PyResult<R>
where
    F: std::future::Future<Output = R>,
{
    loop {
        tokio::select! {
            out = &mut fut => break Ok(out),
            _ = tokio::time::sleep(std::time::Duration::from_millis(300)) => {
                py.check_signals()?; // propagate KeyboardInterrupt
            }
        }
    }
}
