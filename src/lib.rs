use libsql as libsql_core;
use pyo3::create_exception;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::{Bound, PyList, PyModule, PyTuple};
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
    let conn = _connect_core(
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
    )?;
    Ok(conn)
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

    conn.autocommit =
        if autocommit == LEGACY_TRANSACTION_CONTROL || autocommit == 1 || autocommit == 0 {
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
                let sync_interval = sync_interval.map(|i| std::time::Duration::from_secs_f64(i));
                let mut builder = libsql_core::Builder::new_synced_database(
                    database,
                    sync_url,
                    auth_token.to_string(),
                );
                if let Some(_) = encryption_config {
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

// ConnectionGuard omitted (unchanged)...

#[pyclass]
pub struct Connection {
    db: libsql_core::Database,
    conn: RefCell<Option<Arc<ConnectionGuard>>>,
    isolation_level: Option<String>,
    autocommit: i32,
}

// SAFETY omitted (unchanged)...

#[pymethods]
impl Connection {
    // close, cursor, sync, commit, rollback omitted (unchanged)...

    #[pyo3(signature = (sql, parameters=None))]
    fn executemany(
        self_: PyRef<'_, Self>,
        sql: String,
        parameters: Option<&Bound<'_, PyList>>,
    ) -> PyResult<Cursor> {
        let seq = parameters.ok_or_else(|| {
            PyValueError::new_err("executemany() arg 2 must be a non-empty sequence")
        })?;
        let cursor = Connection::cursor(&self_)?;
        for parameters in seq.iter() {
            let parameters = parameters.extract::<ListOrTuple>()?;
            rt().block_on(async { execute(&cursor, sql.clone(), Some(parameters)).await })?;
        }
        Ok(cursor)
    }

    fn executescript(self_: PyRef<'_, Self>, script: String) -> PyResult<()> {
        rt()
            .block_on(async {
                self_
                    .conn
                    .borrow()
                    .as_ref()
                    .unwrap()
                    .execute_batch(&script)
                    .await
            })
            .map_err(to_py_err)?;
        Ok(())
    }

    // rest unchanged...
}

#[pyclass]
pub struct Cursor {
    #[pyo3(get, set)]
    arraysize: usize,
    conn: RefCell<Option<Arc<ConnectionGuard>>>,
    stmt: RefCell<Option<libsql_core::Statement>>,
    rows: RefCell<Option<libsql_core::Rows>>,
    rowcount: RefCell<i64>,
    done: RefCell<bool>,
    isolation_level: Option<String>,
    autocommit: i32,
}

// Cursor impl with executemany fix and rowcount fix:
#[pymethods]
impl Cursor {
    #[pyo3(signature = (sql, parameters=None))]
    fn executemany<'a>(
        self_: PyRef<'a, Self>,
        sql: String,
        parameters: Option<&Bound<'_, PyList>>,
    ) -> PyResult<pyo3::PyRef<'a, Cursor>> {
        let seq = parameters.ok_or_else(|| {
            PyValueError::new_err("executemany() arg 2 must be a non-empty sequence")
        })?;
        for parameters in seq.iter() {
            let parameters = parameters.extract::<ListOrTuple>()?;
            rt().block_on(async { execute(&self_, sql.clone(), Some(parameters)).await })?;
        }
        Ok(self_)
    }

    // rest unchanged...
}

// execute() with rowcount fix
async fn execute<'py>(
    cursor: &Cursor,
    sql: String,
    parameters: Option<ListOrTuple<'py>>,
) -> PyResult<()> {
    if cursor.conn.borrow().as_ref().is_none() {
        return Err(PyValueError::new_err("Connection already closed"));
    }
    let stmt_is_dml = stmt_is_dml(&sql);
    let autocommit = determine_autocommit(cursor);
    if !autocommit && stmt_is_dml && cursor.conn.borrow().as_ref().unwrap().is_autocommit() {
        begin_transaction(&cursor.conn.borrow().as_ref().unwrap()).await?;
    }
    // param conversion unchanged...
    let mut stmt = cursor
        .conn
        .borrow()
        .as_ref()
        .unwrap()
        .prepare(&sql)
        .await
        .map_err(to_py_err)?;

    if stmt.columns().iter().len() > 0 {
        let rows = stmt.query(params).await.map_err(to_py_err)?;
        cursor.rows.replace(Some(rows));
        *cursor.rowcount.borrow_mut() = -1;
    } else {
        let affected = stmt.execute(params).await.map_err(to_py_err)?;
        cursor.rows.replace(None);
        *cursor.rowcount.borrow_mut() = affected as i64;
    }

    cursor.stmt.replace(Some(stmt));
    Ok(())
}

// rest of file unchanged, but check_signals changed to PyResult
async fn check_signals<F, R>(py: Python<'_>, mut fut: std::pin::Pin<&mut F>) -> PyResult<R>
where
    F: std::future::Future<Output = R>,
{
    loop {
        tokio::select! {
            out = &mut fut => {
                break Ok(out);
            }

            _ = tokio::time::sleep(std::time::Duration::from_millis(300)) => {
                py.check_signals()?; // bubble KeyboardInterrupt
            }
        }
    }
}
