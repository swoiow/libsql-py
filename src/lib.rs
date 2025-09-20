use ::libsql as libsql_core;
use pyo3::Bound;
use pyo3::create_exception;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
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

unsafe impl Send for Cursor {}
unsafe impl Sync for Cursor {}

impl Drop for Cursor {
    fn drop(&mut self) {
        let _enter = rt().enter();
        self.conn.replace(None);
        self.stmt.replace(None);
        self.rows.replace(None);
    }
}

#[pymethods]
impl Cursor {
    fn close(self_: PyRef<'_, Self>) -> PyResult<()> {
        self_.conn.replace(None);
        self_.stmt.replace(None);
        self_.rows.replace(None);
        Ok(())
    }

    #[pyo3(signature = (sql, parameters=None))]
    fn execute<'a>(
        self_: PyRef<'a, Self>,
        sql: String,
        parameters: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<PyRef<'a, Self>> {
        let params = match parameters {
            Some(any) => Some(any.extract::<ListOrTuple>()?),
            None => None,
        };
        rt().block_on(async { execute_inner(&self_, sql, params).await })?;
        Ok(self_)
    }

    #[pyo3(signature = (sql, parameters=None))]
    fn executemany<'a>(
        self_: PyRef<'a, Self>,
        sql: String,
        parameters: Option<&Bound<'_, PyList>>,
    ) -> PyResult<PyRef<'a, Self>> {
        let seq = parameters.ok_or_else(|| {
            PyValueError::new_err("executemany() arg 2 must be a non-empty sequence")
        })?;
        for item in seq.iter() {
            let params = item.extract::<ListOrTuple>()?;
            rt().block_on(async { execute_inner(&self_, sql.clone(), Some(params)).await })?;
        }
        Ok(self_)
    }

    fn executescript<'a>(self_: PyRef<'a, Self>, script: String) -> PyResult<PyRef<'a, Self>> {
        rt().block_on(async {
            self_
                .conn
                .borrow()
                .as_ref()
                .unwrap()
                .execute_batch(&script)
                .await
        })
        .map_err(to_py_err)?;
        Ok(self_)
    }

    #[getter]
    fn description(self_: PyRef<'_, Self>) -> PyResult<Option<Bound<'_, PyTuple>>> {
        let stmt = self_.stmt.borrow();
        let mut elements: Vec<Py<PyAny>> = vec![];
        match stmt.as_ref() {
            Some(stmt) => {
                for column in stmt.columns() {
                    let name = column.name();
                    let element = (
                        name,
                        self_.py().None(),
                        self_.py().None(),
                        self_.py().None(),
                        self_.py().None(),
                        self_.py().None(),
                        self_.py().None(),
                    )
                        .into_pyobject(self_.py())
                        .unwrap();
                    elements.push(element.into());
                }
                let elements = PyTuple::new(self_.py(), elements)?;
                Ok(Some(elements))
            }
            None => Ok(None),
        }
    }

    fn fetchone(self_: PyRef<'_, Self>) -> PyResult<Option<Bound<'_, PyTuple>>> {
        let mut rows = self_.rows.borrow_mut();
        match rows.as_mut() {
            Some(rows) => {
                let row = rt().block_on(rows.next()).map_err(to_py_err)?;
                match row {
                    Some(row) => {
                        let row = convert_row(self_.py(), row, rows.column_count())?;
                        Ok(Some(row))
                    }
                    None => Ok(None),
                }
            }
            None => Ok(None),
        }
    }

    #[pyo3(signature = (size=None))]
    fn fetchmany(self_: PyRef<'_, Self>, size: Option<i64>) -> PyResult<Option<Bound<'_, PyList>>> {
        let mut rows = self_.rows.borrow_mut();
        match rows.as_mut() {
            Some(rows) => {
                let size = size.unwrap_or(self_.arraysize as i64);
                let mut elements: Vec<Py<PyAny>> = vec![];
                if !*self_.done.borrow() {
                    for _ in 0..size {
                        let row = rt()
                            .block_on(async { rows.next().await })
                            .map_err(to_py_err)?;
                        match row {
                            Some(row) => {
                                let row = convert_row(self_.py(), row, rows.column_count())?;
                                elements.push(row.into());
                            }
                            None => {
                                self_.done.replace(true);
                                break;
                            }
                        }
                    }
                }
                Ok(Some(PyList::new(self_.py(), elements)?))
            }
            None => Ok(None),
        }
    }

    fn fetchall(self_: PyRef<'_, Self>) -> PyResult<Option<Bound<'_, PyList>>> {
        let mut rows = self_.rows.borrow_mut();
        match rows.as_mut() {
            Some(rows) => {
                let mut elements: Vec<Py<PyAny>> = vec![];
                loop {
                    let row = rt()
                        .block_on(async { rows.next().await })
                        .map_err(to_py_err)?;
                    match row {
                        Some(row) => {
                            let row = convert_row(self_.py(), row, rows.column_count())?;
                            elements.push(row.into());
                        }
                        None => break,
                    }
                }
                Ok(Some(PyList::new(self_.py(), elements)?))
            }
            None => Ok(None),
        }
    }

    #[getter]
    fn lastrowid(self_: PyRef<'_, Self>) -> PyResult<Option<i64>> {
        let stmt = self_.stmt.borrow();
        match stmt.as_ref() {
            Some(_) => Ok(Some(
                self_.conn.borrow().as_ref().unwrap().last_insert_rowid(),
            )),
            None => Ok(None),
        }
    }

    #[getter]
    fn rowcount(self_: PyRef<'_, Self>) -> PyResult<i64> {
        Ok(*self_.rowcount.borrow())
    }
}

async fn execute_inner<'py>(
    cursor: &Cursor,
    sql: String,
    parameters: Option<ListOrTuple<'py>>,
) -> PyResult<()> {
    if cursor.conn.borrow().as_ref().is_none() {
        return Err(PyValueError::new_err("Connection already closed"));
    }

    let mut stmt = cursor
        .conn
        .borrow()
        .as_ref()
        .unwrap()
        .prepare(&sql)
        .await
        .map_err(to_py_err)?;

    if stmt.columns().iter().len() > 0 {
        let rows = stmt
            .query(libsql_core::params::Params::None)
            .await
            .map_err(to_py_err)?;
        cursor.rows.replace(Some(rows));
        *cursor.rowcount.borrow_mut() = -1;
    } else {
        let affected = stmt
            .execute(libsql_core::params::Params::None)
            .await
            .map_err(to_py_err)?;
        cursor.rows.replace(None);
        *cursor.rowcount.borrow_mut() = affected as i64;
    }

    cursor.stmt.replace(Some(stmt));
    Ok(())
}

fn convert_row<'py>(
    py: Python<'py>,
    row: libsql_core::Row,
    column_count: usize,
) -> PyResult<Bound<'py, PyTuple>> {
    let mut values: Vec<Py<PyAny>> = Vec::with_capacity(column_count);
    for i in 0..column_count {
        let v = row.get_value(i).map_err(to_py_err)?;
        let py_obj = match v {
            libsql_core::Value::Integer(i) => i.into_pyobject(py)?.unbind(),
            libsql_core::Value::Real(f) => f.into_pyobject(py)?.unbind(),
            libsql_core::Value::Text(t) => t.into_pyobject(py)?.unbind(),
            libsql_core::Value::Blob(b) => b.into_pyobject(py)?.unbind(),
            libsql_core::Value::Null => py.None().unbind(),
        };
        values.push(py_obj);
    }
    PyTuple::new(py, values)
}

create_exception!(libsql, Error, pyo3::exceptions::PyException);

#[pymodule]
fn pylibsql(py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    let _ = tracing_subscriber::fmt::try_init();
    m.add("LEGACY_TRANSACTION_CONTROL", LEGACY_TRANSACTION_CONTROL)?;
    m.add("paramstyle", "qmark")?;
}
