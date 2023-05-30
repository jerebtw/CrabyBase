use anyhow::Result;
use log::debug;
use r2d2::Pool;
use r2d2_sqlite::SqliteConnectionManager;
use rusqlite::{Column, Row, Rows};
use rusqlite_migration::{Migrations, M};
use serde::de::DeserializeOwned;
use serde_json::{json, Value};

pub type DBPool = Pool<SqliteConnectionManager>;

fn connect_db(connection_str: &str) -> Result<Pool<SqliteConnectionManager>> {
  let manager = SqliteConnectionManager::file(connection_str).with_init(|conn| {
    conn.execute_batch(
      r#"
        PRAGMA foreign_keys = ON;
        PRAGMA journal_mode = WAL;
        PRAGMA synchronous = NORMAL;
        PRAGMA busy_timeout = 5000;
      "#,
    )
  });
  let builder = r2d2::Pool::builder();
  Ok(builder.build(manager)?)
}

pub fn connect_data_pool() -> Result<DBPool> {
  debug!("Setting up data pool");
  let data_pool = connect_db("test.db")?;
  let mut data_connection = data_pool.get()?;

  let migrations = Migrations::new(vec![M::up(include_str!(
    "../migrations/sqlite/data/2023-04-08-create-tables-table.sql"
  ))]);
  migrations.to_latest(&mut data_connection)?;

  Ok(data_pool)
}

pub fn connect_log_pool() -> Result<DBPool> {
  debug!("Setting up log pool");
  let log_pool = connect_db("log.db")?;
  let mut log_connection = log_pool.get()?;

  let migrations = Migrations::new(vec![M::up(include_str!(
    "../migrations/sqlite/log/2023-04-08-create-logs-table.sql"
  ))]);
  migrations.to_latest(&mut log_connection)?;

  Ok(log_pool)
}

#[macro_export]
macro_rules! query_rows_columns {
  ($columns:ident, $rows:ident, $connection:expr, $query:expr) => {
    let statement = $connection.prepare($query)?;
    let $columns = statement.columns();
    let mut statement = $connection.prepare($query)?;
    let $rows = statement.query([])?;
  };

  ($columns:ident, $rows:ident, $connection:expr, $query:expr, $params:expr) => {
    let statement = $connection.prepare($query)?;
    let $columns = statement.columns();
    let mut statement = $connection.prepare($query)?;
    let mut $rows = statement.query($params)?;
  };
}

pub fn parse_rows<T: DeserializeOwned>(columns: &Vec<Column>, mut rows: Rows) -> Result<Vec<T>> {
  let mut rows_data = vec![];
  while let Some(row) = rows.next()? {
    let parsed_row = parse_row(&columns, row)?;
    rows_data.push(serde_json::from_value::<T>(parsed_row)?);
  }
  Ok(rows_data)
}

pub fn parse_rows_dynamic(columns: &Vec<Column>, mut rows: Rows) -> Result<Vec<Value>> {
  let mut rows_data = vec![];
  while let Some(row) = rows.next()? {
    rows_data.push(parse_row(&columns, row)?);
  }
  Ok(rows_data)
}

pub fn parse_row(columns: &Vec<Column>, row: &Row) -> Result<Value> {
  let mut column_data = serde_json::Map::new();
  for i in 0..columns.len() {
    column_data.insert(
      columns[i].name().to_string(),
      parse_column(row, &columns[i], i)?,
    );
  }
  Ok(json!(column_data))
}

fn parse_column(row: &Row, column: &Column, index: usize) -> Result<Value> {
  Ok(match column.decl_type() {
    Some("JSON") => parse_json_column(row, index)?,
    Some("TEXT") => parse_text_column(row, index)?,
    Some("INTEGER") => parse_integer_column(row, index)?,
    Some("REAL") => parse_real_column(row, index)?,
    Some("BLOB") => parse_blob_column(row, index)?,
    _ => panic!(
      "Unknown column type {decl_type}",
      decl_type = column.decl_type().unwrap_or_else(|| "UNKNOWN")
    ),
  })
}

fn parse_json_column(row: &Row, index: usize) -> Result<Value> {
  let json_string: String = row.get(index)?;
  let parsed: Value = serde_json::from_str(&json_string)?;
  Ok(parsed)
}

fn parse_text_column(row: &Row, index: usize) -> Result<Value> {
  let text: String = row.get(index)?;
  Ok(json!(text))
}

fn parse_integer_column(row: &Row, index: usize) -> Result<Value> {
  let integer: i64 = row.get(index)?;
  Ok(json!(integer))
}

fn parse_real_column(row: &Row, index: usize) -> Result<Value> {
  let real: f64 = row.get(index)?;
  Ok(json!(real))
}

fn parse_blob_column(row: &Row, index: usize) -> Result<Value> {
  let blob: Vec<u8> = row.get(index)?;
  Ok(json!(blob))
}
