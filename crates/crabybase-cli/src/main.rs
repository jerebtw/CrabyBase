use crabybase_db;

fn main() {
  let data_pool = crabybase_db::sqlite::connect_data_pool().unwrap();
  let log_pool = crabybase_db::sqlite::connect_log_pool().unwrap();
  println!("{:#?}", data_pool);
  println!("{:#?}", log_pool);
}
