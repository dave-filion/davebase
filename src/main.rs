#[macro_use] extern crate log;
use log::Level;
use std::io::{Error};
use davebase::*;


fn main() -> Result<(), Error>{
    // Load env variables and init logger
    dotenv::dotenv().ok();
    env_logger::init();

    if log_enabled!(Level::Debug) {
        debug!("arg len = {}", std::env::args().len());
    }

    info!("Starting davebase...");

    let _db = DaveBase::new("data");

//    let data_dir_path = DATA_DIR;
//    println!("Starting davebase with data dir: {}", data_dir_path);
//
//    let mut db = DaveBase::new(data_dir_path);
//
//    db.set(String::from("heyo"), String::from("comeon there it is"));
//
//    let val = db.get("heyo")?;
//    println!("value = {}", val.unwrap());

    Ok(())
}

