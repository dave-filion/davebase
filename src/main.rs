use std::fs::{File, OpenOptions};
use std::io::prelude::*;
use std::io::{Error, Cursor, SeekFrom};
use std::time::SystemTime;

use davebase::*;
extern crate byteorder;

#[macro_use] extern crate log;
use log::Level;

static DATA_DIR : &str = "./data";

fn main() -> Result<(), Error>{
    dotenv::dotenv().ok();

    env_logger::init();

    if log_enabled!(Level::Debug) {
        debug!("arg len = {}", std::env::args().len());
        for arg in std::env::args() {
            println!("arg => {}", arg);
        }
    }

    info!("Starting davebase...");


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

