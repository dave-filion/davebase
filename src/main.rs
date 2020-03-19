#[macro_use] extern crate log;
use log::Level;
use std::io::{Error};
use davebase::*;
use std::fs::{File, OpenOptions};
use std::io::prelude::*;

// loads dictionary file and returns vec of all words
fn load_word_dict() -> Vec<String> {
    let mut f = File::open("words.txt")
        .expect("Need words.txt");
    let mut big_string = String::new();
    let result = f.read_to_string(&mut big_string)
        .expect("couldnt read dict to string");

    debug!("Read {} bytes into string", result);

    let all_words : Vec<String> = big_string.split("\n").map(|s| s.to_string()).collect();
    all_words
}

fn main() -> Result<(), Error>{
    // Load env variables and init logger
    dotenv::dotenv().ok();
    env_logger::init();

    info!("Loading dictionary...");
    let all_words = load_word_dict();

    debug!("{} words loaded", all_words.len());

    if log_enabled!(Level::Debug) {
        debug!("arg len = {}", std::env::args().len());
    }

    info!("Starting davebase...");

    let _db = DaveBase::new("data");

    Ok(())
}

