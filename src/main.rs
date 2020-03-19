#[macro_use] extern crate log;
use log::Level;
use std::io::{Error};
use davebase::*;
use std::fs::{File, OpenOptions};
use std::io::prelude::*;
use rand::seq::SliceRandom;

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

fn get_rand_word(all_words : &Vec<String>) -> String {
    all_words.choose(&mut rand::thread_rng())
        .expect("couldnt select random word")
        .clone()
}

fn main() -> Result<(), Error>{
    // Load env variables and init logger
    dotenv::dotenv().ok();
    env_logger::init();

    // Start test
    info!("Loading dictionary...");
    let all_words = load_word_dict();
    debug!("{} words loaded", all_words.len());


    info!("Starting davebase...");

    // clears existing data files in dir
    DaveBase::clear_data("data");

    let mut db = DaveBase::new("data");

    for _ in 0..100 {
        let rand_key= get_rand_word(&all_words);
        let rand_val = get_rand_word(&all_words);
        db.set(rand_key, rand_val);
    }

    Ok(())
}

