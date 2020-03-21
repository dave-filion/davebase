#[macro_use]
extern crate log;
use davebase::*;
use rand::seq::SliceRandom;
use std::fs::File;
use std::io::prelude::*;
use std::io::Error;
use std::net;
use std::net::{Shutdown, TcpListener, TcpStream};
use std::str::from_utf8;

// loads dictionary file and returns vec of all words
fn load_word_dict() -> Vec<String> {
    let mut f = File::open("words.txt").expect("Need words.txt");
    let mut big_string = String::new();
    let result = f
        .read_to_string(&mut big_string)
        .expect("couldnt read dict to string");
    debug!("Read {} bytes into string", result);
    big_string.split("\n").map(|s| s.to_string()).collect()
}

fn get_rand_word(all_words: &Vec<String>) -> String {
    all_words
        .choose(&mut rand::thread_rng())
        .expect("couldnt select random word")
        .clone()
}

fn parse_msg_into_string(mut stream: TcpStream) -> String {
    // stream read buffer
    let mut buff = [0 as u8; 128];

    // parse stream input
    // TODO: error handling
    stream.read(&mut buff).expect("failed to read from stream");
    from_utf8(&buff).unwrap().to_string()
}

// start tcp listener
fn start_server(_db: DaveBase) -> std::io::Result<()> {
    let listener = TcpListener::bind("127.0.0.1:3333")?;
    info!("Waiting for messages on 3333");
    for stream in listener.incoming() {
        info!("Got something from stream...");
        let msg = parse_msg_into_string(stream?);
        info!("MSG -> {}", msg);
    }
    Ok(())
}

fn main() -> Result<(), Error> {
    // Load env variables and init logger
    dotenv::dotenv().ok();
    env_logger::init();

    // Start test
    info!("Loading dictionary...");
    let all_words = load_word_dict();
    debug!("{} words loaded", all_words.len());

    info!("Clearing data...");
    DaveBase::clear_data("data");

    info!("Starting davebase...");
    let mut db = DaveBase::new("data");

    // insert 100 random key/values
    for _ in 0..100 {
        let rand_key = get_rand_word(&all_words);
        let rand_val = get_rand_word(&all_words);
        let _ = db.set(rand_key, rand_val);
    }

    // Start tcp listener
    start_server(db);

    Ok(())
}
