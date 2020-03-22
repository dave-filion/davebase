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

// 10kb per file
const MAX_BYTES_PER_FILE: u64 = 10 * 1024;

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

fn parse_msg_into_string(mut stream: &TcpStream) -> String {
    // stream read buffer
    let mut buff = [0 as u8; 128];

    // parse stream input
    // TODO: error handling
    let size = stream.read(&mut buff).expect("failed to read from stream");
    from_utf8(&buff[0..size]).unwrap().to_string()
}

// start tcp listener
fn start_server(mut db: DaveBase) -> std::io::Result<()> {
    let listener = TcpListener::bind("127.0.0.1:3333")?;
    info!("Waiting for messages on 3333");
    for stream in listener.incoming() {
        let mut stream = stream.expect("Cant unwrap stream");
        let msg = parse_msg_into_string(&stream);
        debug!("INCOMING MSG -> {}", msg);

        // seperate message by whitespace
        let all_args: Vec<&str> = msg.split_whitespace().collect();

        // validate args
        if all_args.is_empty() {
            warn!("Not enough args, responding with help text");
            stream.write(b"Not valid input, send a correct command");
            continue;
        }

        // if only 1 arg, check if valid command
        if all_args.len() == 1 {
            match all_args[0].to_uppercase().as_str() {
                "MERGE" => {
                    info!("Merge command received...");
                    stream.write(b"OK");
                }
                "CLEARALL" => {
                    info!("Clear command received...");
                    // clear data files and reinit db
                    db.clear_all_data();
                    stream.write(b"OK");
                }
                _ => {
                    warn!("Unknown command: {}", all_args[0]);
                }
            }
            continue;
        }

        let cmd = all_args[0];
        let key = all_args[1];

        // make upper to make matching easier
        match cmd.to_uppercase().as_str() {
            "SET" => {
                let val = all_args[2];
                db.set(key.to_string(), val.to_string());
                // respond with OK
                stream.write(b"OK");
            }
            "GET" => {
                let result = db.get(key).unwrap();
                match result {
                    Some(val) => {
                        // return value
                        stream.write(val.into_bytes().as_slice());
                    }
                    None => {
                        // write nil
                        stream.write(b"NIL");
                    }
                }
            }
            _ => warn!("Unknown command: {}", cmd),
        }
    }
    Ok(())
}

fn test_with_random_words(db: &mut DaveBase) {
    // Start test
    info!("Loading dictionary...");
    let all_words = load_word_dict();
    debug!("{} words loaded", all_words.len());

    info!("Clearing data...");
    DaveBase::clear_data("data");

    // insert 100 random key/values
    for _ in 0..100 {
        let rand_key = get_rand_word(&all_words);
        let rand_val = get_rand_word(&all_words);
        let _ = db.set(rand_key, rand_val);
    }
}

fn clear_data(data_dir: &str) {
    info!("Clearing all data");
    DaveBase::clear_data(data_dir);
}

fn main() -> Result<(), Error> {
    // Load env variables and init logger
    dotenv::dotenv().ok();
    env_logger::init();

    let data_dir = "data";

    info!("Starting davebase...");
    let mut db = DaveBase::new(data_dir, MAX_BYTES_PER_FILE);
    start_server(db);

    Ok(())
}
