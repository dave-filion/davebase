#[macro_use]
extern crate log;
use davebase::*;
use rand::seq::SliceRandom;
use std::fs::File;
use std::io::prelude::*;
use std::io::Error;
use std::net::{TcpListener, TcpStream};
use std::str::from_utf8;

// 10kb per file
const MAX_BYTES_PER_FILE: u64 = 10 * 1024;

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
        let arg_len = all_args.len();

        // validate args
        if arg_len == 0 {
            warn!("Not enough args, responding with help text");
            // TODO error check
            let _ = stream.write(b"Not valid input, send a correct command");
            continue;
        }

        // if only 1 arg, check if valid command
        if arg_len == 1 {
            match all_args[0].to_uppercase().as_str() {
                "MERGE" => {
                    info!("Merge command received...");
                    //TODO error check
                    let _ = stream.write(b"OK");
                }
                "CLEARALL" => {
                    info!("Clear command received...");
                    // clear data files and reinit db
                    db.clear_all_data();

                    // error check
                    let _ = stream.write(b"OK");
                }
                "SIZE" => {
                    info!("Get total size received...");
                    let result = db.get_total_data_size();
                    if let Ok(size) = result {
                        let result_string = format!("size={} bytes", size);
                        let _ = stream.write(result_string.into_bytes().as_slice());
                    } else {
                        let _ = stream.write(b"Couldnt fetch size");
                    }
                }
                _ => {
                    warn!("Unknown command: {}", all_args[0]);
                    let _ = stream.write(b"Unknown command");
                }
            }
            continue;
        }

        // valid 2+ arg commands

        let cmd = all_args[0];
        let key = all_args[1];

        // make upper to make matching easier
        match cmd.to_uppercase().as_str() {
            "SET" => {
                // TODO error check
                let _ = db.set(key.to_string(), all_args[2].to_string());

                // respond with OK
                // TODO error check
                let _ = stream.write(b"OK");
            }
            "GET" => {
                let result = db.get(key).unwrap();
                match result {
                    Some(val) => {
                        // return value
                        // TODO error check
                        let _ = stream.write(val.into_bytes().as_slice());
                    }
                    None => {
                        // write nil
                        // TODO error check
                        let _ = stream.write(b"NIL");
                    }
                }
            }
            _ => warn!("Unknown command: {}", cmd),
        }
    }
    Ok(())
}

fn main() -> Result<(), Error> {
    // Load env variables and init logger
    dotenv::dotenv().ok();
    env_logger::init();

    let data_dir = "data";

    let db = DaveBase::new(data_dir, MAX_BYTES_PER_FILE);
    start_server(db)
}
