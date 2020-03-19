use std::fs::{File, OpenOptions};
use std::io::prelude::*;
use std::io::{Error, SeekFrom};
use std::time::SystemTime;
use std::path::{Path, PathBuf};

extern crate byteorder;

use byteorder::{ByteOrder, LittleEndian, ReadBytesExt, WriteBytesExt};
use std::borrow::BorrowMut;
use std::collections::HashMap;

static DATA_DIR: &str = "./data";
const MAX_BYTES : u64 = 512; // max bytes for file before new one is created

// Struct stored in key dir to describe location of values in files
#[derive(Debug)]
pub struct ValueLocation {
    file_id: usize,
    timestamp: u64,
    value_pos: u64, // offset of value from start of file
    value_size: u16,  // size of value in bytes
}

// in-memory data structure pointing keys to their file locations
type KeyDirectory = HashMap<String, ValueLocation>;


fn dat_file_name(data_dir: &str, i: usize) -> PathBuf {
    let mut path = PathBuf::new();
    path.push(data_dir);
    path.push(format!("{}", i));
    path.set_extension("dat");
    path
}

// Opens latest file and returns ownership
fn get_active_file(data_dir_path: &str) -> (usize, File) {
    // open active file in data dir
    let data_files = std::fs::read_dir(data_dir_path).expect("Need to create data dir");

    let mut latest = 0;
    for (_i, file) in data_files.enumerate() {
        latest += 1;

        let file = file.unwrap();
        let path = file.path();
    }

    let (mut active_file, file_id) = {
        if latest == 0 {
            let file_path = dat_file_name(data_dir_path, 1);
            println!(
                "No files exist in data dir, creating first at: {}",
                file_path.to_str().unwrap()
            );
            (File::create(file_path), 1)
        } else {
            let file_path = dat_file_name(data_dir_path, latest);
            (OpenOptions::new().append(true).open(file_path), latest)
        }
    };

    let mut active_file = active_file.expect("Can't open active file");

    (file_id, active_file)
}

fn int_16_to_byte_array(i: u16) -> [u8; 2] {
    let mut byte_array = [0u8; std::mem::size_of::<u16>()];
    byte_array
        .as_mut()
        .write_u16::<LittleEndian>(i)
        .expect("Unable to write");

    byte_array
}

// consumes vec and returns string
fn string_from_bytes(v: Vec<u8>) -> String {
    String::from(std::str::from_utf8(v.as_slice()).unwrap())
}

fn int_64_to_byte_array(i: u64) -> [u8; 8] {
    let mut timestamp_byte_array = [0u8; std::mem::size_of::<u64>()];
    timestamp_byte_array
        .as_mut()
        .write_u64::<LittleEndian>(i)
        .expect("Unable to write");
    timestamp_byte_array
}

fn get_bytes_from_file_u16(mut file: &File, sz: u16) -> Vec<u8> {
    let mut buf = Vec::new();
    for _ in 0..sz {
        buf.push(file.borrow_mut().read_u8().unwrap());
    }
    buf
}

// given file, gets next sz bytes and returns in buffer
fn get_bytes_from_file(mut file: &File, sz: usize) -> Result<Vec<u8>, Error> {
    let mut buf = Vec::new();
    for _ in 0..sz {
        let byte = file.read_u8()?;
        buf.push(byte);
    }
    Ok(buf)
}

#[derive(Debug)]
pub struct RowEntry {
    key: String,
    value: String,
    value_sz: u16,
    value_pos: u64,
    timestamp: u64,
}

fn parse_file_into_key_dir(file_id: usize, data_dir: &str, key_dir: &mut KeyDirectory) {
    let path = dat_file_name(data_dir, file_id);

    let mut file = File::open(path)
        .expect("Cannot open data file");

    // parse row by row
    while let Some(result) = parse_row(&mut file) {
        key_dir.insert(result.key, ValueLocation{
            file_id,
            timestamp: result.timestamp,
            value_pos: result.value_pos,
            value_size: result.value_sz,
        });
    }
}

// Parses current row at cursor in file, returns it or None if end of file
fn parse_row(f: &mut File ) -> Option<RowEntry> {
    // read 3 byte crc
    let maybe_crc_bytes = get_bytes_from_file(f, 3);
    if let Err(e) = maybe_crc_bytes {
        return None
    }
    let crc_bytes = maybe_crc_bytes.unwrap();

    let crc = string_from_bytes(crc_bytes);

    // read 8 byte timestamp
    let timestamp_bytes = get_bytes_from_file(f, 8).unwrap();
    let timestamp: u64 = LittleEndian::read_u64(&timestamp_bytes);

    // read 2 byte key size
    let key_size_bytes = get_bytes_from_file(f, 2).unwrap();
    let key_size = LittleEndian::read_u16(&key_size_bytes);

    // read 2 byte val size
    let val_size_bytes = get_bytes_from_file(f, 2).unwrap();
    let value_sz = LittleEndian::read_u16(&val_size_bytes);

    // read <keysize> key
    let key_bytes = get_bytes_from_file(f, key_size as usize).unwrap();
    let key = string_from_bytes(key_bytes);

    // GET VALUE POSITION
    let value_pos = f.seek(SeekFrom::Current(0)).unwrap();

    // read <valsize> value
    let val_bytes = get_bytes_from_file(f, value_sz as usize).unwrap();
    let value = string_from_bytes(val_bytes);

    Some(RowEntry {
        key,
        value,
        value_sz,
        value_pos,
        timestamp,
    })
}

// Fetches value from data_dir/file_id at position and size
pub fn read_val_in(data_dir: &str, file_id: usize, value_pos: u64, value_size: u16) -> Result<Option<String>, Error> {
    let file_path = dat_file_name(data_dir, file_id);
    let mut file = File::open(file_path)?;
    let new_pos = file.seek(SeekFrom::Start(value_pos))?;
    let value_bytes = get_bytes_from_file_u16(&file, value_size);
    let value_string = string_from_bytes(value_bytes);
    Ok(Some(value_string))
}

// Initializes key_dir map given files in data_dir
pub fn init_key_dir(data_dir: &str) -> KeyDirectory {
    println!("Initializing key dir from data dir={}", data_dir);
    let mut key_dir = KeyDirectory::new();

    // get all files in data dir
    let files = std::fs::read_dir(data_dir).expect("Data dir doesnt exist");

    // find all file ids
    let mut file_ids = Vec::new();
    for file in files {
        file_ids.push(file.unwrap()
            .path()
            .file_stem().unwrap()
            .to_str().unwrap()
            .parse::<usize>().unwrap());
    }

    // sort ids in order (lower is older)
    file_ids.sort();

    for id in file_ids {
        parse_file_into_key_dir(id, data_dir, &mut key_dir);
    }

    println!("KEY-DIR AFTER INIT: {:#?}", key_dir);

    key_dir
}

// Db struct
pub struct DaveBase {
    active_file: File,
    active_file_id: usize,
    key_dir: KeyDirectory,
    data_dir: &'static str,
}

impl DaveBase {
    pub fn new(data_dir: &'static str) -> DaveBase {
        // need to build key_dir from all files in data_dir
        let key_dir = init_key_dir(data_dir);
        let (active_file_id, active_file) = get_active_file(data_dir);
        println!("Creating new DB with active_file_id = {}", active_file_id);

        DaveBase {
            active_file,
            active_file_id,
            key_dir,
            data_dir,
        }
    }

    pub fn get(&self, key: &str) -> Result<Option<String>, Error> {
        // look up key in keydir
        let val_loc = self.key_dir.get(key);
        if let Some(val_loc) = val_loc {
            let file_id = val_loc.file_id;
            let result = read_val_in(self.data_dir, file_id, val_loc.value_pos, val_loc.value_size);
            Ok(result.unwrap())
        } else {
            println!("No key: {} in key_dir", key);
            Ok(Option::None)
        }
    }

    // Writes a key value entry into active file
    pub fn set(&mut self, key: String, value: String) -> Result<(), Error> {
        println!("Setting {} -> {} (active file: {})", key, value, self.active_file_id);
        let key_bytes = key.as_bytes();
        let val_bytes = value.as_bytes();

        // get current timestamp represented by 64 int
        let secs = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .expect("Couldnt get system time")
            .as_secs();
        let timestamp_byte_array = int_64_to_byte_array(secs.clone());

        ////// WRITE ROW
        let header = "CRC";
        self.active_file.write(header.as_bytes()).unwrap();

        // timestamp
        self.active_file.write(&timestamp_byte_array).unwrap();

        // key size (16 bits / 2 bytes)
        let ksz_byte_array = int_16_to_byte_array(key_bytes.len() as u16);
        self.active_file.write(&ksz_byte_array).unwrap();

        // value size (16 bits / 2 bytes)
        let value_size = val_bytes.len() as u16;
        let value_size_byte_array = int_16_to_byte_array(value_size);
        self.active_file.write(&value_size_byte_array).unwrap();

        // Key
        self.active_file.write(&key_bytes).unwrap();

        // Make note of value start position for key_dir reference
        let value_pos = self.active_file.seek(SeekFrom::Current(0)).unwrap();

        // Value
        self.active_file.write(&val_bytes).unwrap();

        // Add entry to key dir or update
        let val_loc = ValueLocation{
                file_id: self.active_file_id,
                timestamp: secs,
                value_pos,
                value_size,
            };

        self.key_dir.insert(key.clone(), val_loc);

        // if current position is greater then max file size, create a new file
        // and move the active file pointer here
        //TODO: using value_pos as lose reference now, do it right
        if value_pos > MAX_BYTES{
            println!("ACTIVE FILE HAS MORE BYTES THEN MAX: {}", value_pos);
            let new_file_id = self.active_file_id + 1;
            let file_path = dat_file_name(self.data_dir, new_file_id);
            println!("Created new file: {}, setting as active", file_path.to_str().unwrap());
            let new_file = File::create(file_path).expect("Cant open new active file");
            self.active_file = new_file;
            self.active_file_id = new_file_id;
            // TODO: should ONLY active_file_id be tracked?
        }

        Ok(())
    }
}
#[cfg(test)]
mod test {
    use super::*;
    use chrono::{NaiveDateTime, Utc, TimeZone, FixedOffset};

    #[test]
    fn test_bytes_from_file_and_convert_to_string() {
        let path = "test-data/1.dat";
        let file = File::open(path).unwrap();
        let result = get_bytes_from_file(&file, 3).unwrap();
        assert_eq!(result.len(), 3);

        let string = string_from_bytes(result);
        assert_eq!(string, "CRC");

        // get next bytes and convert to timestamp
        let timestamp_bytes = get_bytes_from_file(&file, 8).unwrap();
        let timestamp: u64 = LittleEndian::read_u64(&timestamp_bytes);
        println!("timestamp: {}", timestamp);
        assert_eq!(1584550857, timestamp);
    }

    #[test]
    fn test_parse_row_entry() {
        let mut file = File::open("test-data/1.dat").unwrap();
        let result = parse_row(&mut file);
        println!("Result: {:?}", result);
        assert!(result.is_some());
        let result = result.unwrap();
        assert_eq!(result.key, "foi".to_string());
        assert_eq!(result.value, "bazz".to_string());
        assert_eq!(result.value_sz, 4);
        assert_eq!(result.value_pos, 18);
        assert_eq!(result.timestamp, 1584550857);
    }

    #[test]
    fn test_get_value() {
        let data_dir = "test-data";
        let db = DaveBase::new(data_dir);
        let result = db.get("foi");
    }

    #[test]
    fn test_get_value_from_file() {
        let data_dir = "test-data";
        // read value in file 1
        // should be bazz
        let result = read_val_in(data_dir, 1, 18, 4)
            .expect("Cant read val")
            .unwrap();
        assert_eq!("bazz", result);
    }

    #[test]
    fn test_init_key_dir() {
        let data_dir = "test-data";
        let key_dir = init_key_dir(data_dir);
        println!("{:?}", key_dir);
        assert_eq!(2, key_dir.keys().len());
        let foi_key = key_dir.get("foi");
        // should be in file 2
        assert_eq!(2, foi_key.unwrap().file_id);
        // in position 18
        assert_eq!(18, foi_key.unwrap().value_pos);
        // with size of 4
        assert_eq!(4, foi_key.unwrap().value_size);

        let bong = key_dir.get("bong").unwrap();
        assert_eq!(2, bong.file_id);
        assert_eq!(41, bong.value_pos);
        assert_eq!(8, bong.value_size);

        let non_key = key_dir.get("whahaha");
        // should be error
        match non_key {
            Some(_) => panic!("shouldnt happen"),
            None => assert_eq!(1, 1),
        }
    }

    #[test]
    fn test_get_from_db() {
        let data_dir = "test-data";
        let db = DaveBase::new(data_dir);
        let result = db.get("bong");
        match result {
            Ok(r) => {
                if let Some(value) = r {
                    // good!
                    println!("value is: {}", value);
                    assert_eq!(value, "howitzer");
                } else {
                    panic!("Should have been a value");
                }
            },
            Err(e) => {
                panic!("Shouldnt be an error")
            },
        }
    }

    #[test]
    fn test_set_works() {
        let data_dir = "data";
        let mut db = DaveBase::new(data_dir);
        db.set(String::from("fooz"), String::from("baz"));
        db.set(String::from("something"), String::from("somethingelse"));
        db.set(String::from("fooz"), String::from("goop"));

        println!("KEY-DIR after sets: {:#?}", db.key_dir);

        let val_string = db.get("fooz").unwrap();
        if let Some(val) = val_string {
            assert_eq!(val, "goop");
        } else {
            panic!("No val found!");
        }
    }

    #[test]
    fn test_decode_timestamp_field() {
        let data_dir = "data";
        let mut db = DaveBase::new(data_dir);
        db.set(String::from("yo"), String::from("dude"));
        let val_loc = db.key_dir.get("yo").unwrap();
        let t = val_loc.timestamp;
        let timestamp = NaiveDateTime::from_timestamp(t as i64, 0);

        db.set(String::from("dv:23"), String::from("laksjdlakjsdlak"));
        let val_loc = db.key_dir.get("dv:23").unwrap();
        let t2 = val_loc.timestamp;
        let timestamp2 = NaiveDateTime::from_timestamp(t2 as i64, 0);
        println!("t1 = {:?} t2 = {:?}", timestamp.to_string(), timestamp2.to_string());
    }

    #[test]
    fn test_dat_file_name() {
        let data_dir = "data";
        let path = dat_file_name(data_dir, 23);
        println!("path = {}", path.to_str().unwrap());
    }
}
