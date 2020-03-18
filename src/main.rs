use std::fs::{File, OpenOptions};
use std::io::prelude::*;
use std::io::{Error, Cursor, SeekFrom};
use std::time::SystemTime;

extern crate byteorder;

use byteorder::{ByteOrder, LittleEndian, ReadBytesExt, WriteBytesExt};
use std::borrow::BorrowMut;
use std::collections::HashMap;

static DATA_DIR: &str = "./data";

// Struct stored in key dir to describe location of values in files
#[derive(Debug)]
pub struct ValueLocation {
    file_id: usize,
    timestamp: u64,
    value_pos: usize, // offset of value from start of file
    value_size: u16,  // size of value in bytes
}

// in-memory data structure pointing keys to their file locations
#[derive(Debug)]
pub struct KeyDirectory {
    key_dir: HashMap<String, ValueLocation>,
}

impl KeyDirectory {
    pub fn new() -> KeyDirectory {
        KeyDirectory {
            key_dir: HashMap::new(),
        }
    }

    pub fn get(&self, key: &str) -> Option<&ValueLocation> {
        self.key_dir.get(key)
    }
}

fn dat_file_name(data_dir: &str, i: usize) -> String {
    format!("{}/{}.dat", data_dir, i)
}

// Opens latest file and returns ownership
fn get_active_file(data_dir_path: &str) -> File {
    // open active file in data dir
    let data_files = std::fs::read_dir(data_dir_path).expect("Need to create data dir");

    let mut latest = 0;
    for (_i, file) in data_files.enumerate() {
        latest += 1;

        let file = file.unwrap();
        let path = file.path();
        println!("file in data dir: {:?}", path);
    }

    let mut active_file = {
        if latest == 0 {
            let file_path = dat_file_name(data_dir_path, 1);
            println!(
                "No files exist in data dir, creating first at: {}",
                file_path
            );
            File::create(file_path)
        } else {
            let file_path = dat_file_name(data_dir_path, latest);
            println!("opening latest data file in dir: {}", file_path);
            // open for append, dont truncate
            OpenOptions::new().append(true).open(file_path)
        }
    };

    let mut active_file = active_file.expect("Can't open active file");

    active_file
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

fn parse_file_into_key_dir(mut file: File, key_dir: &KeyDirectory) {
    println!("Parsing file {:?} into key dir", file);

    // parse row by row
    while let Some(result) = parse_entry(&mut file) {
        println!("Parsed row, got entry: {:?}", result);
    }
}

fn parse_entry(f: &mut File ) -> Option<RowEntry> {
    // read 3 byte crc
    let maybe_crc_bytes = get_bytes_from_file(f, 3);
    if let Err(e) = maybe_crc_bytes {
        println!("No more bytes, got error: {}", e);
        return None
    }
    let crc_bytes = maybe_crc_bytes.unwrap();

    let crc = string_from_bytes(crc_bytes);
    println!("crc = {}", crc);

    // read 8 byte timestamp
    let timestamp_bytes = get_bytes_from_file(f, 8).unwrap();
    let timestamp: u64 = LittleEndian::read_u64(&timestamp_bytes);
    println!("timestamp: {}", timestamp);

    // read 2 byte key size
    let key_size_bytes = get_bytes_from_file(f, 2).unwrap();
    let key_size = LittleEndian::read_u16(&key_size_bytes);
    println!("key size: {}", key_size);

    // read 2 byte val size
    let val_size_bytes = get_bytes_from_file(f, 2).unwrap();
    let value_size = LittleEndian::read_u16(&val_size_bytes);
    println!("val size: {}", value_size);

    // read <keysize> key
    let key_bytes = get_bytes_from_file(f, key_size as usize).unwrap();
    let key = string_from_bytes(key_bytes);
    println!("Key: {}", key);

    // GET VALUE POSITION
    let value_pos = f.seek(SeekFrom::Current(0)).unwrap();
    println!("Value starts at byte position: {}", value_pos);

    // read <valsize> value
    let val_bytes = get_bytes_from_file(f, value_size as usize).unwrap();
    let value = string_from_bytes(val_bytes);
    println!("Val: {}", value);

    Some(RowEntry {
        key,
        value,
        value_sz: value_size,
        value_pos,
        timestamp,
    })
}

pub fn read_val_in(data_dir: &str, file_id: usize, value_pos: u64, value_size: u16) -> Result<Option<String>, Error> {
    println!("Fetching value in file: {}, value_pos: {}, size: {}", file_id, value_pos, value_size);
    let file_path = dat_file_name(data_dir, file_id);
    let mut file = File::open(file_path)?;
    let new_pos = file.seek(SeekFrom::Start(value_pos))?;
    println!("Seeked to pos: {}", new_pos);
    let value_bytes = get_bytes_from_file_u16(&file, value_size);
    let value_string = string_from_bytes(value_bytes);
    Ok(Some(value_string))
}

pub fn init_key_dir(data_dir: &str) -> KeyDirectory {
    println!("Initializing key dir from data dir={}", data_dir);
    let key_dir = KeyDirectory::new();

    // get all files in data dir
    let files = std::fs::read_dir(data_dir).expect("Data dir doesnt exist");

    for file in files {
        let file = File::open(file.unwrap().path());
        parse_file_into_key_dir(file.unwrap(), &key_dir);
    }

    key_dir
}

// Db struct
pub struct DaveBase {
    active_file: File,
    current_file_index: usize,
    key_dir: KeyDirectory,
    data_dir: &'static str,
}

impl DaveBase {
    pub fn new(data_path: &'static str) -> DaveBase {
        // need to build key_dir from all files in data_dir
        let key_dir = init_key_dir(data_path);

        DaveBase {
            active_file: get_active_file(data_path),
            current_file_index: 0,
            key_dir,
            data_dir: data_path,
        }
    }

    pub fn get(&self, key: String) -> Result<Option<String>, Error> {
        // look up key in keydir
        let val_loc = self.key_dir.get(key.as_str());
        if let Some(val_loc) = val_loc {
            let file_id = val_loc.file_id;
            println!("key exists in file: {}", file_id);
            let file_path = dat_file_name(self.data_dir, file_id);
            println!("value in file: {}", file_path);

            Ok(Option::None)
        } else {
            println!("No key: {} in key_dir", key);
            Ok(Option::None)
        }
    }

    pub fn read_file_row(&self) -> Result<(), Error> {
        println!("Reading first row of active file");

        Ok(())
    }

    // Writes a key value entry into active file
    pub fn write_entry(&mut self, key: String, value: String) -> Result<(), Error> {
        // go to next offset based on current index
        println!("Seeking to current index:{}", self.current_file_index);
        self.active_file
            .seek(std::io::SeekFrom::Start(self.current_file_index as u64));
        println!("Writing entry: {} = {}", key, value);

        let key_bytes = key.into_bytes();
        let val_bytes = value.into_bytes();

        // get current timestamp represented by 64 int
        let secs = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .expect("Couldnt get system time")
            .as_secs();

        // convert u64 to byte array (u8)
        let timestamp_byte_array = int_64_to_byte_array(secs);

        let mut total_byte_offset = 0;

        ////// WRITE ROW
        let header = "CRC";
        total_byte_offset += self.active_file.write(header.as_bytes()).unwrap();

        // timestamp
        total_byte_offset += self.active_file.write(&timestamp_byte_array).unwrap();

        // key size (16 bits / 2 bytes)
        let ksz_byte_array = int_16_to_byte_array(key_bytes.len() as u16);
        total_byte_offset += self.active_file.write(&ksz_byte_array).unwrap();

        // value size (16 bits / 2 bytes)
        let value_size_byte_array = int_16_to_byte_array(val_bytes.len() as u16);
        total_byte_offset += self.active_file.write(&value_size_byte_array).unwrap();

        // Key
        total_byte_offset += self.active_file.write(&key_bytes).unwrap();

        // Value
        total_byte_offset += self.active_file.write(&val_bytes).unwrap();

        println!("Finished, total row bytes : {}", total_byte_offset);
        self.current_file_index += total_byte_offset;

        Ok(())
    }
}

// entries in file are append-only, look like this:
// [ CRC ][ tstamp (64bit int)][ key_size ][ value_size ][ key ][ value  ]

fn main() {
    let data_dir_path = DATA_DIR;
    println!("Starting davebase with data dir: {}", data_dir_path);

    let mut db = DaveBase::new(data_dir_path);
    println!("generating test file");

    db.write_entry("foi".to_string(), "bazz".to_string());
    db.write_entry("bong".to_string(), "howitzer".to_string());
}

#[cfg(test)]
mod test {
    use super::*;

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
        let result = parse_entry(&mut file);
        println!("Result: {:?}", result);
    }

    #[test]
    fn test_get_value() {
        let data_dir = "test-data";
        let db = DaveBase::new(data_dir);
        let result = db.get("foi".to_string());
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
    }
}
