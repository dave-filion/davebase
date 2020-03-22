use std::fs::{File, OpenOptions};
use std::io::prelude::*;
use std::io::{Error, SeekFrom};
use std::path::PathBuf;
use std::time::SystemTime;
#[macro_use]
extern crate log;

extern crate byteorder;

use byteorder::{ByteOrder, LittleEndian, ReadBytesExt, WriteBytesExt};
use std::borrow::BorrowMut;
use std::collections::HashMap;

// Struct stored in key dir to describe location of values in files
#[derive(Debug)]
pub struct ValueLocation {
    file_id: usize,
    timestamp: u64,
    value_pos: u64,  // offset of value from start of file
    value_size: u16, // size of value in bytes
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
    for (_i, _file) in data_files.enumerate() {
        latest += 1;
    }

    let (active_file, file_id) = {
        if latest == 0 {
            let file_path = dat_file_name(data_dir_path, 1);
            debug!(
                "No files exist in data dir, creating first at: {}",
                file_path.to_str().unwrap()
            );
            (File::create(file_path), 1)
        } else {
            let file_path = dat_file_name(data_dir_path, latest);
            (OpenOptions::new().append(true).open(file_path), latest)
        }
    };

    let active_file = active_file.expect("Can't open active file");

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

    let mut file = File::open(path).expect("Cannot open data file");

    // parse row by row
    while let Some(result) = parse_row(&mut file) {
        key_dir.insert(
            result.key,
            ValueLocation {
                file_id,
                timestamp: result.timestamp,
                value_pos: result.value_pos,
                value_size: result.value_sz,
            },
        );
    }
}

// Parses current row at cursor in file, returns it or None if end of file
fn parse_row(f: &mut File) -> Option<RowEntry> {
    // read 3 byte crc
    let maybe_crc_bytes = get_bytes_from_file(f, 3);
    if let Err(_) = maybe_crc_bytes {
        return None;
    }

    // TODO: should validate on the first section
    let crc_bytes = maybe_crc_bytes.unwrap();
    let _crc = string_from_bytes(crc_bytes);

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
pub fn read_val_in(
    data_dir: &str,
    file_id: usize,
    value_pos: u64,
    value_size: u16,
) -> Result<Option<String>, Error> {
    let file_path = dat_file_name(data_dir, file_id);
    let mut file = File::open(file_path)?;
    file.seek(SeekFrom::Start(value_pos))?;
    let value_bytes = get_bytes_from_file_u16(&file, value_size);
    let value_string = string_from_bytes(value_bytes);
    Ok(Some(value_string))
}

// Initializes key_dir map given files in data_dir
pub fn init_key_dir(data_dir: &str) -> KeyDirectory {
    debug!("Initializing key dir from data dir={}", data_dir);
    let mut key_dir = KeyDirectory::new();

    // get all files in data dir
    let files = std::fs::read_dir(data_dir).expect("Data dir doesnt exist");

    // find all file ids
    let mut file_ids = Vec::new();
    for file in files {
        file_ids.push(
            file.unwrap()
                .path()
                .file_stem()
                .unwrap()
                .to_str()
                .unwrap()
                .parse::<usize>()
                .unwrap(),
        );
    }

    // sort ids in order (lower is older)
    file_ids.sort();

    for id in file_ids {
        parse_file_into_key_dir(id, data_dir, &mut key_dir);
    }

    debug!("KEY-DIR AFTER INIT: {:#?}", key_dir);

    key_dir
}

// Db struct
pub struct DaveBase {
    active_file: File,
    active_file_id: usize,
    key_dir: KeyDirectory,
    data_dir: &'static str,
    max_bytes_per_file: u64,
}

impl Drop for DaveBase {
    fn drop(&mut self) {
        debug!("Dropping davebase struct!");
    }
}

fn init_db(data_dir: &str) -> (KeyDirectory, usize, File) {
    // need to build key_dir from all files in data_dir
    let key_dir = init_key_dir(data_dir);
    let (active_file_id, active_file) = get_active_file(data_dir);
    debug!("Creating new DB with active_file_id = {}", active_file_id);
    (key_dir, active_file_id, active_file)
}

impl DaveBase {
    pub fn new(data_dir: &'static str, max_bytes_per_file: u64) -> DaveBase {
        info!(
            "Creating db with data_dir={} and max_bytes_per_file = {}",
            data_dir, max_bytes_per_file
        );
        let (key_dir, active_file_id, active_file) = init_db(data_dir);

        DaveBase {
            active_file,
            active_file_id,
            key_dir,
            data_dir,
            max_bytes_per_file,
        }
    }

    pub fn get(&self, key: &str) -> Result<Option<String>, Error> {
        // look up key in keydir
        let val_loc = self.key_dir.get(key);
        if let Some(val_loc) = val_loc {
            let file_id = val_loc.file_id;
            let result = read_val_in(
                self.data_dir,
                file_id,
                val_loc.value_pos,
                val_loc.value_size,
            );
            Ok(result.unwrap())
        } else {
            debug!("No key: {} in key_dir", key);
            Ok(Option::None)
        }
    }

    // Writes a key value entry into active file
    pub fn set(&mut self, key: String, value: String) -> Result<(), Error> {
        debug!(
            "Setting {} -> {} (active file: {})",
            key, value, self.active_file_id
        );
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
        let val_loc = ValueLocation {
            file_id: self.active_file_id,
            timestamp: secs,
            value_pos,
            value_size,
        };

        self.key_dir.insert(key.clone(), val_loc);

        // if current position is greater then max file size, create a new file
        // and move the active file pointer here
        //TODO: using value_pos as lose reference now, do it right
        if value_pos > self.max_bytes_per_file {
            debug!("ACTIVE FILE HAS MORE BYTES THEN MAX: {}", value_pos);
            let new_file_id = self.active_file_id + 1;
            let file_path = dat_file_name(self.data_dir, new_file_id);
            debug!(
                "Created new file: {}, setting as active",
                file_path.to_str().unwrap()
            );
            let new_file = File::create(file_path).expect("Cant open new active file");
            self.active_file = new_file;
            self.active_file_id = new_file_id;
            // TODO: should ONLY active_file_id be tracked?
        }
        Ok(())
    }

    // clears all data files in given dir
    pub fn clear_data(dir: &str) {
        let files = std::fs::read_dir(dir).expect("Data dir doesnt exist");

        let mut paths = Vec::new();
        for file in files {
            paths.push(file.unwrap().path());
        }

        for path in paths {
            debug!("Deleting data file at path: {:?}", path);
            // TODO: error check
            let _ = std::fs::remove_file(path);
        }
    }

    // deletes data files and reinits db (clear key_dir)
    pub fn clear_all_data(&mut self) {
        info!("Clearing all data...");
        DaveBase::clear_data(self.data_dir);
        // reinit db
        let (key_dir, active_file_id, active_file) = init_db(self.data_dir);
        self.key_dir = key_dir;
        self.active_file_id = active_file_id;
        self.active_file = active_file;
    }
}
#[cfg(test)]
mod test;
