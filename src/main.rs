use std::fs::{File, OpenOptions};
use std::io::prelude::*;
use std::io::Error;
use std::time::{SystemTime};
extern crate byteorder;

use byteorder::{LittleEndian, WriteBytesExt};

static DATA_DIR: &str = "./data";
fn dat_file_name(data_dir: &str, i: usize) -> String {
    format!("{}/{}.dat", data_dir, i)
}
// Opens latest file and returns ownership
fn get_active_file(data_dir_path: &str) -> File {
    // open active file in data dir
    let data_files = std::fs::read_dir(data_dir_path)
        .expect("Need to create data dir");


    let mut latest = 0;
    for (i, file) in data_files.enumerate() {
        latest += 1;

        let file = file.unwrap();
        let path = file.path();
        println!("file in data dir: {:?}", path);
    }

    let mut active_file = {
        if latest == 0 {
            let file_path = dat_file_name(data_dir_path, 1);
            println!("No files exist in data dir, creating first at: {}", file_path);
            File::create(file_path)
        } else {
            let file_path = dat_file_name(data_dir_path, latest);
            println!("opening latest data file in dir: {}", file_path);
            // open for append, dont truncate
            OpenOptions::new().append(true).open(file_path)
        }
    };

    let mut active_file = active_file
        .expect("Can't open active file");

    active_file
}

fn int_16_to_byte_array(i : u16) -> [u8; 2] {
    let mut byte_array = [0u8; std::mem::size_of::<u16>()];
    byte_array.as_mut()
        .write_u16::<LittleEndian>(i)
        .expect("Unable to write");
    byte_array
}

fn int_64_to_byte_array(i : u64) -> [u8; 8] {
    let mut timestamp_byte_array = [0u8; std::mem::size_of::<u64>()];
    timestamp_byte_array.as_mut()
        .write_u64::<LittleEndian>(i)
        .expect("Unable to write");
    timestamp_byte_array
}

// Db struct
pub struct DaveBase {
    active_file: File
}

impl DaveBase {

    pub fn new(data_path: &str) -> DaveBase {
        DaveBase{
            active_file: get_active_file(data_path)
        }
    }

    pub fn read_file_row(&self) -> Result<(), Error> {
        println!("Reading first row of active file");

    }

    // Writes a key value entry into active file
    pub fn write_entry(&mut self, key: String, value: String) -> Result<(), Error> {
        println!("Writing entry: {} = {}", key, value);
        // get size of key in bytes
        let key_bytes = key.into_bytes();
        let ksz = key_bytes.len();
        println!("key is {} bytes long", ksz);

        // get size of val in bytes
        let val_bytes = value.into_bytes();
        let valsz = val_bytes.len();
        println!("value is {} bytes long", valsz);

        // get current timestamp represented by 32bit int
        let timestamp = chrono::Utc::now();
        let tmstmp = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH)
            .expect("Couldnt get system time");
        let secs = tmstmp.as_secs();
        println!("Now systemtime = {} sizeof = {:?} bytes", secs, std::mem::size_of_val(&secs));
        // convert u64 to byte array (u8)
        let timestamp_byte_array = int_64_to_byte_array(secs);
        println!("timestamp byte array: {:?}  len = {}", timestamp_byte_array, timestamp_byte_array.len());

        // write row
        let header = "CRC";
        let header = self.active_file.write(header.as_bytes())
            .expect("Couldnt write header to file");
        println!("Header written size: {} bytes", header);

        // key size (16 bits / 2 bytes)
        println!("key size={}, sizeof={:?}", ksz, std::mem::size_of_val(&ksz));
        let ksz_byte_array = int_16_to_byte_array(ksz as u16);
        println!("ksz_byte_array= {:?} len = {}", ksz_byte_array, ksz_byte_array.len());
        let write_size = self.active_file.write(&ksz_byte_array)
            .expect("Couldnt write key size");
        println!("Write size in bytes: {:?}", write_size);

        // value size (16 bits / 2 bytes)
        println!("value size={}", valsz);
        // TODO reuse same buffer
        let value_size_byte_array = int_16_to_byte_array(valsz as u16);
        println!("value_size_byte_array= {:?} len = {}", value_size_byte_array, value_size_byte_array.len());
        let write_size = self.active_file.write(&value_size_byte_array)
            .expect("Couldnt write value size");
        println!("Write size in bytes: {:?}", write_size);

        // Key
        let write_size = self.active_file.write(&key_bytes)
            .expect("Couldnt write key");
        println!("Write size (key) in bytes: {:?}", write_size);

        // Value
        let write_size = self.active_file.write(&val_bytes)
            .expect("Couldnt write value");
        println!("Write size (val) in bytes: {:?}", write_size);

        Ok(())
    }
}

// entries in file are append-only, look like this:
// [ CRC ][ tstamp (64bit int)][ key_size ][ value_size ][ key ][ value  ]


fn main() {
    let data_dir_path = DATA_DIR;
    println!("Starting davebase with data dir: {}", data_dir_path);

    let mut db = DaveBase::new(data_dir_path);

    db.read_file_row();

//    db.write_entry("foo".to_string(), "ooogogosdjflaksfjlakwjlakjflwakjldskdjalskdjalkwjlakwlakwjdlakjwldkajwldkj".to_string());

}

