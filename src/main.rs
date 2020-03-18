use std::fs::{File, OpenOptions};
use std::io::prelude::*;
use std::io::{Error, Bytes};
use std::io;
use std::time::{SystemTime};
extern crate byteorder;

use byteorder::{LittleEndian, WriteBytesExt, ReadBytesExt};
use std::borrow::BorrowMut;

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

fn get_bytes_from_file(mut file: File, sz: usize) -> (File, String) {
    let mut buf = Vec::new();
    let mut i = 0;
    loop {
        if i >= sz {
            break;
        }
        println!("i ={} key_sz = {}", i, sz);
        buf.push(file.borrow_mut().read_u8().unwrap());
        i+=1;
    }
    (file, String::from(std::str::from_utf8(buf.as_slice()).unwrap()))
}

#[derive(Debug)]
pub struct RowEntry {
    key: String,
    value: String,
    value_sz: usize,
    value_pos: usize,
}

fn parse_file(path : String) -> RowEntry {
    let mut file = File::open(path).unwrap();
    // Read 3 byte CRC
    let mut crc_buff = [0u8; 3];
    let bytes_read = file.read(&mut crc_buff).unwrap();
    let crc_string = std::str::from_utf8(&crc_buff).unwrap();

    // Read 8 byte timestamp
    // TODO missing

    // Read 2 byte key size
    let mut sz_buf = [0u8;2];
    let bytes_read = file.read(&mut sz_buf).unwrap();
    let key_sz = sz_buf[0].clone() as usize;
    println!("Key size.{} bytes read: {}, buffer => {:?}", key_sz, bytes_read, sz_buf);
    // TODO: hack, just taking first byte and using digit

    // Read 2 byte value size
    let bytes_read = file.read(&mut sz_buf).unwrap();
    let val_sz = sz_buf[0].clone() as usize;

    let (file, key_string) = get_bytes_from_file(file, key_sz);
    let (file, val_string) = get_bytes_from_file(file, val_sz);

    RowEntry{
        key: key_string,
        value: val_string,
        value_sz: val_sz,
        value_pos: (3 + 2 + 2 + key_sz)
    }
}

// Db struct
pub struct DaveBase {
    active_file: File,
    current_file_index: usize,
}

impl DaveBase {

    pub fn new(data_path: &str) -> DaveBase {
        DaveBase{
            active_file: get_active_file(data_path),
            current_file_index: 0
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
        self.active_file.seek(std::io::SeekFrom::Start(self.current_file_index as u64));
        println!("Writing entry: {} = {}", key, value);

        let key_bytes = key.into_bytes();
        let val_bytes = value.into_bytes();

        // get current timestamp represented by 64 int
        let secs = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH)
            .expect("Couldnt get system time").as_secs();

        // convert u64 to byte array (u8)
        let timestamp_byte_array = int_64_to_byte_array(secs);
        //TODO: write this

        let mut total_byte_offset = 0;

        ////// WRITE ROW
        let header = "CRC";
        total_byte_offset += self.active_file.write(header.as_bytes()).unwrap();

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

    db.write_entry("foi".to_string(), "bazz".to_string());
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_parse_file() {
        let path = "data/1.dat";
        let row = parse_file(String::from(path));
        println!("parsed row {:?}", row);
    }

}