use std::fs::{File, OpenOptions};
use std::io::prelude::*;
use std::io::Error;


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

    // Writes a key value entry into active file
    pub fn write_entry(&self, key: String, value: String) -> Result<(), Error> {
        println!("Writing entry: {} = {}", key, value);
        Ok(())
    }
}

// entries in file are append-only, look like this:
// [ CRC ][ tstamp (32bit int)][ key_size ][ value_size ][ key ][ value  ]


fn main() {
    let data_dir_path = DATA_DIR;
    println!("Starting davebase with data dir: {}", data_dir_path);

    let db = DaveBase::new(data_dir_path);

    db.write_entry("foo".to_string(), "bar".to_string());

}

