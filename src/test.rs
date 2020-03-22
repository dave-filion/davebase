use super::*;
use chrono::{FixedOffset, NaiveDateTime, TimeZone, Utc};

const TEST_MAX_BYTES: u64 = 512;

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
    assert_eq!(1584550857, timestamp);
}

#[test]
fn test_parse_row_entry() {
    let mut file = File::open("test-data/1.dat").unwrap();
    let result = parse_row(&mut file);
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
    let db = DaveBase::new(data_dir, TEST_MAX_BYTES);
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
    let db = DaveBase::new(data_dir, TEST_MAX_BYTES);
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
        }
        Err(e) => panic!("Shouldnt be an error"),
    }
}

#[test]
fn test_set_works() {
    let data_dir = "data";
    let mut db = DaveBase::new(data_dir, TEST_MAX_BYTES);
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
    let mut db = DaveBase::new(data_dir, TEST_MAX_BYTES);
    db.set(String::from("yo"), String::from("dude"));
    let val_loc = db.key_dir.get("yo").unwrap();
    let t = val_loc.timestamp;
    let timestamp = NaiveDateTime::from_timestamp(t as i64, 0);

    db.set(String::from("dv:23"), String::from("laksjdlakjsdlak"));
    let val_loc = db.key_dir.get("dv:23").unwrap();
    let t2 = val_loc.timestamp;
    let timestamp2 = NaiveDateTime::from_timestamp(t2 as i64, 0);
    println!(
        "t1 = {:?} t2 = {:?}",
        timestamp.to_string(),
        timestamp2.to_string()
    );
}

#[test]
fn test_dat_file_name() {
    let data_dir = "data";
    let path = dat_file_name(data_dir, 23);
    println!("path = {}", path.to_str().unwrap());
}
