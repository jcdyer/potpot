use std::path::PathBuf;

use potpot::{aligned, bufferpool::BufferPool, easyhashtable::EasyHashTable, storage::PagedFile};

enum Cmd {
    Get { key: String },
    Insert { key: String, value: String },
}

fn main() {
    let mut args = std::env::args();
    let _ = args.next(); // Discard command name
    println!("{:?}", args);
    let path = PathBuf::from(args.next().unwrap());
    let is_new = !path.exists();
    let cmd = args.next().expect("cmd");
    let cmd = match cmd.as_str() {
        "get" => {
            let key = args.next().expect("key");
            Cmd::Get { key }
        }
        "insert" => {
            let key = args.next().expect("key");
            let value = args.next().expect("value");
            Cmd::Insert { key, value }
        }
        _ => panic!("unknown cmd: {cmd}"),
    };

    let storage = PagedFile::from_path(&path).expect("new storage");
    let mut buffer_pool = BufferPool::new(storage, 1);
    let mut page = aligned::Buffer::new();
    let mut ht = if is_new {
        EasyHashTable::new_in_page(&mut buffer_pool, 0)
    } else {
        EasyHashTable::from_page(&mut buffer_pool, 0).expect("no valid hash table found")
    };
    buffer_pool
        .read_page(0, &mut page)
        .expect("read a page into an aligned buffer");

    match cmd {
        Cmd::Get { key } => match ht.get(&mut buffer_pool, key.as_bytes()) {
            Ok(Some(value)) => println!("{}", String::from_utf8_lossy(&value)),
            Ok(None) => {}
            Err(err) => println!("{}", err),
        },
        Cmd::Insert { key, value } => println!(
            "{:?}",
            ht.insert(&mut buffer_pool, key.as_bytes(), value.as_bytes())
        ),
    }
}
