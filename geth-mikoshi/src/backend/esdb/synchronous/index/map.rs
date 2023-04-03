use crate::backend::esdb::synchronous::index::mem_table::MemTable;
use geth_common::Position;
use std::fs::OpenOptions;
use std::io;
use std::io::{BufRead, BufReader};
use std::path::PathBuf;

pub fn load_index_map(root: &PathBuf) -> eyre::Result<MemTable> {
    let file = OpenOptions::new()
        .read(true)
        .open(root.join("index").join("indexmap"))?;

    let mut file = BufReader::new(file);
    let mut version = 0i32;
    let mut position = Position(0);

    for (idx, line) in file.lines().enumerate() {
        let line = line?;

        if idx == 0 {
            // TODO - Store the MD5 string representation.
            continue;
        }

        if idx == 1 {
            version = line.parse::<i32>()?;
            continue;
        }

        if idx == 2 {
            let splits = line.split('/').collect::<Vec<_>>();

            if splits.len() != 2 {
                eyre::bail!("Wrong prepare and commit format");
            }

            let prepare = splits[0].parse::<i64>()?;
            let commit = splits[1].parse::<i64>()?;

            if prepare != commit || prepare < -1 {
                eyre::bail!("Wrong prepare and commit format");
            }

            // TODO - Handle the case when -1
            position = Position(prepare as u64);
            continue;
        }

        if idx == 3 && version > 1 {
            // TODO - Read max auto merge level.
            continue;
        }

        // PTable parsing section
        let splits = line.split(',').collect::<Vec<_>>();
        let level = splits[0].parse::<i32>()?;
        let position = splits[1].parse::<i32>()?;
        let file = splits[2];
        let filepath = root.join("index").join(file);

        // TODO - Parse the PTable here.
    }

    todo!()
}
