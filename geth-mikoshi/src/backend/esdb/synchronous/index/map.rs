use crate::backend::esdb::synchronous::index::{
    IndexMap, Key, Midpoint, PTable, PTableFooter, PTableHeader, ENTRY_SIZE, MD5_SIZE,
    PTABLE_FOOTER_SIZE, PTABLE_HEADER_SIZE,
};
use byteorder::{LittleEndian, ReadBytesExt};
use bytes::BytesMut;
use geth_common::Position;
use std::fs::OpenOptions;

use std::io::{BufRead, BufReader, Read, Seek, SeekFrom};
use std::path::PathBuf;
use uuid::Uuid;

pub fn load_index_map(root: &PathBuf, buffer: &mut BytesMut) -> eyre::Result<IndexMap> {
    let file = OpenOptions::new()
        .read(true)
        .open(root.join("index").join("indexmap"))?;

    let file = BufReader::new(file);
    let mut version = 0i32;
    let mut position = Position(0);
    let mut auto_merge_level = u64::MAX;
    let mut tables = Vec::new();

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
            auto_merge_level = line.parse::<i32>()? as u64;
            continue;
        }

        // PTable parsing section
        let splits = line.split(',').collect::<Vec<_>>();
        let level = splits[0].parse::<i32>()?;
        let order = splits[1].parse::<i32>()?;
        let file = splits[2];
        let id = file.parse::<Uuid>()?;
        let filepath = root.join("index").join(file);

        let mut file = OpenOptions::new().read(true).open(filepath)?;
        file.seek(SeekFrom::Start(0))?;

        buffer.reserve(PTABLE_HEADER_SIZE);
        unsafe {
            buffer.set_len(PTABLE_HEADER_SIZE);
        }

        file.read_exact(&mut buffer.as_mut()[..PTABLE_HEADER_SIZE])?;
        let header = PTableHeader::load(buffer.split().freeze())?;

        // TODO - Double check because I'm pretty sure this is not required at all.
        buffer.reserve(PTABLE_FOOTER_SIZE);
        unsafe {
            buffer.set_len(PTABLE_FOOTER_SIZE);
        }

        file.seek(SeekFrom::End(-((MD5_SIZE + PTABLE_FOOTER_SIZE) as i64)))?;
        file.read_exact(&mut buffer.as_mut()[..PTABLE_FOOTER_SIZE])?;
        let footer = PTableFooter::load(buffer.split().freeze())?;

        let file_size = file.metadata()?.len();
        let entries_size = file_size as usize
            - PTABLE_HEADER_SIZE
            - MD5_SIZE
            - PTABLE_FOOTER_SIZE
            - footer.cached_midpoint_size();

        if entries_size % ENTRY_SIZE != 0 {
            eyre::bail!(
                "Total size of index entries {} is not divisible by index entry size {}",
                entries_size,
                ENTRY_SIZE
            );
        }

        let entries_count = entries_size / ENTRY_SIZE;

        if entries_count > 0 && footer.cached_midpoints_num < 2 {
            eyre::bail!(
                "Less than 2 midpoints cached in PTable. Index entries = {}, Midpoints cached {}",
                entries_count,
                footer.cached_midpoints_num
            );
        }

        let offset = MD5_SIZE + PTABLE_FOOTER_SIZE + entries_size;
        file.seek(SeekFrom::End(-(offset as i64)))?;

        let mut file = BufReader::new(file);
        let mut midpoints = Vec::new();

        for _ in 0..footer.cached_midpoints_num {
            let revision = file.read_i64::<LittleEndian>()?;
            let stream = file.read_u64::<LittleEndian>()?;
            let offset = file.read_i64::<LittleEndian>()?;

            midpoints.push(Midpoint {
                key: Key {
                    stream,
                    revision: revision as u64,
                },
                offset: offset as u64,
            });
        }

        tables.push(PTable {
            id,
            level: level as u64,
            order,
            header,
            footer,
            midpoints,
        });
    }

    // TODO - Double check, pretty sure we don't need to enforce order here.
    tables.sort_by_key(|p| p.order);

    Ok(IndexMap {
        version: version as u64,
        position,
        auto_merge_level,
        tables,
    })
}

#[cfg(test)]
mod tests {
    use crate::backend::esdb::synchronous::index::map::load_index_map;
    use bytes::BytesMut;
    use std::path::PathBuf;

    #[test]
    fn load_working_set_index_map() -> eyre::Result<()> {
        let mut buffer = BytesMut::new();
        let root = PathBuf::from("./working-set");

        let index_map = load_index_map(&root, &mut buffer)?;
        Ok(())
    }
}
