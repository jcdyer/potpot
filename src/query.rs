use crate::types;

pub fn insert(_row: &types::Row) -> anyhow::Result<()> {
    Ok(())
}

pub fn get(_id: i32) -> anyhow::Result<types::Row> {
    types::Row::new(Vec::new())
}