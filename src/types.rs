use std::convert::{TryFrom, TryInto};
use std::io::{Read, Write};

pub trait DataType {
    fn to_tuple<W: Write>(&self, w: W) -> anyhow::Result<()>;
    fn from_tuple<R: Read>(r: R) -> anyhow::Result<Self>
    where
        Self: Sized;
}

#[derive(PartialEq, Eq, Clone, Debug)]
pub struct Text(String);

impl Text {
    pub fn new(s: String) -> anyhow::Result<Self> {
        anyhow::ensure!(s.len() <= 1024, "string too long for text type");
        Ok(Text(s))
    }
}

impl DataType for Text {
    fn to_tuple<W: Write>(&self, mut w: W) -> anyhow::Result<()> {
        let u32len: u32 = self.0.len().try_into()?;
        w.write_all(u32len.to_le_bytes().as_ref())?;
        w.write_all(self.0.as_bytes())?;
        Ok(())
    }

    fn from_tuple<R: Read>(mut r: R) -> anyhow::Result<Self> {
        let mut len = [0; 4];
        r.read_exact(&mut len)?;
        let len = u32::from_le_bytes(len).try_into()?;
        anyhow::ensure!(len <= 1024, "string too long for text type");

        let mut buf = vec![0; 1024];
        r.read_exact(&mut buf[..len])?;
        buf.truncate(len);

        Ok(Text(String::from_utf8(buf)?))
    }
}
#[derive(PartialEq, Eq, Clone, Debug)]
pub struct I32(i32);

impl I32 {
    pub fn new(i: i32) -> Self {
        I32(i)
    }
}

impl DataType for I32 {
    fn to_tuple<W: Write>(&self, mut w: W) -> anyhow::Result<()> {
        Ok(w.write_all(&self.0.to_le_bytes())?)
    }
    fn from_tuple<R: Read>(mut r: R) -> anyhow::Result<Self> {
        let mut data = [0; 4];
        r.read_exact(&mut data[..])?;
        Ok(I32(i32::from_le_bytes(data)))
    }
}

#[derive(PartialEq, Eq, Clone, Debug)]
pub enum AnyType {
    Text(Text),
    I32(I32),
}

#[repr(u8)]
enum Tag {
    I32 = 1,
    Text = 2,
}

impl TryFrom<u8> for Tag {
    type Error = anyhow::Error;
    fn try_from(val: u8) -> Result<Tag, anyhow::Error> {
        anyhow::ensure!(val >= 1 && val <= 2, "invalid tag");
        Ok(match val {
            1 => Tag::I32,
            2 => Tag::Text,
            _ => anyhow::bail!("invalid tag: {}", val),
        })
    }
}

impl DataType for AnyType {
    fn to_tuple<W: Write>(&self, mut w: W) -> anyhow::Result<()> {
        match self {
            AnyType::I32(i) => {
                w.write_all(&[Tag::I32 as u8])?;
                i.to_tuple(w)
            }
            AnyType::Text(t) => {
                w.write_all(&[Tag::Text as u8])?;
                t.to_tuple(w)
            }
        }
    }
    fn from_tuple<R: Read>(mut r: R) -> anyhow::Result<Self>
    where
        Self: Sized,
    {
        let mut tag = [0; 1];
        r.read_exact(&mut tag)?;
        Ok(match tag[0].try_into()? {
            Tag::I32 => AnyType::I32(I32::from_tuple(r)?),
            Tag::Text => AnyType::Text(Text::from_tuple(r)?),
        })
    }
}

impl From<I32> for AnyType {
    fn from(val: I32) -> AnyType {
        AnyType::I32(val)
    }
}

impl From<Text> for AnyType {
    fn from(val: Text) -> AnyType {
        AnyType::Text(val)
    }
}

#[derive(PartialEq, Eq, Clone, Debug)]
pub struct Row(Vec<AnyType>);

impl Row {
    pub fn new(row: Vec<AnyType>) -> anyhow::Result<Row> {
        anyhow::ensure!(row.len() <= 64, "row length {} too long", row.len());
        Ok(Row(row))
    }
}

impl DataType for Row {
    fn to_tuple<W: Write>(&self, mut w: W) -> anyhow::Result<()> {
        let len: u32 = self.0.len().try_into()?;
        w.write_all(&len.to_le_bytes())?;
        for value in &self.0 {
            value.to_tuple(&mut w)?;
        }
        Ok(())
    }

    fn from_tuple<R: Read>(mut r: R) -> anyhow::Result<Self>
    where
        Self: Sized,
    {
        let mut len = [0; 4];
        r.read_exact(&mut len[..])?;
        let len = u32::from_le_bytes(len);
        let mut v = Vec::with_capacity(len as usize);
        for _ in 0..len {
            v.push(AnyType::from_tuple(&mut r)?);
        }
        Ok(Row(v))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn roundtrip() -> anyhow::Result<()> {
        let r = Row::new(vec![
            I32(48).into(),
            Text(String::from("J. Cliff Dyer")).into(),
            Text(String::from("jcd@sdf.org")).into(),
        ])?;

        let mut tuple = Vec::new();
        r.to_tuple(&mut tuple)?;
        println!("{:?}", tuple);

        let new_row = Row::from_tuple(Cursor::new(tuple))?;
        assert_eq!(r, new_row);
        Ok(())
    }

    #[test]
    fn from_tuple() -> anyhow::Result<()> {
        // len 2,
        // 1: I32(514) (0x202)
        // 2: Text("abc")
        let bytes = vec![2, 0, 0, 0, 1, 2, 2, 0, 0, 2, 3, 0, 0, 0, b'a', b'b', b'c'];

        let r = Row::from_tuple(Cursor::new(bytes))?;

        assert_eq!(r.0.len(), 2);
        assert_eq!(r.0, vec![
            I32(514).into(),
            Text(String::from("abc")).into(),
        ]);
        Ok(())
    }

    #[test]
    fn to_tuple() -> anyhow::Result<()> {
        let r = Row::new(vec![
            AnyType::I32(I32(514)),
            AnyType::Text(Text(String::from("abc"))),
        ])?;
        let mut tuple = Vec::new();
        r.to_tuple(&mut tuple)?;
        assert_eq!(tuple, vec![2, 0, 0, 0, 1, 2, 2, 0, 0, 2, 3, 0, 0, 0, b'a', b'b', b'c']);
        Ok(())
    }
}
