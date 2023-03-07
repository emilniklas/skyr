use std::io;

use serde::{Serialize, Deserialize};

#[derive(Serialize, Deserialize)]
pub struct State {
}

impl State {
    pub fn open(reader: impl io::Read) -> io::Result<State> {
        bincode::deserialize_from(reader).map_err(Self::bincode_to_io_error)
    }

    pub fn save(self, writer: impl io::Write) -> io::Result<()> {
        bincode::serialize_into(writer, &self).map_err(Self::bincode_to_io_error)
    }

    fn bincode_to_io_error(e: bincode::Error) -> io::Error {
        match *e {
            bincode::ErrorKind::Io(e) => e,
            e => io::Error::new(io::ErrorKind::Other, e),
        }
    }
}
