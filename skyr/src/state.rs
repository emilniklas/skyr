use std::collections::BTreeMap;
use std::io;
use std::sync::RwLock;

use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub enum ResourceId {
    Named(String),
}

impl From<&str> for ResourceId {
    fn from(value: &str) -> Self {
        ResourceId::Named(value.into())
    }
}

#[derive(Serialize, Deserialize, Default, Debug)]
pub struct State {
    resources: RwLock<BTreeMap<ResourceId, Resource>>,
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

    pub fn get(&self, id: &ResourceId) -> Option<Resource> {
        self.resources.read().unwrap().get(id).cloned()
    }

    pub fn insert(&self, resource: Resource) {
        self.resources
            .write()
            .unwrap()
            .insert(resource.id.clone(), resource);
    }
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Resource {
    pub id: ResourceId,
    pub state: ResourceValue,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub enum ResourceValue {
    String(String),
    Record(Vec<(String, ResourceValue)>),
}

impl ResourceValue {
    pub fn record(
        i: impl IntoIterator<Item = (impl Into<String>, impl Into<ResourceValue>)>,
    ) -> Self {
        Self::Record(i.into_iter().map(|(n, t)| (n.into(), t.into())).collect())
    }
}

impl From<&str> for ResourceValue {
    fn from(value: &str) -> Self {
        ResourceValue::String(value.into())
    }
}

impl From<String> for ResourceValue {
    fn from(value: String) -> Self {
        ResourceValue::String(value.into())
    }
}
