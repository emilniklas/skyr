use std::collections::{BTreeMap, BTreeSet};
use std::sync::RwLock;
use std::{fmt, io};

use serde::{Deserialize, Serialize};

use crate::analyze::Type;
use crate::Value;

#[derive(Serialize, Deserialize, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct ResourceId {
    pub type_: Type,
    pub id: String,
}

impl fmt::Debug for ResourceId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}({})", self.type_, self.id)
    }
}

impl ResourceId {
    pub fn new(type_: impl Into<Type>, id: impl Into<String>) -> ResourceId {
        ResourceId {
            type_: type_.into(),
            id: id.into(),
        }
    }

    pub fn has_type(&self, type_: &Type) -> bool {
        &self.type_ == type_
    }
}

#[derive(Serialize, Deserialize, Default, Debug)]
pub struct State {
    resources: RwLock<BTreeMap<ResourceId, ResourceState>>,
}

impl State {
    pub fn into_resources(self) -> BTreeMap<ResourceId, ResourceState> {
        self.resources.into_inner().unwrap()
    }

    pub fn open(reader: impl io::Read) -> io::Result<State> {
        rmp_serde::from_read(reader).map_err(|e| io::Error::new(io::ErrorKind::Other, e))
    }

    pub fn save(&self, writer: &mut impl io::Write) -> io::Result<()> {
        rmp_serde::encode::write(writer, &self).map_err(|e| io::Error::new(io::ErrorKind::Other, e))
    }

    pub fn is_empty(&self) -> bool {
        self.resources.read().unwrap().is_empty()
    }

    pub fn get(&self, id: &ResourceId) -> Option<ResourceState> {
        self.resources.read().unwrap().get(id).cloned()
    }

    pub fn insert(&self, resource: ResourceState) {
        self.resources
            .write()
            .unwrap()
            .insert(resource.id.clone(), resource);
    }

    pub fn remove(&self, id: &ResourceId) -> Option<ResourceState> {
        self.resources.write().unwrap().remove(id)
    }

    pub fn all_not_in(&self, ids: &BTreeSet<ResourceId>) -> Vec<ResourceState> {
        self.resources
            .read()
            .unwrap()
            .iter()
            .filter(|(id, _)| !ids.contains(id))
            .map(|(_, r)| r)
            .cloned()
            .collect()
    }
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct ResourceState {
    pub id: ResourceId,
    pub dependencies: Vec<ResourceId>,
    pub arg: Value,
    pub state: Value,
}

impl ResourceState {
    #[inline]
    pub fn has_type(&self, type_: &Type) -> bool {
        self.id.has_type(type_)
    }
}
