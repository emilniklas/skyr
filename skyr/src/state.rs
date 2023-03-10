use std::collections::{BTreeMap, BTreeSet};
use std::sync::RwLock;
use std::{fmt, io};

use serde::{Deserialize, Serialize};

use crate::analyze::Type;
use crate::execute::Value;
use crate::{Diff, DisplayAsDebug};

#[derive(Serialize, Deserialize, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ResourceId {
    type_: Type,
    id: String,
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
    resources: RwLock<BTreeMap<ResourceId, Resource>>,
}

impl State {
    pub fn into_resources(self) -> BTreeMap<ResourceId, Resource> {
        self.resources.into_inner().unwrap()
    }

    pub fn open(reader: impl io::Read) -> io::Result<State> {
        bincode::deserialize_from(reader).map_err(Self::bincode_to_io_error)
    }

    pub fn save(self, writer: impl io::Write) -> io::Result<()> {
        bincode::serialize_into(writer, &self).map_err(Self::bincode_to_io_error)
    }

    pub fn is_empty(&self) -> bool {
        self.resources.read().unwrap().is_empty()
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

    pub fn remove(&self, id: &ResourceId) -> Option<Resource> {
        self.resources.write().unwrap().remove(id)
    }

    pub fn all_not_in(&self, ids: &BTreeSet<ResourceId>) -> Vec<Resource> {
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
pub struct Resource {
    pub id: ResourceId,
    pub arg: ResourceValue,
    pub state: ResourceValue,
}

impl Resource {
    #[inline]
    pub fn has_type(&self, type_: &Type) -> bool {
        self.id.has_type(type_)
    }
}

#[derive(Serialize, Deserialize, Clone, PartialEq)]
pub enum ResourceValue {
    Nil,
    String(String),
    Integer(i128),
    Boolean(bool),
    Record(Vec<(String, ResourceValue)>),
    List(Vec<ResourceValue>),
}

impl ResourceValue {
    pub fn record(
        i: impl IntoIterator<Item = (impl Into<String>, impl Into<ResourceValue>)>,
    ) -> Self {
        Self::Record(i.into_iter().map(|(n, t)| (n.into(), t.into())).collect())
    }

    pub fn list(i: impl IntoIterator<Item = impl Into<ResourceValue>>) -> Self {
        Self::List(i.into_iter().map(|e| e.into()).collect())
    }

    pub fn set_member(&mut self, name: &str, value: impl Into<ResourceValue>) {
        if let ResourceValue::Record(r) = self {
            for (n, v) in r.iter_mut() {
                if n == name {
                    *v = value.into();
                    return;
                }
            }
            r.push((name.into(), value.into()));
        } else {
            panic!("cannot set {} on {:?}", name, self);
        }
    }

    pub fn access_member(&self, name: &str) -> &ResourceValue {
        if let ResourceValue::Record(r) = self {
            for (n, v) in r.iter() {
                if n == name {
                    return v;
                }
            }
        }
        panic!("cannot get member {} on {:?}", name, self);
    }

    pub fn as_str(&self) -> &str {
        if let ResourceValue::String(s) = self {
            s.as_str()
        } else {
            panic!("{:?} is not a string", self);
        }
    }

    pub fn diff<'v, 'a>(&'v self, value: &'v Value<'a>) -> Diff<'v, 'a> {
        Diff::calculate(self, value)
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

impl From<i128> for ResourceValue {
    fn from(value: i128) -> Self {
        ResourceValue::Integer(value)
    }
}

impl From<u8> for ResourceValue {
    fn from(value: u8) -> Self {
        ResourceValue::Integer(value.into())
    }
}

impl From<Value<'_>> for ResourceValue {
    fn from(value: Value) -> Self {
        match value {
            Value::Nil => ResourceValue::Nil,
            Value::String(s) => ResourceValue::String(s),
            Value::Integer(i) => ResourceValue::Integer(i),
            Value::Boolean(b) => ResourceValue::Boolean(b),
            Value::Record(r) => {
                ResourceValue::Record(r.into_iter().map(|(n, v)| (n, v.into())).collect())
            }
            Value::List(l) => ResourceValue::List(l.into_iter().map(|e| e.into()).collect()),
            _ => panic!("cannot derive a resource value from {:?}", value),
        }
    }
}

impl fmt::Debug for ResourceValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ResourceValue::Nil => write!(f, "<nil>"),
            ResourceValue::String(s) => s.fmt(f),
            ResourceValue::Integer(i) => i.fmt(f),
            ResourceValue::Boolean(b) => b.fmt(f),
            ResourceValue::Record(r) => {
                let mut m = f.debug_map();
                for (n, d) in r {
                    m.entry(&DisplayAsDebug(n), d);
                }
                m.finish()
            }
            ResourceValue::List(l) => {
                let mut m = f.debug_list();
                for d in l {
                    m.entry(d);
                }
                m.finish()
            }
        }
    }
}
