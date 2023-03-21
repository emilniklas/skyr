use std::borrow::Cow;
use std::fmt;

use serde::{Deserialize, Serialize};

use crate::execute::{DependentValue, RuntimeValue};
use crate::{Diff, DisplayAsDebug, SerializationError, ValueDeserializer, ValueSerializer};

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum Value {
    Primitive(Primitive),
    Collection(Collection<Value>),
}

impl Value {
    pub fn diff<'a>(&'a self, other: &'a Self) -> Diff<'a> {
        Diff::calculate(self, other)
    }

    pub fn serialize(v: &impl Serialize) -> Result<Value, SerializationError> {
        v.serialize(ValueSerializer)
    }

    pub fn deserialize<'a, T: Deserialize<'a>>(&'a self) -> Result<T, SerializationError> {
        T::deserialize(ValueDeserializer::new(self))
    }

    pub fn as_collection_mut(&mut self) -> &mut Collection<Value> {
        if let Self::Collection(c) = self {
            c
        } else {
            panic!("{:?} is not a collection", self)
        }
    }

    pub fn as_collection(&self) -> &Collection<Value> {
        if let Self::Collection(c) = self {
            &c
        } else {
            panic!("{:?} is not a collection", self)
        }
    }

    pub fn as_primitive(&self) -> &Primitive {
        if let Self::Primitive(p) = self {
            &p
        } else {
            panic!("{:?} is not a primitive", self)
        }
    }
}

impl<T: Into<Primitive>> From<T> for Value {
    fn from(value: T) -> Self {
        Self::Primitive(value.into())
    }
}

impl fmt::Debug for Value {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Primitive(v) => v.fmt(f),
            Self::Collection(v) => v.fmt(f),
        }
    }
}

impl fmt::Display for Value {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Primitive(v) => v.fmt(f),
            Self::Collection(v) => v.fmt(f),
        }
    }
}

impl<'a> From<RuntimeValue<'a>> for DependentValue<Value> {
    fn from(value: RuntimeValue<'a>) -> Self {
        match value {
            RuntimeValue::Primitive(p) => Value::Primitive(p).into(),
            RuntimeValue::Collection(c) => {
                Into::<DependentValue<Collection<RuntimeValue<'a>>>>::into(c)
                    .flat_map(|rv_collection| {
                        let collection_dv: Collection<DependentValue<Value>> =
                            rv_collection.map(RuntimeValue::into);
                        let dv_collection: DependentValue<Collection<Value>> = collection_dv.into();
                        dv_collection
                    })
                    .map(Value::Collection)
            }

            v => panic!(
                "runtime value {:?} doesn't support being cast to static value",
                v
            ),
        }
    }
}

#[derive(Clone, PartialEq, PartialOrd)]
pub enum Primitive {
    Nil,
    String(Cow<'static, str>),
    Integer(i128),
    Float(f64),
    Boolean(bool),
}

impl Eq for Primitive {}

impl Ord for Primitive {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.partial_cmp(other).unwrap_or(std::cmp::Ordering::Equal)
    }
}

impl Primitive {
    pub fn static_str(s: &'static str) -> Self {
        Self::String(Cow::Borrowed(s))
    }

    pub fn string(s: impl Into<String>) -> Self {
        Self::String(Cow::Owned(s.into()))
    }

    pub fn as_str(&self) -> &str {
        if let Self::String(s) = self {
            s.as_ref()
        } else {
            panic!("{:?} is not a string", self)
        }
    }

    pub fn isize(i: isize) -> Self {
        Self::Integer(i as _)
    }

    pub fn as_isize(&self) -> isize {
        if let Self::Integer(i) = self {
            *i as _
        } else {
            panic!("{:?} is not an integer", self)
        }
    }

    pub fn usize(i: usize) -> Self {
        Self::Integer(i as _)
    }

    pub fn as_usize(&self) -> usize {
        if let Self::Integer(i) = self {
            *i as _
        } else {
            panic!("{:?} is not an integer", self)
        }
    }

    pub fn i128(i: i128) -> Self {
        Self::Integer(i)
    }

    pub fn as_i128(&self) -> i128 {
        if let Self::Integer(i) = self {
            *i
        } else {
            panic!("{:?} is not an integer", self)
        }
    }

    pub fn i64(i: i64) -> Self {
        Self::Integer(i as _)
    }

    pub fn as_i64(&self) -> i64 {
        if let Self::Integer(i) = self {
            *i as _
        } else {
            panic!("{:?} is not an integer", self)
        }
    }

    pub fn i32(i: i32) -> Self {
        Self::Integer(i as _)
    }

    pub fn as_i32(&self) -> i32 {
        if let Self::Integer(i) = self {
            *i as _
        } else {
            panic!("{:?} is not an integer", self)
        }
    }

    pub fn i16(i: i16) -> Self {
        Self::Integer(i as _)
    }

    pub fn as_i16(&self) -> i16 {
        if let Self::Integer(i) = self {
            *i as _
        } else {
            panic!("{:?} is not an integer", self)
        }
    }

    pub fn i8(i: i8) -> Self {
        Self::Integer(i as _)
    }

    pub fn as_i8(&self) -> i8 {
        if let Self::Integer(i) = self {
            *i as _
        } else {
            panic!("{:?} is not an integer", self)
        }
    }

    pub fn u128(u: u128) -> Self {
        Self::Integer(u as _)
    }

    pub fn as_u128(&self) -> u128 {
        if let Self::Integer(i) = self {
            *i as _
        } else {
            panic!("{:?} is not an integer", self)
        }
    }

    pub fn u64(u: u64) -> Self {
        Self::Integer(u as _)
    }

    pub fn as_u64(&self) -> u64 {
        if let Self::Integer(i) = self {
            *i as _
        } else {
            panic!("{:?} is not an integer", self)
        }
    }

    pub fn u32(u: u32) -> Self {
        Self::Integer(u as _)
    }

    pub fn as_u32(&self) -> u32 {
        if let Self::Integer(i) = self {
            *i as _
        } else {
            panic!("{:?} is not an integer", self)
        }
    }

    pub fn u16(u: u16) -> Self {
        Self::Integer(u as _)
    }

    pub fn as_u16(&self) -> u16 {
        if let Self::Integer(i) = self {
            *i as _
        } else {
            panic!("{:?} is not an integer", self)
        }
    }

    pub fn u8(u: u8) -> Self {
        Self::Integer(u as _)
    }

    pub fn as_u8(&self) -> u8 {
        if let Self::Integer(i) = self {
            *i as _
        } else {
            panic!("{:?} is not an integer", self)
        }
    }

    pub fn f64(f: f64) -> Self {
        Self::Float(f)
    }

    pub fn as_f64(&self) -> f64 {
        if let Self::Float(f) = self {
            *f
        } else {
            panic!("{:?} is not a float", self)
        }
    }

    pub fn f32(f: f32) -> Self {
        Self::Float(f as _)
    }

    pub fn as_f32(&self) -> f32 {
        if let Self::Float(f) = self {
            *f as _
        } else {
            panic!("{:?} is not a float", self)
        }
    }

    pub fn bool(b: bool) -> Self {
        Self::Boolean(b)
    }

    pub fn as_bool(&self) -> bool {
        if let Self::Boolean(b) = self {
            *b
        } else {
            panic!("{:?} is not a boolean", self)
        }
    }
}

impl Default for Primitive {
    fn default() -> Self {
        Primitive::Nil
    }
}

impl From<()> for Primitive {
    fn from(_value: ()) -> Self {
        Primitive::Nil
    }
}

impl<T: Into<Self>> From<Option<T>> for Primitive {
    fn from(value: Option<T>) -> Self {
        match value {
            None => Primitive::Nil,
            Some(v) => v.into(),
        }
    }
}

impl From<String> for Primitive {
    fn from(value: String) -> Self {
        Self::string(value)
    }
}

impl From<isize> for Primitive {
    fn from(value: isize) -> Self {
        Self::isize(value)
    }
}

impl From<usize> for Primitive {
    fn from(value: usize) -> Self {
        Self::usize(value)
    }
}

impl From<i128> for Primitive {
    fn from(value: i128) -> Self {
        Self::i128(value)
    }
}

impl From<i64> for Primitive {
    fn from(value: i64) -> Self {
        Self::i64(value)
    }
}

impl From<i32> for Primitive {
    fn from(value: i32) -> Self {
        Self::i32(value)
    }
}

impl From<i16> for Primitive {
    fn from(value: i16) -> Self {
        Self::i16(value)
    }
}

impl From<i8> for Primitive {
    fn from(value: i8) -> Self {
        Self::i8(value)
    }
}

impl From<u128> for Primitive {
    fn from(value: u128) -> Self {
        Self::u128(value)
    }
}

impl From<u64> for Primitive {
    fn from(value: u64) -> Self {
        Self::u64(value)
    }
}

impl From<u32> for Primitive {
    fn from(value: u32) -> Self {
        Self::u32(value)
    }
}

impl From<u16> for Primitive {
    fn from(value: u16) -> Self {
        Self::u16(value)
    }
}

impl From<u8> for Primitive {
    fn from(value: u8) -> Self {
        Self::u8(value)
    }
}

impl From<f64> for Primitive {
    fn from(value: f64) -> Self {
        Self::f64(value)
    }
}

impl From<f32> for Primitive {
    fn from(value: f32) -> Self {
        Self::f32(value)
    }
}

impl From<bool> for Primitive {
    fn from(value: bool) -> Self {
        Self::bool(value)
    }
}

impl fmt::Debug for Primitive {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Nil => write!(f, "nil"),
            Self::String(v) => v.fmt(f),
            Self::Integer(v) => v.fmt(f),
            Self::Float(v) => v.fmt(f),
            Self::Boolean(v) => v.fmt(f),
        }
    }
}

impl fmt::Display for Primitive {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Nil => write!(f, "nil"),
            Self::String(v) => v.fmt(f),
            Self::Integer(v) => v.fmt(f),
            Self::Float(v) => v.fmt(f),
            Self::Boolean(v) => v.fmt(f),
        }
    }
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum Collection<T = Primitive> {
    List(Vec<T>),
    Record(Vec<(Cow<'static, str>, T)>),
    Tuple(Vec<T>),
    Dict(Vec<(T, T)>),
}

impl<T> Collection<T> {
    pub fn list(i: impl IntoIterator<Item = impl Into<T>>) -> Self {
        Self::List(i.into_iter().map(|i| i.into()).collect())
    }

    pub fn tuple(i: impl IntoIterator<Item = impl Into<T>>) -> Self {
        Self::Tuple(i.into_iter().map(|i| i.into()).collect())
    }

    pub fn dict(i: impl IntoIterator<Item = (impl Into<T>, impl Into<T>)>) -> Self {
        Self::Dict(i.into_iter().map(|(k, v)| (k.into(), v.into())).collect())
    }

    pub fn static_record(i: impl IntoIterator<Item = (&'static str, impl Into<T>)>) -> Self {
        Self::Record(
            i.into_iter()
                .map(|(k, v)| (Cow::Borrowed(k), v.into()))
                .collect(),
        )
    }

    pub fn record(i: impl IntoIterator<Item = (impl Into<String>, impl Into<T>)>) -> Self {
        Self::Record(
            i.into_iter()
                .map(|(k, v)| (Cow::Owned(k.into()), v.into()))
                .collect(),
        )
    }

    pub fn map<F, U>(self, mut f: F) -> Collection<U>
    where
        F: FnMut(T) -> U,
    {
        match self {
            Collection::List(l) => Collection::List(l.into_iter().map(f).collect()),
            Collection::Tuple(l) => Collection::Tuple(l.into_iter().map(f).collect()),
            Collection::Record(r) => {
                Collection::Record(r.into_iter().map(|(k, v)| (k, f(v))).collect())
            }
            Collection::Dict(m) => {
                Collection::Dict(m.into_iter().map(|(k, v)| (f(k), f(v))).collect())
            }
        }
    }
}

impl Collection<Value> {
    pub fn access_member(&self, n: &str) -> Option<&Value> {
        match self {
            Self::Record(r) => r.iter().find_map(|(k, v)| (k == n).then_some(v)),
            Self::Dict(d) => d
                .iter()
                .find_map(|(k, v)| (k.as_primitive().as_str() == n).then_some(v)),
            _ => None,
        }
    }

    pub fn set_member(&mut self, k: impl Into<Cow<'static, str>>, v: impl Into<Value>) {
        let k = k.into();
        match self {
            Self::Record(r) => {
                if let Some(entry) = r.iter_mut().find_map(|(n, v)| (n == &k).then_some(v)) {
                    *entry = v.into();
                } else {
                    r.push((k, v.into()));
                }
            }
            Self::Dict(d) => {
                if let Some(entry) = d
                    .iter_mut()
                    .find_map(|(n, v)| (n.as_primitive().as_str() == &k).then_some(v))
                {
                    *entry = v.into();
                } else {
                    d.push((Value::Primitive(Primitive::String(k)), v.into()));
                }
            }
            _ => panic!("{:?} is not a record", self),
        }
    }
}

impl<'a> Collection<DependentValue<RuntimeValue<'a>>> {
    pub fn access_member(&self, n: &str) -> Cow<DependentValue<RuntimeValue<'a>>> {
        match self {
            Self::Record(r) => r
                .iter()
                .find_map(|(k, v)| (k == n).then_some(v))
                .map(Cow::Borrowed)
                .expect("no such field"),
            Self::Dict(d) => {
                let mut deps = vec![];
                for (k, v) in d {
                    match k.clone().into_result() {
                        Ok(k) => {
                            if let RuntimeValue::Primitive(Primitive::String(k)) = k {
                                if k == n {
                                    return Cow::Borrowed(v);
                                }
                            }
                        }
                        Err(p) => deps.extend(p.dependencies),
                    }
                }
                if deps.is_empty() {
                    panic!("no such field");
                }
                Cow::Owned(DependentValue::pending(deps))
            }
            _ => panic!("{:?} is not a record", self),
        }
    }
}

impl<T: fmt::Debug> Collection<T> {
    pub fn as_vec(&self) -> &Vec<T> {
        if let Self::List(l) = self {
            l
        } else {
            panic!("{:?} is not a list", self)
        }
    }
}

impl<V: Into<Primitive>> From<Vec<V>> for Collection {
    fn from(value: Vec<V>) -> Self {
        Self::list(value)
    }
}

impl<T, V: Into<Collection<T>>> From<Vec<V>> for Collection<Collection<T>> {
    fn from(value: Vec<V>) -> Self {
        Self::list(value)
    }
}

impl<T, V: Into<T>> From<Vec<(&'static str, V)>> for Collection<T> {
    fn from(value: Vec<(&'static str, V)>) -> Self {
        Self::record(value)
    }
}

impl<T, V: Into<T>> From<Vec<(String, V)>> for Collection<T> {
    fn from(value: Vec<(String, V)>) -> Self {
        Self::record(value)
    }
}

impl<T: fmt::Debug> fmt::Debug for Collection<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::List(v) => v.fmt(f),
            Self::Tuple(t) => {
                let mut f = f.debug_tuple("");
                for e in t {
                    f.field(e);
                }
                f.finish()
            }
            Self::Record(r) => {
                let mut f = f.debug_map();
                for (k, v) in r {
                    f.entry(&DisplayAsDebug(k), v);
                }
                f.finish()
            }
            Self::Dict(r) => {
                let mut f = f.debug_map();
                for (k, v) in r {
                    f.entry(k, v);
                }
                f.finish()
            }
        }
    }
}

impl<T: fmt::Debug> fmt::Display for Collection<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:#?}", self)
    }
}
