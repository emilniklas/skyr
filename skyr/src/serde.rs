use std::borrow::Cow;
use std::fmt;

use serde::de::{EnumAccess, MapAccess, SeqAccess, VariantAccess, Visitor};
use serde::ser::{
    SerializeMap, SerializeSeq, SerializeStruct, SerializeStructVariant, SerializeTuple,
    SerializeTupleStruct, SerializeTupleVariant,
};
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::analyze::{CompositeType, PrimitiveType, Type};
use crate::{Collection, Flyweight, Primitive, Value};

#[derive(Debug)]
pub enum SerializationError {
    Other(String),
}

impl std::error::Error for SerializationError {}

impl fmt::Display for SerializationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl serde::ser::Error for SerializationError {
    fn custom<T>(msg: T) -> Self
    where
        T: fmt::Display,
    {
        SerializationError::Other(msg.to_string())
    }
}

impl serde::de::Error for SerializationError {
    fn custom<T>(msg: T) -> Self
    where
        T: fmt::Display,
    {
        SerializationError::Other(msg.to_string())
    }
}

impl Serialize for Value {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self {
            Value::Primitive(Primitive::Nil) => serializer.serialize_none(),
            Value::Primitive(Primitive::Integer(i)) => serializer.serialize_i64(*i),
            Value::Primitive(Primitive::Float(f)) => serializer.serialize_f64(*f),
            Value::Primitive(Primitive::Boolean(b)) => serializer.serialize_bool(*b),
            Value::Primitive(Primitive::String(s)) => serializer.serialize_str(s.as_ref()),

            Value::Collection(Collection::List(l)) => {
                let mut seq = serializer.serialize_seq(Some(l.len()))?;
                for e in l {
                    seq.serialize_element(e)?;
                }
                seq.end()
            }
            Value::Collection(Collection::Tuple(t)) => {
                let mut s = serializer.serialize_tuple(t.len())?;
                for e in t {
                    s.serialize_element(e)?
                }
                s.end()
            }
            Value::Collection(Collection::Record(r)) => {
                let mut s = serializer.serialize_struct("record", r.len())?;
                for (k, v) in r {
                    let k = RECORD_FIELDS.make(k.to_string());
                    s.serialize_field(k, v)?;
                }
                s.end()
            }
            Value::Collection(Collection::Dict(d)) => {
                let mut s = serializer.serialize_map(Some(d.len()))?;
                for (k, v) in d {
                    s.serialize_entry(k, v)?;
                }
                s.end()
            }
        }
    }
}

static RECORD_FIELDS: Flyweight<String> = Flyweight::new();

pub struct ValueSerializer;

impl Serializer for ValueSerializer {
    type Ok = Value;

    type Error = SerializationError;

    type SerializeSeq = ValueSeqSerializer;

    type SerializeTuple = ValueTupleSerializer;

    type SerializeTupleStruct = ValueTupleSerializer;

    type SerializeTupleVariant = ValueTupleVariantSerializer;

    type SerializeMap = ValueMapSerializer;

    type SerializeStruct = ValueStructSerializer;

    type SerializeStructVariant = ValueStructVariantSerializer;

    fn serialize_bool(self, v: bool) -> Result<Self::Ok, Self::Error> {
        Ok(Value::Primitive(Primitive::Boolean(v)))
    }

    fn serialize_i8(self, v: i8) -> Result<Self::Ok, Self::Error> {
        Ok(Value::Primitive(Primitive::i8(v)))
    }

    fn serialize_i16(self, v: i16) -> Result<Self::Ok, Self::Error> {
        Ok(Value::Primitive(Primitive::i16(v)))
    }

    fn serialize_i32(self, v: i32) -> Result<Self::Ok, Self::Error> {
        Ok(Value::Primitive(Primitive::i32(v)))
    }

    fn serialize_i64(self, v: i64) -> Result<Self::Ok, Self::Error> {
        Ok(Value::Primitive(Primitive::i64(v)))
    }

    fn serialize_i128(self, v: i128) -> Result<Self::Ok, Self::Error> {
        Ok(Value::Primitive(Primitive::i128(v)))
    }

    fn serialize_u8(self, v: u8) -> Result<Self::Ok, Self::Error> {
        Ok(Value::Primitive(Primitive::u8(v)))
    }

    fn serialize_u16(self, v: u16) -> Result<Self::Ok, Self::Error> {
        Ok(Value::Primitive(Primitive::u16(v)))
    }

    fn serialize_u32(self, v: u32) -> Result<Self::Ok, Self::Error> {
        Ok(Value::Primitive(Primitive::u32(v)))
    }

    fn serialize_u64(self, v: u64) -> Result<Self::Ok, Self::Error> {
        Ok(Value::Primitive(Primitive::u64(v)))
    }

    fn serialize_u128(self, v: u128) -> Result<Self::Ok, Self::Error> {
        Ok(Value::Primitive(Primitive::u128(v)))
    }

    fn serialize_f32(self, v: f32) -> Result<Self::Ok, Self::Error> {
        Ok(Value::Primitive(Primitive::f32(v)))
    }

    fn serialize_f64(self, v: f64) -> Result<Self::Ok, Self::Error> {
        Ok(Value::Primitive(Primitive::f64(v)))
    }

    fn serialize_char(self, v: char) -> Result<Self::Ok, Self::Error> {
        let mut s = String::new();
        s.push(v);
        Ok(Value::Primitive(Primitive::String(s.into())))
    }

    fn serialize_str(self, v: &str) -> Result<Self::Ok, Self::Error> {
        Ok(Value::Primitive(Primitive::String(v.to_string().into())))
    }

    fn serialize_bytes(self, v: &[u8]) -> Result<Self::Ok, Self::Error> {
        Ok(Value::Collection(Collection::list(v.into_iter().copied())))
    }

    fn serialize_none(self) -> Result<Self::Ok, Self::Error> {
        Ok(Value::Primitive(Primitive::Nil))
    }

    fn serialize_some<T: ?Sized>(self, value: &T) -> Result<Self::Ok, Self::Error>
    where
        T: Serialize,
    {
        value.serialize(self)
    }

    fn serialize_unit(self) -> Result<Self::Ok, Self::Error> {
        Ok(Value::Primitive(Primitive::Nil))
    }

    fn serialize_unit_struct(self, name: &'static str) -> Result<Self::Ok, Self::Error> {
        Ok(Value::Primitive(Primitive::String(name.into())))
    }

    fn serialize_unit_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        variant: &'static str,
    ) -> Result<Self::Ok, Self::Error> {
        Ok(Value::Primitive(Primitive::String(variant.into())))
    }

    fn serialize_newtype_struct<T: ?Sized>(
        self,
        _name: &'static str,
        value: &T,
    ) -> Result<Self::Ok, Self::Error>
    where
        T: Serialize,
    {
        value.serialize(self)
    }

    fn serialize_newtype_variant<T: ?Sized>(
        self,
        _name: &'static str,
        _variant_index: u32,
        variant: &'static str,
        value: &T,
    ) -> Result<Self::Ok, Self::Error>
    where
        T: Serialize,
    {
        Ok(Value::Collection(Collection::tuple([
            Value::Primitive(Primitive::static_str(variant)),
            value.serialize(self)?,
        ])))
    }

    fn serialize_seq(self, len: Option<usize>) -> Result<Self::SerializeSeq, Self::Error> {
        Ok(ValueSeqSerializer {
            elements: if let Some(len) = len {
                Vec::with_capacity(len)
            } else {
                Vec::new()
            },
        })
    }

    fn serialize_tuple(self, len: usize) -> Result<Self::SerializeTuple, Self::Error> {
        Ok(ValueTupleSerializer {
            elements: Vec::with_capacity(len),
        })
    }

    fn serialize_tuple_struct(
        self,
        _name: &'static str,
        len: usize,
    ) -> Result<Self::SerializeTupleStruct, Self::Error> {
        Ok(ValueTupleSerializer {
            elements: Vec::with_capacity(len),
        })
    }

    fn serialize_tuple_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        variant: &'static str,
        len: usize,
    ) -> Result<Self::SerializeTupleVariant, Self::Error> {
        Ok(ValueTupleVariantSerializer {
            name: variant,
            elements: Vec::with_capacity(len + 1),
        })
    }

    fn serialize_map(self, len: Option<usize>) -> Result<Self::SerializeMap, Self::Error> {
        Ok(ValueMapSerializer {
            elements: if let Some(len) = len {
                Vec::with_capacity(len)
            } else {
                Vec::new()
            },
            pending_entry: None,
        })
    }

    fn serialize_struct(
        self,
        _name: &'static str,
        len: usize,
    ) -> Result<Self::SerializeStruct, Self::Error> {
        Ok(ValueStructSerializer {
            fields: Vec::with_capacity(len),
        })
    }

    fn serialize_struct_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        variant: &'static str,
        len: usize,
    ) -> Result<Self::SerializeStructVariant, Self::Error> {
        Ok(ValueStructVariantSerializer {
            name: variant,
            fields: Vec::with_capacity(len),
        })
    }
}

pub struct ValueSeqSerializer {
    elements: Vec<Value>,
}

impl SerializeSeq for ValueSeqSerializer {
    type Ok = Value;
    type Error = SerializationError;

    fn serialize_element<T: ?Sized>(&mut self, value: &T) -> Result<(), Self::Error>
    where
        T: Serialize,
    {
        self.elements.push(value.serialize(ValueSerializer)?);
        Ok(())
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        Ok(Value::Collection(Collection::List(self.elements)))
    }
}

pub struct ValueMapSerializer {
    elements: Vec<(Value, Value)>,
    pending_entry: Option<Value>,
}

impl SerializeMap for ValueMapSerializer {
    type Ok = Value;
    type Error = SerializationError;

    fn end(self) -> Result<Self::Ok, Self::Error> {
        Ok(Value::Collection(Collection::Dict(self.elements)))
    }

    fn serialize_key<T: ?Sized>(&mut self, key: &T) -> Result<(), Self::Error>
    where
        T: Serialize,
    {
        self.pending_entry = Some(key.serialize(ValueSerializer)?);
        Ok(())
    }

    fn serialize_value<T: ?Sized>(&mut self, value: &T) -> Result<(), Self::Error>
    where
        T: Serialize,
    {
        self.elements.push((
            self.pending_entry
                .take()
                .ok_or_else(|| SerializationError::Other(format!("value without key in map")))?,
            value.serialize(ValueSerializer)?,
        ));
        Ok(())
    }
}

pub struct ValueTupleSerializer {
    elements: Vec<Value>,
}

impl SerializeTuple for ValueTupleSerializer {
    type Ok = Value;
    type Error = SerializationError;

    fn serialize_element<T: ?Sized>(&mut self, value: &T) -> Result<(), Self::Error>
    where
        T: Serialize,
    {
        self.elements.push(value.serialize(ValueSerializer)?);
        Ok(())
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        Ok(Value::Collection(Collection::Tuple(self.elements)))
    }
}

impl SerializeTupleStruct for ValueTupleSerializer {
    type Ok = Value;
    type Error = SerializationError;

    fn serialize_field<T: ?Sized>(&mut self, value: &T) -> Result<(), Self::Error>
    where
        T: Serialize,
    {
        self.elements.push(value.serialize(ValueSerializer)?);
        Ok(())
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        Ok(Value::Collection(Collection::Tuple(self.elements)))
    }
}

pub struct ValueTupleVariantSerializer {
    name: &'static str,
    elements: Vec<Value>,
}

impl SerializeTupleVariant for ValueTupleVariantSerializer {
    type Ok = Value;

    type Error = SerializationError;

    fn serialize_field<T: ?Sized>(&mut self, value: &T) -> Result<(), Self::Error>
    where
        T: Serialize,
    {
        self.elements.push(value.serialize(ValueSerializer)?);
        Ok(())
    }

    fn end(mut self) -> Result<Self::Ok, Self::Error> {
        self.elements
            .insert(0, Value::Primitive(Primitive::static_str(self.name)));
        Ok(Value::Collection(Collection::Tuple(self.elements)))
    }
}

pub struct ValueStructSerializer {
    fields: Vec<(Cow<'static, str>, Value)>,
}

impl SerializeStruct for ValueStructSerializer {
    type Ok = Value;

    type Error = SerializationError;

    fn serialize_field<T: ?Sized>(
        &mut self,
        key: &'static str,
        value: &T,
    ) -> Result<(), Self::Error>
    where
        T: Serialize,
    {
        self.fields
            .push((key.into(), value.serialize(ValueSerializer)?));
        Ok(())
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        Ok(Value::Collection(Collection::Record(self.fields)))
    }
}

pub struct ValueStructVariantSerializer {
    name: &'static str,
    fields: Vec<(Cow<'static, str>, Value)>,
}

impl SerializeStructVariant for ValueStructVariantSerializer {
    type Ok = Value;

    type Error = SerializationError;

    fn serialize_field<T: ?Sized>(
        &mut self,
        key: &'static str,
        value: &T,
    ) -> Result<(), Self::Error>
    where
        T: Serialize,
    {
        self.fields
            .push((key.into(), value.serialize(ValueSerializer)?));
        Ok(())
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        Ok(Value::Collection(Collection::tuple([
            Value::Primitive(Primitive::static_str(self.name)),
            Value::Collection(Collection::Record(self.fields)),
        ])))
    }
}

impl<'de> Deserialize<'de> for Value {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_any(ValueVisitor::default())
    }
}

#[derive(Default)]
struct ValueVisitor {
    in_struct: bool,
}

impl<'de> Visitor<'de> for ValueVisitor {
    type Value = Value;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        write!(formatter, "a value")
    }

    fn visit_newtype_struct<D>(mut self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: Deserializer<'de>,
    {
        self.in_struct = true;
        deserializer.deserialize_any(self)
    }

    fn visit_none<E>(self) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Ok(Value::Primitive(Primitive::Nil))
    }

    fn visit_i128<E>(self, v: i128) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Ok(Value::Primitive(Primitive::i128(v)))
    }

    fn visit_i64<E>(self, v: i64) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Ok(Value::Primitive(Primitive::i64(v)))
    }

    fn visit_i32<E>(self, v: i32) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Ok(Value::Primitive(Primitive::i32(v)))
    }

    fn visit_i16<E>(self, v: i16) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Ok(Value::Primitive(Primitive::i16(v)))
    }

    fn visit_i8<E>(self, v: i8) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Ok(Value::Primitive(Primitive::i8(v)))
    }

    fn visit_u128<E>(self, v: u128) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Ok(Value::Primitive(Primitive::u128(v)))
    }

    fn visit_u64<E>(self, v: u64) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Ok(Value::Primitive(Primitive::u64(v)))
    }

    fn visit_u32<E>(self, v: u32) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Ok(Value::Primitive(Primitive::u32(v)))
    }

    fn visit_u16<E>(self, v: u16) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Ok(Value::Primitive(Primitive::u16(v)))
    }

    fn visit_u8<E>(self, v: u8) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Ok(Value::Primitive(Primitive::u8(v)))
    }

    fn visit_f64<E>(self, v: f64) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Ok(Value::Primitive(Primitive::f64(v)))
    }

    fn visit_f32<E>(self, v: f32) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Ok(Value::Primitive(Primitive::f32(v)))
    }

    fn visit_bool<E>(self, v: bool) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Ok(Value::Primitive(Primitive::Boolean(v)))
    }

    fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        self.visit_string(v.into())
    }

    fn visit_string<E>(self, v: String) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Ok(Value::Primitive(Primitive::String(v.into())))
    }

    fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
    where
        A: SeqAccess<'de>,
    {
        let mut elements = if let Some(len) = seq.size_hint() {
            Vec::with_capacity(len)
        } else {
            Vec::new()
        };
        while let Some(element) = seq.next_element()? {
            elements.push(element);
        }
        Ok(Value::Collection(Collection::List(elements)))
    }

    fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
    where
        A: MapAccess<'de>,
    {
        let mut fields = if let Some(len) = map.size_hint() {
            Vec::with_capacity(len)
        } else {
            Vec::new()
        };
        while let Some(entry) = map.next_entry()? {
            fields.push(entry);
        }
        Ok(Value::Collection(Collection::Dict(fields)))
    }

    fn visit_unit<E>(self) -> Result<Self::Value, E> {
        Ok(Value::Primitive(Primitive::Nil))
    }

    fn visit_char<E>(self, v: char) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        let mut s = String::with_capacity(1);
        s.push(v);
        Ok(Value::Primitive(Primitive::String(s.into())))
    }

    fn visit_some<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_any(self)
    }

    fn visit_bytes<E>(self, v: &[u8]) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Ok(Value::Collection(Collection::list(
            v.into_iter().copied().map(Primitive::u8),
        )))
    }

    fn visit_byte_buf<E>(self, v: Vec<u8>) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        self.visit_bytes(&v)
    }
}

enum Deserializable<'de> {
    Value(&'de Value),
    FieldName(Cow<'de, str>),
    Nil,
}

impl<'de> fmt::Debug for Deserializable<'de> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Value(v) => v.fmt(f),
            Self::FieldName(v) => v.fmt(f),
            Self::Nil => write!(f, "<nil>"),
        }
    }
}

pub struct ValueDeserializer<'de> {
    value: Deserializable<'de>,
    type_: Type,
}

impl<'de> ValueDeserializer<'de> {
    pub fn new(value: &'de Value, type_: Type) -> Self {
        Self {
            value: Deserializable::Value(value),
            type_,
        }
    }

    pub fn new_field_name(value: Cow<'de, str>) -> Self {
        Self {
            value: Deserializable::FieldName(value),
            type_: Type::Primitive(PrimitiveType::String),
        }
    }
}

impl<'de> Deserializer<'de> for ValueDeserializer<'de> {
    type Error = SerializationError;

    fn deserialize_any<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        dbg!((&self.value, self.type_.as_not_named()));
        match (self.value, self.type_.when_not_named()) {
            (Deserializable::Value(Value::Primitive(Primitive::Nil)), _) => visitor.visit_none(),
            (Deserializable::Value(Value::Primitive(Primitive::Integer(i))), _) => {
                visitor.visit_i64(*i)
            }
            (Deserializable::Value(Value::Primitive(Primitive::Float(f))), _) => {
                visitor.visit_f64(*f)
            }
            (Deserializable::Value(Value::Primitive(Primitive::Boolean(b))), _) => {
                visitor.visit_bool(*b)
            }
            (Deserializable::Value(Value::Primitive(Primitive::String(s))), _) => {
                visitor.visit_str(s.as_ref())
            }

            (
                Deserializable::Value(Value::Collection(Collection::List(l))),
                Type::Composite(CompositeType::List(type_)),
            ) => visitor.visit_seq(ValueSeqDeserializer {
                iterator: l.iter().zip(std::iter::repeat(*type_)),
            }),
            (Deserializable::Value(Value::Collection(Collection::List(_))), t) => Err(
                SerializationError::Other(format!("type mismatch: {:?} is not a list type", t)),
            ),

            (
                Deserializable::Value(Value::Collection(Collection::Dict(d))),
                Type::Composite(CompositeType::Dict(key_type, value_type)),
            ) => visitor.visit_map(ValueMapDeserializer {
                key_type: *key_type,
                value_type: *value_type,
                iterator: d
                    .iter()
                    .map(|(k, v)| (Deserializable::Value(k), Deserializable::Value(v))),
                trailing_value: None,
            }),
            (
                Deserializable::Value(Value::Collection(Collection::Dict(d))),
                Type::Composite(CompositeType::Record(r)),
            ) => visitor.visit_map(ValueStructMapDeserializer {
                iterator: d
                    .iter()
                    .filter_map(|(k, v)| match k {
                        Value::Primitive(Primitive::String(s)) => Some((s, v)),
                        _ => None,
                    })
                    .filter_map(|(k, v)| {
                        Some((
                            k.clone(),
                            Deserializable::Value(v),
                            r.iter().find_map(|(f, t)| (f == k).then_some(t))?.clone(),
                        ))
                    }),
                trailing_value: None,
            }),
            (Deserializable::Value(Value::Collection(Collection::Dict(_))), t) => {
                Err(SerializationError::Other(format!(
                    "type mismatch: {:?} is not a dictionary type",
                    t
                )))
            }

            (Deserializable::Value(Value::Collection(Collection::Record(r))), t) => visitor
                .visit_map(ValueStructMapDeserializer {
                    iterator: r.iter().map(|(k, v)| {
                        (
                            k.clone(),
                            Deserializable::Value(v),
                            t.access_member(k.as_ref()).clone(),
                        )
                    }),
                    trailing_value: None,
                }),
            (
                Deserializable::Value(Value::Collection(Collection::Tuple(t))),
                Type::Composite(CompositeType::Tuple(type_)),
            ) => visitor.visit_seq(ValueSeqDeserializer {
                iterator: t.iter().zip(type_.into_iter()),
            }),

            (Deserializable::Value(Value::Collection(Collection::Tuple(_))), t) => Err(
                SerializationError::Other(format!("type mismatch: {:?} is not a tuple type", t)),
            ),

            (Deserializable::FieldName(f), _) => visitor.visit_str(f.as_ref()),

            (Deserializable::Nil, _) => visitor.visit_none(),
        }
    }

    fn deserialize_bool<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        match self.value {
            Deserializable::Value(Value::Primitive(Primitive::Boolean(b))) => {
                visitor.visit_bool(*b)
            }
            v => Err(SerializationError::Other(format!(
                "{:?} is not a boolean",
                v
            ))),
        }
    }

    fn deserialize_i8<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        match self.value {
            Deserializable::Value(Value::Primitive(Primitive::Integer(v))) => {
                visitor.visit_i8(*v as _)
            }
            v => Err(SerializationError::Other(format!(
                "{:?} is not an integer",
                v
            ))),
        }
    }

    fn deserialize_i16<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        match self.value {
            Deserializable::Value(Value::Primitive(Primitive::Integer(v))) => {
                visitor.visit_i16(*v as _)
            }
            v => Err(SerializationError::Other(format!(
                "{:?} is not an integer",
                v
            ))),
        }
    }

    fn deserialize_i32<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        match self.value {
            Deserializable::Value(Value::Primitive(Primitive::Integer(v))) => {
                visitor.visit_i32(*v as _)
            }
            v => Err(SerializationError::Other(format!(
                "{:?} is not an integer",
                v
            ))),
        }
    }

    fn deserialize_i64<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        match self.value {
            Deserializable::Value(Value::Primitive(Primitive::Integer(v))) => {
                visitor.visit_i64(*v as _)
            }
            v => Err(SerializationError::Other(format!(
                "{:?} is not an integer",
                v
            ))),
        }
    }

    fn deserialize_i128<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        match self.value {
            Deserializable::Value(Value::Primitive(Primitive::Integer(v))) => {
                visitor.visit_i128(*v as _)
            }
            v => Err(SerializationError::Other(format!(
                "{:?} is not an integer",
                v
            ))),
        }
    }

    fn deserialize_u8<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        match self.value {
            Deserializable::Value(Value::Primitive(Primitive::Integer(v))) => {
                visitor.visit_u8(*v as _)
            }
            v => Err(SerializationError::Other(format!(
                "{:?} is not an integer",
                v
            ))),
        }
    }

    fn deserialize_u16<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        match self.value {
            Deserializable::Value(Value::Primitive(Primitive::Integer(v))) => {
                visitor.visit_u16(*v as _)
            }
            v => Err(SerializationError::Other(format!(
                "{:?} is not an integer",
                v
            ))),
        }
    }

    fn deserialize_u32<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        match self.value {
            Deserializable::Value(Value::Primitive(Primitive::Integer(v))) => {
                visitor.visit_u32(*v as _)
            }
            v => Err(SerializationError::Other(format!(
                "{:?} is not an integer",
                v
            ))),
        }
    }

    fn deserialize_u64<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        match self.value {
            Deserializable::Value(Value::Primitive(Primitive::Integer(v))) => {
                visitor.visit_u64(*v as _)
            }
            v => Err(SerializationError::Other(format!(
                "{:?} is not an integer",
                v
            ))),
        }
    }

    fn deserialize_u128<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        match self.value {
            Deserializable::Value(Value::Primitive(Primitive::Integer(v))) => {
                visitor.visit_u128(*v as _)
            }
            v => Err(SerializationError::Other(format!(
                "{:?} is not an integer",
                v
            ))),
        }
    }

    fn deserialize_f32<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        match self.value {
            Deserializable::Value(Value::Primitive(Primitive::Float(v))) => {
                visitor.visit_f32(*v as _)
            }
            v => Err(SerializationError::Other(format!(
                "{:?} is not a floating point number",
                v
            ))),
        }
    }

    fn deserialize_f64<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        match self.value {
            Deserializable::Value(Value::Primitive(Primitive::Float(v))) => visitor.visit_f64(*v),
            v => Err(SerializationError::Other(format!(
                "{:?} is not a floating point number",
                v
            ))),
        }
    }

    fn deserialize_char<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        match self.value {
            Deserializable::Value(Value::Primitive(Primitive::String(v))) if v.len() == 1 => {
                visitor.visit_char(v.chars().next().unwrap())
            }
            v => Err(SerializationError::Other(format!(
                "{:?} is not a single character string",
                v
            ))),
        }
    }

    fn deserialize_str<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        match self.value {
            Deserializable::Value(Value::Primitive(Primitive::String(v))) => {
                visitor.visit_str(v.as_ref())
            }
            v => Err(SerializationError::Other(format!(
                "{:?} is not a string",
                v
            ))),
        }
    }

    fn deserialize_string<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        match self.value {
            Deserializable::Value(Value::Primitive(Primitive::String(v))) => {
                visitor.visit_string(v.to_string())
            }
            v => Err(SerializationError::Other(format!(
                "{:?} is not a string",
                v
            ))),
        }
    }

    fn deserialize_bytes<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        match self.value {
            Deserializable::Value(Value::Collection(Collection::List(l))) => {
                let mut bytes = vec![];
                for el in l {
                    if let Value::Primitive(Primitive::Integer(b)) = el {
                        bytes.push(*b as _);
                    } else {
                        return Err(SerializationError::Other(format!(
                            "{:?} is not a list of integers",
                            self.value
                        )));
                    }
                }
                visitor.visit_bytes(&bytes)
            }
            v => Err(SerializationError::Other(format!(
                "{:?} is not a list of integers",
                v
            ))),
        }
    }

    fn deserialize_byte_buf<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        match self.value {
            Deserializable::Value(Value::Collection(Collection::List(l))) => {
                let mut bytes = vec![];
                for el in l {
                    if let Value::Primitive(Primitive::Integer(b)) = el {
                        bytes.push(*b as _);
                    } else {
                        return Err(SerializationError::Other(format!(
                            "{:?} is not a list of integers",
                            self.value
                        )));
                    }
                }
                visitor.visit_byte_buf(bytes)
            }
            v => Err(SerializationError::Other(format!(
                "{:?} is not a list of integers",
                v
            ))),
        }
    }

    fn deserialize_option<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        if let Deserializable::Value(Value::Primitive(Primitive::Nil)) | Deserializable::Nil =
            self.value
        {
            visitor.visit_none()
        } else {
            visitor.visit_some(self)
        }
    }

    fn deserialize_unit<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        match self.value {
            Deserializable::Value(Value::Primitive(Primitive::Nil)) => visitor.visit_unit(),
            v => Err(SerializationError::Other(format!("{:?} is not nil", v))),
        }
    }

    fn deserialize_unit_struct<V>(
        self,
        _name: &'static str,
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        visitor.visit_unit()
    }

    fn deserialize_newtype_struct<V>(
        self,
        _name: &'static str,
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        visitor.visit_newtype_struct(self)
    }

    fn deserialize_seq<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        match (self.value, self.type_.when_not_named()) {
            (
                Deserializable::Value(Value::Collection(Collection::List(t))),
                Type::Composite(CompositeType::List(type_)),
            ) => visitor.visit_seq(ValueSeqDeserializer {
                iterator: t.iter().zip(std::iter::repeat(*type_)),
            }),
            v => Err(SerializationError::Other(format!("{:?} is not a list", v))),
        }
    }

    fn deserialize_tuple<V>(self, len: usize, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        match (self.value, self.type_.when_not_named()) {
            (
                Deserializable::Value(Value::Collection(Collection::Tuple(t))),
                Type::Composite(CompositeType::Tuple(ty)),
            ) => {
                if t.len() != len {
                    return Err(SerializationError::Other(format!(
                        "{:?} does not match expected length of {}",
                        Value::Collection(Collection::Tuple(t.clone())),
                        len
                    )));
                }

                visitor.visit_seq(ValueSeqDeserializer {
                    iterator: t.iter().zip(ty.into_iter()),
                })
            }
            v => Err(SerializationError::Other(format!("{:?} is not a tuple", v))),
        }
    }

    fn deserialize_tuple_struct<V>(
        self,
        _name: &'static str,
        len: usize,
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        self.deserialize_tuple(len, visitor)
    }

    fn deserialize_map<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        match (self.value, self.type_.when_not_named()) {
            (
                Deserializable::Value(Value::Collection(Collection::Dict(d))),
                Type::Composite(CompositeType::Dict(key_type, value_type)),
            ) => visitor.visit_map(ValueMapDeserializer {
                key_type: *key_type,
                value_type: *value_type,
                iterator: d
                    .iter()
                    .map(|(k, v)| (Deserializable::Value(k), Deserializable::Value(v))),
                trailing_value: None,
            }),
            v => Err(SerializationError::Other(format!(
                "{:?} is not a dictionary",
                v
            ))),
        }
    }

    fn deserialize_struct<V>(
        self,
        _name: &'static str,
        fields: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        match (self.value, self.type_.when_not_named()) {
            (
                Deserializable::Value(Value::Collection(Collection::Record(r))),
                Type::Composite(CompositeType::Record(rt)),
            ) => visitor.visit_map(ValueStructMapDeserializer {
                trailing_value: None,
                iterator: fields.into_iter().map(|field| {
                    match r.iter().find(|(k, _)| k == field) {
                        None => (
                            Cow::Owned(field.to_string()),
                            Deserializable::Nil,
                            Type::VOID,
                        ),
                        Some((k, v)) => (
                            k.clone(),
                            Deserializable::Value(v),
                            rt.iter()
                                .cloned()
                                .find_map(|(f, v)| (f == *field).then_some(v))
                                .unwrap_or(Type::VOID),
                        ),
                    }
                }),
            }),
            (
                Deserializable::Value(Value::Collection(Collection::Dict(d))),
                Type::Composite(CompositeType::Dict(key_type, value_type)),
            ) => visitor.visit_map(ValueMapDeserializer {
                trailing_value: None,
                key_type: *key_type,
                value_type: *value_type,
                iterator: fields.into_iter().map(|field| {
                    d.iter()
                        .find(|(k, _)| match k {
                            Value::Primitive(Primitive::String(k)) => k == field,
                            _ => false,
                        })
                        .map(|(k, v)| (Deserializable::Value(k), Deserializable::Value(v)))
                        .unwrap_or((
                            Deserializable::FieldName(Cow::Borrowed(field)),
                            Deserializable::Nil,
                        ))
                }),
            }),
            (
                Deserializable::Value(Value::Collection(Collection::Dict(r))),
                Type::Composite(CompositeType::Record(rt)),
            ) => visitor.visit_map(ValueStructMapDeserializer {
                trailing_value: None,
                iterator: fields.into_iter().map(|field| {
                    match r
                        .iter()
                        .filter_map(|(k, v)| match k {
                            Value::Primitive(Primitive::String(k)) => Some((k, v)),
                            _ => None,
                        })
                        .find(|(k, _)| k == field)
                    {
                        None => (
                            Cow::Owned(field.to_string()),
                            Deserializable::Nil,
                            Type::VOID,
                        ),
                        Some((k, v)) => (
                            k.clone(),
                            Deserializable::Value(v),
                            rt.iter()
                                .cloned()
                                .find_map(|(f, v)| (f == *field).then_some(v))
                                .unwrap_or(Type::VOID),
                        ),
                    }
                }),
            }),
            (v, t) => Err(SerializationError::Other(format!(
                "{:?} is not a record or dictionary with string keys as described by {:?}",
                v, t
            ))),
        }
    }

    fn deserialize_enum<V>(
        self,
        name: &'static str,
        variants: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        match (&self.value, self.type_.when_not_named()) {
            (Deserializable::Value(v @ Value::Collection(Collection::Tuple(t))), ty) => {
                if t.len() > 0 {
                    if let Value::Primitive(Primitive::String(tag)) = &t[0] {
                        for variant in variants {
                            if variant == tag {
                                return visitor.visit_enum(ValueEnumDeserializer {
                                    tag: &t[0],
                                    value: Some(v),
                                    type_: Some(ty),
                                });
                            }
                        }
                        return Err(SerializationError::Other(format!(
                            "{:?} is not a valid variant of {:?} {:?}",
                            tag, name, variants
                        )));
                    }
                }
            }

            (Deserializable::Value(t @ Value::Primitive(Primitive::String(tag))), _) => {
                for variant in variants {
                    if variant == tag {
                        return visitor.visit_enum(ValueEnumDeserializer {
                            tag: t,
                            value: None,
                            type_: None,
                        });
                    }
                }
                return Err(SerializationError::Other(format!(
                    "{:?} is not a valid variant of {:?} {:?}",
                    tag, name, variants
                )));
            }

            _ => {}
        }

        Err(SerializationError::Other(format!(
            "{:?} is not a tagged enum variant",
            self.value
        )))
    }

    fn deserialize_identifier<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        match self.value {
            Deserializable::FieldName(n) => visitor.visit_str(n.as_ref()),
            Deserializable::Value(Value::Primitive(Primitive::String(s))) => {
                visitor.visit_str(s.as_ref())
            }
            v => Err(SerializationError::Other(format!(
                "{:?} is not valid as a field name",
                v
            ))),
        }
    }

    fn deserialize_ignored_any<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        self.deserialize_any(visitor)
    }
}

struct ValueEnumDeserializer<'de> {
    tag: &'de Value,
    value: Option<&'de Value>,
    type_: Option<Type>,
}

impl<'de> EnumAccess<'de> for ValueEnumDeserializer<'de> {
    type Error = SerializationError;

    type Variant = ValueVariantDeserializer<'de>;

    fn variant_seed<V>(self, seed: V) -> Result<(V::Value, Self::Variant), Self::Error>
    where
        V: serde::de::DeserializeSeed<'de>,
    {
        let v = seed.deserialize(ValueDeserializer::new(self.tag, Type::STRING))?;

        Ok((
            v,
            ValueVariantDeserializer {
                value: self.value,
                type_: self.type_,
            },
        ))
    }
}

struct ValueVariantDeserializer<'de> {
    value: Option<&'de Value>,
    type_: Option<Type>,
}

impl<'de> VariantAccess<'de> for ValueVariantDeserializer<'de> {
    type Error = SerializationError;

    fn unit_variant(self) -> Result<(), Self::Error> {
        match self.value {
            None => Ok(()),
            Some(v) => Err(SerializationError::Other(format!(
                "{:?} was provided to a unit variant",
                v
            ))),
        }
    }

    fn newtype_variant_seed<T>(self, seed: T) -> Result<T::Value, Self::Error>
    where
        T: serde::de::DeserializeSeed<'de>,
    {
        match (self.value, self.type_.map(Type::when_not_named)) {
            (Some(Value::Collection(Collection::Tuple(t))), Some(ty)) if t.len() == 2 => {
                seed.deserialize(ValueDeserializer::new(&t[1], ty))
            }
            (Some(v), _) => Err(SerializationError::Other(format!(
                "{:?} should be a tagged tuple with the tag and a single value",
                v,
            ))),
            (None, _) => Err(SerializationError::Other(format!(
                "Non-unit variant didn't receive a value",
            ))),
        }
    }

    fn tuple_variant<V>(self, len: usize, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        match (self.value, self.type_.map(Type::when_not_named)) {
            (
                Some(Value::Collection(Collection::Tuple(t))),
                Some(Type::Composite(CompositeType::Tuple(ty))),
            ) if t.len() == len + 1 => visitor.visit_seq(ValueSeqDeserializer {
                iterator: t.iter().skip(1).zip(ty.into_iter()),
            }),
            (Some(v), _) => Err(SerializationError::Other(format!(
                "{:?} should be a tagged tuple with {} elements (including the tag)",
                v, len,
            ))),
            (None, _) => Err(SerializationError::Other(format!(
                "Non-unit variant didn't receive a value",
            ))),
        }
    }

    fn struct_variant<V>(
        self,
        fields: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        match (self.value, self.type_.map(Type::when_not_named)) {
            (Some(v @ Value::Collection(Collection::Tuple(t))), Some(ty)) if t.len() == 2 => {
                if let Value::Collection(Collection::Record(r)) = &t[1] {
                    visitor.visit_map(ValueStructMapDeserializer {
                        trailing_value: None,
                        iterator: fields.into_iter().map(|field| {
                            match r.iter().find(|(k, _)| k == field) {
                                None => (
                                    Cow::Owned(field.to_string()),
                                    Deserializable::Nil,
                                    ty.access_member(field).clone(),
                                ),
                                Some((k, v)) => (
                                    k.clone(),
                                    Deserializable::Value(v),
                                    ty.access_member(k.as_ref()).clone(),
                                ),
                            }
                        }),
                    })
                } else {
                    Err(SerializationError::Other(format!(
                        "{:?} should be a tagged tuple with a record",
                        v,
                    )))
                }
            }
            (Some(v), _) => Err(SerializationError::Other(format!(
                "{:?} should be a tagged tuple with a record",
                v,
            ))),
            (None, _) => Err(SerializationError::Other(format!(
                "Non-unit variant didn't receive a value",
            ))),
        }
    }
}

struct ValueSeqDeserializer<I> {
    iterator: I,
}

impl<'de, I> SeqAccess<'de> for ValueSeqDeserializer<I>
where
    I: Iterator<Item = (&'de Value, Type)>,
{
    type Error = SerializationError;

    fn next_element_seed<T>(&mut self, seed: T) -> Result<Option<T::Value>, Self::Error>
    where
        T: serde::de::DeserializeSeed<'de>,
    {
        if let Some((element, ty)) = self.iterator.next() {
            Ok(Some(seed.deserialize(ValueDeserializer::new(element, ty))?))
        } else {
            Ok(None)
        }
    }
}

struct ValueMapDeserializer<'de, I> {
    iterator: I,
    trailing_value: Option<Deserializable<'de>>,
    key_type: Type,
    value_type: Type,
}

impl<'de, I> MapAccess<'de> for ValueMapDeserializer<'de, I>
where
    I: Iterator<Item = (Deserializable<'de>, Deserializable<'de>)>,
{
    type Error = SerializationError;

    fn next_key_seed<K>(&mut self, seed: K) -> Result<Option<K::Value>, Self::Error>
    where
        K: serde::de::DeserializeSeed<'de>,
    {
        if let Some((key, value)) = self.iterator.next() {
            self.trailing_value = Some(value);
            Ok(Some(seed.deserialize(ValueDeserializer {
                value: key,
                type_: self.key_type.clone(),
            })?))
        } else {
            Ok(None)
        }
    }

    fn next_value_seed<V>(&mut self, seed: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::DeserializeSeed<'de>,
    {
        seed.deserialize(ValueDeserializer {
            type_: self.value_type.clone(),
            value: self
                .trailing_value
                .take()
                .ok_or_else(|| SerializationError::Other("value without key".to_string()))?,
        })
    }
}

struct ValueStructMapDeserializer<'de, I> {
    iterator: I,
    trailing_value: Option<(Deserializable<'de>, Type)>,
}

impl<'de, I> MapAccess<'de> for ValueStructMapDeserializer<'de, I>
where
    I: Iterator<Item = (Cow<'de, str>, Deserializable<'de>, Type)>,
{
    type Error = SerializationError;

    fn next_key_seed<K>(&mut self, seed: K) -> Result<Option<K::Value>, Self::Error>
    where
        K: serde::de::DeserializeSeed<'de>,
    {
        if let Some((key, value, type_)) = self.iterator.next() {
            self.trailing_value = Some((value, type_));
            Ok(Some(
                seed.deserialize(ValueDeserializer::new_field_name(key))?,
            ))
        } else {
            Ok(None)
        }
    }

    fn next_value_seed<V>(&mut self, seed: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::DeserializeSeed<'de>,
    {
        let (value, type_) = self
            .trailing_value
            .take()
            .ok_or_else(|| SerializationError::Other("value without key".to_string()))?;

        seed.deserialize(ValueDeserializer { value, type_ })
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use serde::{Deserialize, Serialize};

    use crate::analyze::Type;
    use crate::{Collection, Primitive, TypeOf, Value};

    fn test_value(input: Value, type_: Type) {
        let serialized = Value::serialize(&input).unwrap();
        assert_eq!(serialized, input);
        let output: Value = serialized.deserialize(type_).unwrap();
        assert_eq!(output, input);
    }

    #[test]
    fn isometric_self_encoding_value() {
        test_value(
            Value::Primitive(Primitive::Nil),
            Type::optional(Type::STRING),
        );
        test_value(Value::Primitive(Primitive::Integer(123)), Type::INTEGER);
        test_value(Value::Primitive(Primitive::Float(32.432)), Type::FLOAT);
        test_value(Value::Primitive(Primitive::Boolean(true)), Type::BOOLEAN);
        test_value(Value::Primitive(Primitive::string("owned")), Type::STRING);
        test_value(
            Value::Primitive(Primitive::static_str("borrowed")),
            Type::STRING,
        );

        test_value(
            Value::Collection(Collection::list([
                Value::Primitive(Primitive::Integer(123)),
                Value::Primitive(Primitive::Integer(456)),
            ])),
            Type::list(Type::INTEGER),
        );

        test_value(
            Value::Collection(Collection::dict([
                (
                    Value::Primitive(Primitive::static_str("one")),
                    Value::Primitive(Primitive::Integer(123)),
                ),
                (
                    Value::Primitive(Primitive::static_str("two")),
                    Value::Primitive(Primitive::Integer(234)),
                ),
            ])),
            Type::dict(Type::STRING, Type::INTEGER),
        );
    }

    #[test]
    fn derived_serialization() {
        #[derive(Serialize, Deserialize, Debug, PartialEq)]
        struct X {
            a: usize,
            b: u64,
            c: (),
            d: (String, i32),
            e: BTreeMap<String, usize>,
            f: Vec<(usize, f64, BTreeMap<String, usize>)>,
            g: Vec<Y>,
            h: Option<u64>,
            i: Option<u64>,
            k: K,
            l: L,
            m: M,
            n: Vec<u8>,
        }

        impl TypeOf for X {
            fn type_of() -> Type {
                Type::record([
                    ("a", usize::type_of()),
                    ("b", u64::type_of()),
                    ("c", <()>::type_of()),
                    ("d", <(String, i32)>::type_of()),
                    ("e", BTreeMap::<String, usize>::type_of()),
                    (
                        "f",
                        <Vec<(usize, f64, BTreeMap<String, usize>)> as TypeOf>::type_of(),
                    ),
                    ("g", Vec::<Y>::type_of()),
                    ("h", Option::<u64>::type_of()),
                    ("i", Option::<u64>::type_of()),
                    ("k", K::type_of()),
                    ("l", L::type_of()),
                    ("m", M::type_of()),
                    ("n", Vec::<u8>::type_of()),
                ])
            }
        }

        #[derive(Serialize, Deserialize, Debug, PartialEq)]
        enum Y {
            One,
            Two(usize),
            Three(String, String),
            Four { a: usize, b: f64 },
        }

        impl TypeOf for Y {
            fn type_of() -> Type {
                Type::tuple([Type::STRING, Type::open()])
            }
        }

        #[derive(Serialize, Deserialize, Debug, PartialEq)]
        struct K;

        impl TypeOf for K {
            fn type_of() -> Type {
                Type::VOID
            }
        }

        #[derive(Serialize, Deserialize, Debug, PartialEq)]
        struct L(String);

        impl TypeOf for L {
            fn type_of() -> Type {
                Type::STRING
            }
        }

        #[derive(Serialize, Deserialize, Debug, PartialEq)]
        struct M(String, usize);

        impl TypeOf for M {
            fn type_of() -> Type {
                Type::tuple([Type::STRING, Type::INTEGER])
            }
        }

        let input = X {
            a: 12,
            b: 543,
            c: (),
            d: ("hello".into(), 423),
            e: [("field".to_string(), 123)].into_iter().collect(),
            f: vec![(123, 43.123, Default::default())],
            g: vec![
                Y::One,
                Y::Two(123),
                Y::Three("a".into(), "b".into()),
                Y::Four { a: 123, b: 43.32 },
            ],
            h: None,
            i: Some(123),
            k: K,
            l: L("whatever".into()),
            m: M("m".into(), 123),
            n: vec![255, 255, 255, 255],
        };

        let serialized = Value::serialize(&input).unwrap();

        let output: X = serialized.deserialize_typed().unwrap();

        assert_eq!(output, input);
    }

    #[test]
    fn serializing_from_derive_into_value() {
        #[derive(Serialize)]
        struct X {
            a: Vec<u8>,
        }

        impl TypeOf for X {
            fn type_of() -> Type {
                Type::record([("a", Vec::<u8>::type_of())])
            }
        }

        let serialized = Value::serialize(&X { a: vec![1, 2, 3] }).unwrap();
        let value: Value = serialized.deserialize(Type::default()).unwrap();

        assert_eq!(
            value,
            Value::Collection(Collection::Dict(vec![(
                Value::Primitive(Primitive::String("a".into())),
                Value::Collection(Collection::List(vec![
                    Value::Primitive(Primitive::Integer(1)),
                    Value::Primitive(Primitive::Integer(2)),
                    Value::Primitive(Primitive::Integer(3)),
                ]))
            )]))
        )
    }

    #[test]
    fn serializing_from_value_into_deserialize() {
        #[derive(Deserialize, Debug, PartialEq)]
        struct X {
            a: Vec<u8>,
        }

        impl TypeOf for X {
            fn type_of() -> Type {
                Type::record([("a", Vec::<u8>::type_of())])
            }
        }

        let serialized = Value::serialize(&Value::Collection(Collection::Dict(vec![(
            Value::Primitive(Primitive::String("a".into())),
            Value::Collection(Collection::List(vec![
                Value::Primitive(Primitive::Integer(1)),
                Value::Primitive(Primitive::Integer(2)),
                Value::Primitive(Primitive::Integer(3)),
            ])),
        )])))
        .unwrap();
        let value: X = serialized.deserialize_typed().unwrap();

        assert_eq!(value, X { a: vec![1, 2, 3] })
    }

    #[test]
    fn serde_through_msgpack() {
        let input = Value::record([
            ("a", Value::dict([(Value::string("test"), Value::i64(32))])),
            ("b", Value::list([Value::i32(12), Value::i32(34)])),
        ]);

        let serialized = rmp_serde::to_vec_named(&input).unwrap();

        let output: Value = rmp_serde::from_slice(&serialized).unwrap();
        let output = output.coerce(&Type::record([
            ("a", Type::dict(Type::STRING, Type::INTEGER)),
            ("b", Type::list(Type::INTEGER)),
        ]));

        assert_eq!(input, output);
    }
}
