use std::borrow::Cow;
use std::fmt;

use serde::de::{EnumAccess, MapAccess, SeqAccess, VariantAccess, Visitor};
use serde::ser::{
    SerializeMap, SerializeSeq, SerializeStruct, SerializeStructVariant, SerializeTuple,
    SerializeTupleStruct, SerializeTupleVariant,
};
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::{Collection, Primitive, Value};

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
            Value::Primitive(Primitive::Integer(i)) => serializer.serialize_i128(*i),
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
                let mut s = serializer.serialize_map(Some(r.len()))?;
                for (k, v) in r {
                    s.serialize_entry(k, v)?;
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
        deserializer.deserialize_any(ValueVisitor)
    }
}

struct ValueVisitor;

impl<'de> Visitor<'de> for ValueVisitor {
    type Value = Value;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        write!(formatter, "a value")
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
        Ok(Value::Primitive(Primitive::Integer(v)))
    }

    fn visit_f64<E>(self, v: f64) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Ok(Value::Primitive(Primitive::Float(v)))
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
}

enum Deserializable<'de> {
    Value(&'de Value),
    FieldName(&'de Cow<'de, str>),
}

impl<'de> fmt::Debug for Deserializable<'de> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Value(v) => v.fmt(f),
            Self::FieldName(v) => v.fmt(f),
        }
    }
}

pub struct ValueDeserializer<'de> {
    value: Deserializable<'de>,
}

impl<'de> ValueDeserializer<'de> {
    pub fn new(value: &'de Value) -> Self {
        Self {
            value: Deserializable::Value(value),
        }
    }

    pub fn new_field_name(value: &'de Cow<'de, str>) -> Self {
        Self {
            value: Deserializable::FieldName(value),
        }
    }
}

impl<'de> Deserializer<'de> for ValueDeserializer<'de> {
    type Error = SerializationError;

    fn deserialize_any<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        match self.value {
            Deserializable::Value(Value::Primitive(Primitive::Nil)) => visitor.visit_none(),
            Deserializable::Value(Value::Primitive(Primitive::Integer(i))) => {
                visitor.visit_i128(*i)
            }
            Deserializable::Value(Value::Primitive(Primitive::Float(f))) => visitor.visit_f64(*f),
            Deserializable::Value(Value::Primitive(Primitive::Boolean(b))) => {
                visitor.visit_bool(*b)
            }
            Deserializable::Value(Value::Primitive(Primitive::String(s))) => {
                visitor.visit_str(s.as_ref())
            }

            Deserializable::Value(Value::Collection(Collection::List(l))) => {
                visitor.visit_seq(ValueSeqDeserializer { iterator: l.iter() })
            }
            Deserializable::Value(Value::Collection(Collection::Dict(d))) => {
                visitor.visit_map(ValueMapDeserializer {
                    iterator: d.iter(),
                    trailing_value: None,
                })
            }
            Deserializable::Value(Value::Collection(Collection::Record(r))) => {
                visitor.visit_map(ValueStructMapDeserializer {
                    iterator: r.iter().map(|(k, v)| (k, v)),
                    trailing_value: None,
                })
            }
            Deserializable::Value(Value::Collection(Collection::Tuple(t))) => {
                visitor.visit_seq(ValueSeqDeserializer { iterator: t.iter() })
            }
            Deserializable::FieldName(f) => visitor.visit_str(f.as_ref()),
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
                visitor.visit_i128(*v)
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
        if let Deserializable::Value(Value::Primitive(Primitive::Nil)) = self.value {
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
        match self.value {
            Deserializable::Value(Value::Collection(Collection::List(t))) => {
                visitor.visit_seq(ValueSeqDeserializer { iterator: t.iter() })
            }
            v => Err(SerializationError::Other(format!("{:?} is not a list", v))),
        }
    }

    fn deserialize_tuple<V>(self, len: usize, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        match self.value {
            Deserializable::Value(Value::Collection(Collection::Tuple(t))) => {
                if t.len() != len {
                    return Err(SerializationError::Other(format!(
                        "{:?} does not match expected length of {}",
                        self.value, len
                    )));
                }

                visitor.visit_seq(ValueSeqDeserializer { iterator: t.iter() })
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
        match self.value {
            Deserializable::Value(Value::Collection(Collection::Tuple(t))) => {
                if t.len() != len {
                    return Err(SerializationError::Other(format!(
                        "{:?} does not match expected length of {}",
                        self.value, len
                    )));
                }

                visitor.visit_seq(ValueSeqDeserializer { iterator: t.iter() })
            }
            v => Err(SerializationError::Other(format!("{:?} is not a tuple", v))),
        }
    }

    fn deserialize_map<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        match self.value {
            Deserializable::Value(Value::Collection(Collection::Dict(d))) => {
                visitor.visit_map(ValueMapDeserializer {
                    iterator: d.iter(),
                    trailing_value: None,
                })
            }
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
        match self.value {
            Deserializable::Value(Value::Collection(Collection::Record(r))) => {
                let fields = fields
                    .into_iter()
                    .map(|field| match r.iter().find(|(k, _)| k == field) {
                        None => Err(SerializationError::Other(format!(
                            "Field {:?} is missing in {:?}",
                            field, self.value
                        ))),
                        Some((k, v)) => Ok((k, v)),
                    })
                    .collect::<Result<Vec<_>, _>>()?;

                visitor.visit_map(ValueStructMapDeserializer {
                    trailing_value: None,
                    iterator: fields.into_iter(),
                })
            }
            v => Err(SerializationError::Other(format!(
                "{:?} is not a record or string map",
                v
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
        if let Deserializable::Value(v @ Value::Collection(Collection::Tuple(t))) = self.value {
            if t.len() > 0 {
                if let Value::Primitive(Primitive::String(tag)) = &t[0] {
                    for variant in variants {
                        if variant == tag {
                            return visitor.visit_enum(ValueEnumDeserializer {
                                tag: &t[0],
                                value: Some(v),
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

        if let Deserializable::Value(t @ Value::Primitive(Primitive::String(tag))) = self.value {
            for variant in variants {
                if variant == tag {
                    return visitor.visit_enum(ValueEnumDeserializer {
                        tag: t,
                        value: None,
                    });
                }
            }
            return Err(SerializationError::Other(format!(
                "{:?} is not a valid variant of {:?} {:?}",
                tag, name, variants
            )));
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
}

impl<'de> EnumAccess<'de> for ValueEnumDeserializer<'de> {
    type Error = SerializationError;

    type Variant = ValueVariantDeserializer<'de>;

    fn variant_seed<V>(self, seed: V) -> Result<(V::Value, Self::Variant), Self::Error>
    where
        V: serde::de::DeserializeSeed<'de>,
    {
        let v = seed.deserialize(ValueDeserializer::new(self.tag))?;

        Ok((v, ValueVariantDeserializer { value: self.value }))
    }
}

struct ValueVariantDeserializer<'de> {
    value: Option<&'de Value>,
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
        match self.value {
            Some(Value::Collection(Collection::Tuple(t))) if t.len() == 2 => {
                seed.deserialize(ValueDeserializer::new(&t[1]))
            }
            Some(v) => Err(SerializationError::Other(format!(
                "{:?} should be a tagged tuple with the tag and a single value",
                v,
            ))),
            None => Err(SerializationError::Other(format!(
                "Non-unit variant didn't receive a value",
            ))),
        }
    }

    fn tuple_variant<V>(self, len: usize, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        match self.value {
            Some(Value::Collection(Collection::Tuple(t))) if t.len() == len + 1 => visitor
                .visit_seq(ValueSeqDeserializer {
                    iterator: t.iter().skip(1),
                }),
            Some(v) => Err(SerializationError::Other(format!(
                "{:?} should be a tagged tuple with {} elements (including the tag)",
                v, len,
            ))),
            None => Err(SerializationError::Other(format!(
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
        match self.value {
            Some(v @ Value::Collection(Collection::Tuple(t))) if t.len() == 2 => {
                if let Value::Collection(Collection::Record(r)) = &t[1] {
                    let fields = fields
                        .into_iter()
                        .map(|field| match r.iter().find(|(k, _)| k == field) {
                            None => Err(SerializationError::Other(format!(
                                "Field {:?} is missing in {:?}",
                                field, self.value
                            ))),
                            Some((k, v)) => Ok((k, v)),
                        })
                        .collect::<Result<Vec<_>, _>>()?;

                    visitor.visit_map(ValueStructMapDeserializer {
                        trailing_value: None,
                        iterator: fields.into_iter(),
                    })
                } else {
                    Err(SerializationError::Other(format!(
                        "{:?} should be a tagged tuple with a record",
                        v,
                    )))
                }
            }
            Some(v) => Err(SerializationError::Other(format!(
                "{:?} should be a tagged tuple with a record",
                v,
            ))),
            None => Err(SerializationError::Other(format!(
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
    I: Iterator<Item = &'de Value>,
{
    type Error = SerializationError;

    fn next_element_seed<T>(&mut self, seed: T) -> Result<Option<T::Value>, Self::Error>
    where
        T: serde::de::DeserializeSeed<'de>,
    {
        if let Some(element) = self.iterator.next() {
            Ok(Some(seed.deserialize(ValueDeserializer::new(element))?))
        } else {
            Ok(None)
        }
    }
}

struct ValueMapDeserializer<'de, I> {
    iterator: I,
    trailing_value: Option<&'de Value>,
}

impl<'de, I> MapAccess<'de> for ValueMapDeserializer<'de, I>
where
    I: Iterator<Item = &'de (Value, Value)>,
{
    type Error = SerializationError;

    fn next_key_seed<K>(&mut self, seed: K) -> Result<Option<K::Value>, Self::Error>
    where
        K: serde::de::DeserializeSeed<'de>,
    {
        if let Some((key, value)) = self.iterator.next() {
            self.trailing_value = Some(value);
            Ok(Some(seed.deserialize(ValueDeserializer::new(key))?))
        } else {
            Ok(None)
        }
    }

    fn next_value_seed<V>(&mut self, seed: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::DeserializeSeed<'de>,
    {
        seed.deserialize(ValueDeserializer::new(
            self.trailing_value
                .take()
                .ok_or_else(|| SerializationError::Other("value without key".to_string()))?,
        ))
    }
}

struct ValueStructMapDeserializer<'de, I> {
    iterator: I,
    trailing_value: Option<&'de Value>,
}

impl<'de, I> MapAccess<'de> for ValueStructMapDeserializer<'de, I>
where
    I: Iterator<Item = (&'de Cow<'de, str>, &'de Value)>,
{
    type Error = SerializationError;

    fn next_key_seed<K>(&mut self, seed: K) -> Result<Option<K::Value>, Self::Error>
    where
        K: serde::de::DeserializeSeed<'de>,
    {
        if let Some((key, value)) = self.iterator.next() {
            self.trailing_value = Some(value);
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
        seed.deserialize(ValueDeserializer::new(
            self.trailing_value
                .take()
                .ok_or_else(|| SerializationError::Other("value without key".to_string()))?,
        ))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use serde::{Deserialize, Serialize};

    use crate::{Collection, Primitive, Value};

    fn test_value(input: Value) {
        let serialized = Value::serialize(&input).unwrap();
        assert_eq!(serialized, input);
        let output: Value = serialized.deserialize().unwrap();
        assert_eq!(output, input);
    }

    #[test]
    fn isometric_self_encoding_value() {
        test_value(Value::Primitive(Primitive::Nil));
        test_value(Value::Primitive(Primitive::Integer(123)));
        test_value(Value::Primitive(Primitive::Float(32.432)));
        test_value(Value::Primitive(Primitive::Boolean(true)));
        test_value(Value::Primitive(Primitive::string("owned")));
        test_value(Value::Primitive(Primitive::static_str("borrowed")));

        test_value(Value::Collection(Collection::list([
            Value::Primitive(Primitive::Integer(123)),
            Value::Primitive(Primitive::Integer(456)),
        ])));

        test_value(Value::Collection(Collection::dict([
            (
                Value::Primitive(Primitive::static_str("one")),
                Value::Primitive(Primitive::Integer(123)),
            ),
            (
                Value::Primitive(Primitive::static_str("two")),
                Value::Primitive(Primitive::static_str("hello")),
            ),
        ])));
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
        }

        #[derive(Serialize, Deserialize, Debug, PartialEq)]
        enum Y {
            One,
            Two(usize),
            Three(String, String),
            Four { a: usize, b: f64 },
        }

        #[derive(Serialize, Deserialize, Debug, PartialEq)]
        struct K;

        #[derive(Serialize, Deserialize, Debug, PartialEq)]
        struct L(String);

        #[derive(Serialize, Deserialize, Debug, PartialEq)]
        struct M(String, usize);

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
        };

        let serialized = Value::serialize(&input).unwrap();

        let output: X = serialized.deserialize().unwrap();

        assert_eq!(output, input);
    }
}
