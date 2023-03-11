use std::borrow::Cow;
use std::fmt;

use serde::de::SeqAccess;
use serde::ser::{Impossible, SerializeSeq, SerializeStruct};
use serde::{Deserializer, Serializer};

use crate::{Collection, Primitive, Value};

pub struct ValueSerializer;

#[derive(Debug)]
pub enum SerializationError {
    Other(String),
    StringToChar,
    MissingField,
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

#[allow(unused_variables)]
impl Serializer for ValueSerializer {
    type Ok = Value;

    type Error = SerializationError;

    type SerializeSeq = ListValueSerializer;

    type SerializeTuple = Impossible<Value, SerializationError>;

    type SerializeTupleStruct = Impossible<Value, SerializationError>;

    type SerializeTupleVariant = Impossible<Value, SerializationError>;

    type SerializeMap = Impossible<Value, SerializationError>;

    type SerializeStruct = RecordValueSerializer;

    type SerializeStructVariant = Impossible<Value, SerializationError>;

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

    fn serialize_f32(self, v: f32) -> Result<Self::Ok, Self::Error> {
        Ok(Value::Primitive(Primitive::f32(v)))
    }

    fn serialize_f64(self, v: f64) -> Result<Self::Ok, Self::Error> {
        Ok(Value::Primitive(Primitive::f64(v)))
    }

    fn serialize_char(self, v: char) -> Result<Self::Ok, Self::Error> {
        Ok(Value::Primitive(Primitive::string(v.to_string())))
    }

    fn serialize_str(self, v: &str) -> Result<Self::Ok, Self::Error> {
        Ok(Value::Primitive(Primitive::string(v.to_string())))
    }

    fn serialize_bytes(self, v: &[u8]) -> Result<Self::Ok, Self::Error> {
        Ok(Value::Collection(Collection::list(
            v.into_iter().copied().map(Into::into).map(Value::Primitive),
        )))
    }

    fn serialize_none(self) -> Result<Self::Ok, Self::Error> {
        Ok(Value::Primitive(Primitive::Nil))
    }

    fn serialize_some<T: ?Sized>(self, value: &T) -> Result<Self::Ok, Self::Error>
    where
        T: serde::Serialize,
    {
        value.serialize(self)
    }

    fn serialize_unit(self) -> Result<Self::Ok, Self::Error> {
        Ok(Value::Primitive(Primitive::Nil))
    }

    fn serialize_unit_struct(self, name: &'static str) -> Result<Self::Ok, Self::Error> {
        Ok(Value::Primitive(Primitive::Nil))
    }

    fn serialize_unit_variant(
        self,
        name: &'static str,
        variant_index: u32,
        variant: &'static str,
    ) -> Result<Self::Ok, Self::Error> {
        self.serialize_u32(variant_index)
    }

    fn serialize_newtype_struct<T: ?Sized>(
        self,
        name: &'static str,
        value: &T,
    ) -> Result<Self::Ok, Self::Error>
    where
        T: serde::Serialize,
    {
        value.serialize(self)
    }

    fn serialize_newtype_variant<T: ?Sized>(
        self,
        name: &'static str,
        variant_index: u32,
        variant: &'static str,
        value: &T,
    ) -> Result<Self::Ok, Self::Error>
    where
        T: serde::Serialize,
    {
        self.serialize_u32(variant_index)
    }

    fn serialize_seq(self, len: Option<usize>) -> Result<Self::SerializeSeq, Self::Error> {
        Ok(ListValueSerializer::default())
    }

    fn serialize_tuple(self, len: usize) -> Result<Self::SerializeTuple, Self::Error> {
        todo!()
    }

    fn serialize_tuple_struct(
        self,
        name: &'static str,
        len: usize,
    ) -> Result<Self::SerializeTupleStruct, Self::Error> {
        todo!()
    }

    fn serialize_tuple_variant(
        self,
        name: &'static str,
        variant_index: u32,
        variant: &'static str,
        len: usize,
    ) -> Result<Self::SerializeTupleVariant, Self::Error> {
        todo!()
    }

    fn serialize_map(self, len: Option<usize>) -> Result<Self::SerializeMap, Self::Error> {
        todo!()
    }

    fn serialize_struct(
        self,
        name: &'static str,
        len: usize,
    ) -> Result<Self::SerializeStruct, Self::Error> {
        Ok(RecordValueSerializer::default())
    }

    fn serialize_struct_variant(
        self,
        name: &'static str,
        variant_index: u32,
        variant: &'static str,
        len: usize,
    ) -> Result<Self::SerializeStructVariant, Self::Error> {
        todo!()
    }
}

#[derive(Default)]
pub struct ListValueSerializer {
    elements: Vec<Value>,
}

impl SerializeSeq for ListValueSerializer {
    type Ok = Value;

    type Error = SerializationError;

    fn serialize_element<T: ?Sized>(&mut self, value: &T) -> Result<(), Self::Error>
    where
        T: serde::Serialize,
    {
        self.elements.push(value.serialize(ValueSerializer)?);
        Ok(())
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        Ok(Value::Collection(Collection::List(self.elements)))
    }
}

#[derive(Default)]
pub struct RecordValueSerializer {
    elements: Vec<(Cow<'static, str>, Value)>,
}

impl SerializeStruct for RecordValueSerializer {
    type Ok = Value;

    type Error = SerializationError;

    fn serialize_field<T: ?Sized>(
        &mut self,
        key: &'static str,
        value: &T,
    ) -> Result<(), Self::Error>
    where
        T: serde::Serialize,
    {
        self.elements
            .push((Cow::Borrowed(key), value.serialize(ValueSerializer)?));
        Ok(())
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        Ok(Value::Collection(Collection::Record(self.elements)))
    }
}

pub struct ValueDeserializer<'a> {
    value: &'a Value,
}

impl<'a> ValueDeserializer<'a> {
    pub fn new(value: &'a Value) -> Self {
        Self { value }
    }
}

#[allow(unused_variables)]
impl<'de> Deserializer<'de> for ValueDeserializer<'de> {
    type Error = SerializationError;

    fn deserialize_any<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        match self.value {
            Value::Primitive(Primitive::Nil) => visitor.visit_unit(),
            Value::Primitive(Primitive::String(s)) => visitor.visit_str(s.as_ref()),
            Value::Primitive(Primitive::Integer(i)) => visitor.visit_i128(*i),
            Value::Primitive(Primitive::Float(i)) => visitor.visit_f64(*i),
            Value::Primitive(Primitive::Boolean(b)) => visitor.visit_bool(*b),

            Value::Collection(Collection::List(l)) => {
                visitor.visit_seq(ListValueDeserializer { elements: l.iter() })
            }

            Value::Collection(Collection::Tuple(t)) => {
                todo!()
            }

            Value::Collection(Collection::Record(_)) => visitor.visit_newtype_struct(self),
        }
    }

    fn deserialize_bool<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        if let Value::Primitive(Primitive::Boolean(b)) = self.value {
            visitor.visit_bool(*b)
        } else {
            Err(SerializationError::Other(format!(
                "Expected boolean, got {:?}",
                self.value
            )))
        }
    }

    fn deserialize_i8<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        if let Value::Primitive(Primitive::Integer(i)) = self.value {
            visitor.visit_i8(*i as _)
        } else {
            Err(SerializationError::Other(format!(
                "Expected integer, got {:?}",
                self.value
            )))
        }
    }

    fn deserialize_i16<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        if let Value::Primitive(Primitive::Integer(i)) = self.value {
            visitor.visit_i16(*i as _)
        } else {
            Err(SerializationError::Other(format!(
                "Expected integer, got {:?}",
                self.value
            )))
        }
    }

    fn deserialize_i32<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        if let Value::Primitive(Primitive::Integer(i)) = self.value {
            visitor.visit_i32(*i as _)
        } else {
            Err(SerializationError::Other(format!(
                "Expected integer, got {:?}",
                self.value
            )))
        }
    }

    fn deserialize_i64<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        if let Value::Primitive(Primitive::Integer(i)) = self.value {
            visitor.visit_i64(*i as _)
        } else {
            Err(SerializationError::Other(format!(
                "Expected integer, got {:?}",
                self.value
            )))
        }
    }

    fn deserialize_u8<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        if let Value::Primitive(Primitive::Integer(i)) = self.value {
            visitor.visit_u8(*i as _)
        } else {
            Err(SerializationError::Other(format!(
                "Expected integer, got {:?}",
                self.value
            )))
        }
    }

    fn deserialize_u16<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        if let Value::Primitive(Primitive::Integer(i)) = self.value {
            visitor.visit_u16(*i as _)
        } else {
            Err(SerializationError::Other(format!(
                "Expected integer, got {:?}",
                self.value
            )))
        }
    }

    fn deserialize_u32<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        if let Value::Primitive(Primitive::Integer(i)) = self.value {
            visitor.visit_u32(*i as _)
        } else {
            Err(SerializationError::Other(format!(
                "Expected integer, got {:?}",
                self.value
            )))
        }
    }

    fn deserialize_u64<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        if let Value::Primitive(Primitive::Integer(i)) = self.value {
            visitor.visit_u64(*i as _)
        } else {
            Err(SerializationError::Other(format!(
                "Expected integer, got {:?}",
                self.value
            )))
        }
    }

    fn deserialize_f32<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        if let Value::Primitive(Primitive::Float(f)) = self.value {
            visitor.visit_f32(*f as _)
        } else {
            Err(SerializationError::Other(format!(
                "Expected float, got {:?}",
                self.value
            )))
        }
    }

    fn deserialize_f64<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        if let Value::Primitive(Primitive::Float(f)) = self.value {
            visitor.visit_f64(*f)
        } else {
            Err(SerializationError::Other(format!(
                "Expected float, got {:?}",
                self.value
            )))
        }
    }

    fn deserialize_char<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        if let Value::Primitive(Primitive::String(s)) = self.value {
            if s.len() > 1 {
                Err(SerializationError::StringToChar)
            } else {
                visitor.visit_char(s.chars().next().unwrap_or('\0'))
            }
        } else {
            Err(SerializationError::Other(format!(
                "Expected single character string, got {:?}",
                self.value
            )))
        }
    }

    fn deserialize_str<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        if let Value::Primitive(Primitive::String(s)) = self.value {
            visitor.visit_borrowed_str(s.as_ref())
        } else {
            Err(SerializationError::Other(format!(
                "Expected string, got {:?}",
                self.value
            )))
        }
    }

    fn deserialize_string<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        if let Value::Primitive(Primitive::String(s)) = self.value {
            visitor.visit_string(s.to_string())
        } else {
            Err(SerializationError::Other(format!(
                "Expected string, got {:?}",
                self.value
            )))
        }
    }

    fn deserialize_bytes<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_byte_buf<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_option<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_unit<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_unit_struct<V>(
        self,
        _name: &'static str,
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        visitor.visit_unit()
    }

    fn deserialize_newtype_struct<V>(
        self,
        name: &'static str,
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_seq<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        if let Value::Collection(Collection::List(l)) = self.value {
            visitor.visit_seq(ListValueDeserializer { elements: l.iter() })
        } else {
            Err(SerializationError::Other(format!(
                "Expected list, got {:?}",
                self.value
            )))
        }
    }

    fn deserialize_tuple<V>(self, len: usize, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_tuple_struct<V>(
        self,
        name: &'static str,
        len: usize,
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_map<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_struct<V>(
        self,
        _name: &'static str,
        fields: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        if let Value::Collection(Collection::Record(r)) = self.value {
            let mut values = vec![];
            'fields: for field in fields {
                for (k, v) in r {
                    if k == field {
                        values.push(v);
                        continue 'fields;
                    }
                }
                return Err(SerializationError::MissingField);
            }

            visitor.visit_seq(ListValueDeserializer {
                elements: values.into_iter(),
            })
        } else {
            Err(SerializationError::Other(format!(
                "Expected record, got {:?}",
                self.value
            )))
        }
    }

    fn deserialize_enum<V>(
        self,
        name: &'static str,
        variants: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_identifier<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_ignored_any<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        todo!()
    }
}

struct ListValueDeserializer<I> {
    elements: I,
}

impl<'de, I: Iterator<Item = &'de Value>> SeqAccess<'de> for ListValueDeserializer<I> {
    type Error = SerializationError;

    fn next_element_seed<T>(&mut self, seed: T) -> Result<Option<T::Value>, Self::Error>
    where
        T: serde::de::DeserializeSeed<'de>,
    {
        if let Some(value) = self.elements.next() {
            Ok(Some(seed.deserialize(ValueDeserializer::new(value))?))
        } else {
            Ok(None)
        }
    }
}

#[cfg(test)]
mod tests {
    use serde::{Deserialize, Serialize};

    use crate::Value;

    #[test]
    fn borrowed_string() {
        let value = Value::serialize(&"test").unwrap();
        let s: &str = value.deserialize().unwrap();
        assert_eq!(s, "test");
    }

    #[test]
    fn vec() {
        let value = Value::serialize(&vec![1, 2, 3]).unwrap();
        let s: Vec<u16> = value.deserialize().unwrap();
        assert_eq!(s, [1, 2, 3]);
    }

    #[test]
    fn unit_struct() {
        #[derive(Debug, PartialEq, Serialize, Deserialize)]
        struct X;
        let value = Value::serialize(&X).unwrap();
        let x: X = value.deserialize().unwrap();
        assert_eq!(x, X);
    }

    #[test]
    fn single_field_struct() {
        #[derive(Debug, PartialEq, Serialize, Deserialize)]
        struct X {
            x: i64,
        }
        let input = X { x: 12354 };
        let value = Value::serialize(&input).unwrap();
        let output: X = value.deserialize().unwrap();
        assert_eq!(output, input);
    }

    #[test]
    fn multi_field_struct() {
        #[derive(Debug, PartialEq, Serialize, Deserialize)]
        struct X {
            x: i64,
            y: String,
        }
        let input = X { x: 12354, y: "hello".into() };
        let value = Value::serialize(&input).unwrap();
        let output: X = value.deserialize().unwrap();
        assert_eq!(output, input);
    }
}
