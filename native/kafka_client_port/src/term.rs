//! Conversion between Erlang terms and the values used by the port programs.

use eetf::{Atom, BigInteger, Binary, FixInteger, Float, List, Map, Term, Tuple};
use std::collections::HashMap;

/// A decoded Erlang term. The rules are the same as in the Java port:
///
/// - `{:__binary__, binary}` is decoded as bytes, and `{:__binary__, nil}` as null
/// - other binaries are decoded as strings
/// - `true` and `false` are decoded as booleans, `nil` as null
#[derive(Clone, Debug, PartialEq)]
pub enum Value {
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    Atom(String),
    Str(String),
    Bytes(Vec<u8>),
    List(Vec<Value>),
    Tuple(Vec<Value>),
    Map(Vec<(Value, Value)>),
}

impl Value {
    pub fn decode(bytes: &[u8]) -> Self {
        Self::from_term(Term::decode(bytes).expect("invalid erlang term"))
    }

    fn from_term(term: Term) -> Self {
        match term {
            Term::Atom(atom) => match atom.name.as_str() {
                "true" => Value::Bool(true),
                "false" => Value::Bool(false),
                "nil" => Value::Null,
                _ => Value::Atom(atom.name),
            },
            Term::FixInteger(int) => Value::Int(int.value.into()),
            Term::BigInteger(int) => {
                Value::Int(int.to_string().parse().expect("integer out of range"))
            }
            Term::Float(float) => Value::Float(float.value),
            Term::Binary(binary) => Value::Str(String::from_utf8_lossy(&binary.bytes).into_owned()),
            // `term_to_binary` encodes a list of small integers as a byte list
            Term::ByteList(list) => Value::List(
                list.bytes
                    .into_iter()
                    .map(|b| Value::Int(b.into()))
                    .collect(),
            ),
            Term::List(list) => {
                Value::List(list.elements.into_iter().map(Self::from_term).collect())
            }
            Term::Tuple(tuple) => Self::from_tuple(tuple.elements),
            Term::Map(map) => Value::Map(
                map.map
                    .into_iter()
                    .map(|(key, value)| (Self::from_term(key), Self::from_term(value)))
                    .collect(),
            ),
            other => panic!("unsupported erlang term {other}"),
        }
    }

    fn from_tuple(mut elements: Vec<Term>) -> Self {
        let is_binary = matches!(
            elements.as_slice(),
            [Term::Atom(atom), _] if atom.name == "__binary__"
        );

        if !is_binary {
            return Value::Tuple(elements.into_iter().map(Self::from_term).collect());
        }

        match elements.pop() {
            Some(Term::Binary(binary)) => Value::Bytes(binary.bytes),
            Some(Term::Atom(atom)) if atom.name == "nil" => Value::Null,
            other => panic!("invalid binary wrapper {other:?}"),
        }
    }

    pub fn as_str(&self) -> &str {
        match self {
            Value::Str(value) | Value::Atom(value) => value,
            other => panic!("expected a string, got {other:?}"),
        }
    }

    pub fn as_i64(&self) -> i64 {
        match self {
            Value::Int(value) => *value,
            other => panic!("expected an integer, got {other:?}"),
        }
    }

    pub fn as_i32(&self) -> i32 {
        i32::try_from(self.as_i64()).expect("integer out of range")
    }

    pub fn as_list(&self) -> &[Value] {
        match self {
            Value::List(values) | Value::Tuple(values) => values,
            other => panic!("expected a list, got {other:?}"),
        }
    }

    pub fn as_map(&self) -> &[(Value, Value)] {
        match self {
            Value::Map(entries) => entries,
            other => panic!("expected a map, got {other:?}"),
        }
    }

    /// Looks up a map entry, treating atom and string keys the same, as the Java port does.
    pub fn get(&self, key: &str) -> &Value {
        self.as_map()
            .iter()
            .find(|(candidate, _)| {
                matches!(candidate, Value::Str(name) | Value::Atom(name) if name == key)
            })
            .map_or(&Value::Null, |(_, value)| value)
    }

    /// Moves a map entry out of the value, which avoids copying large payloads.
    pub fn take(&mut self, key: &str) -> Value {
        let Value::Map(entries) = self else {
            panic!("expected a map, got {self:?}");
        };

        entries
            .iter_mut()
            .find(|(candidate, _)| {
                matches!(candidate, Value::Str(name) | Value::Atom(name) if name == key)
            })
            .map_or(Value::Null, |(_, value)| std::mem::replace(value, Value::Null))
    }

    pub fn into_list(self) -> Vec<Value> {
        match self {
            Value::List(values) | Value::Tuple(values) => values,
            other => panic!("expected a list, got {other:?}"),
        }
    }

    pub fn into_bytes(self) -> Option<Vec<u8>> {
        match self {
            Value::Bytes(bytes) => Some(bytes),
            Value::Str(string) => Some(string.into_bytes()),
            Value::Null => None,
            other => panic!("expected bytes, got {other:?}"),
        }
    }

    pub fn opt_i32(&self) -> Option<i32> {
        (*self != Value::Null).then(|| self.as_i32())
    }

    pub fn opt_i64(&self) -> Option<i64> {
        (*self != Value::Null).then(|| self.as_i64())
    }

    /// Converts a map of client parameters to the string map required by the Rust client. Null
    /// values are dropped, as they are in the Java port.
    pub fn to_config(&self) -> HashMap<String, String> {
        self.as_map()
            .iter()
            .filter_map(|(key, value)| {
                let value = match value {
                    Value::Null => return None,
                    Value::Bool(value) => value.to_string(),
                    Value::Int(value) => value.to_string(),
                    Value::Float(value) => value.to_string(),
                    Value::Str(value) | Value::Atom(value) => value.clone(),
                    other => panic!("unsupported config value {other:?}"),
                };

                Some((key.as_str().to_string(), value))
            })
            .collect()
    }
}

pub fn atom(name: &str) -> Term {
    Atom::from(name).into()
}

pub fn nil() -> Term {
    atom("nil")
}

pub fn boolean(value: bool) -> Term {
    atom(if value { "true" } else { "false" })
}

pub fn string(value: &str) -> Term {
    bytes(value.as_bytes().to_vec())
}

pub fn bytes(value: Vec<u8>) -> Term {
    Binary::from(value).into()
}

pub fn integer(value: i64) -> Term {
    match i32::try_from(value) {
        Ok(value) => FixInteger::from(value).into(),
        Err(_) => BigInteger::from(value).into(),
    }
}

pub fn float(value: f64) -> Term {
    // only NaN and infinite values are rejected, and these are reported as nil
    Float::try_from(value).map_or_else(|_| nil(), Into::into)
}

pub fn tuple(elements: Vec<Term>) -> Term {
    Tuple::from(elements).into()
}

pub fn list(elements: Vec<Term>) -> Term {
    List::from(elements).into()
}

pub fn map(entries: Vec<(Term, Term)>) -> Term {
    Map::from(entries.into_iter().collect::<HashMap<_, _>>()).into()
}

pub fn ok(value: Term) -> Term {
    tuple(vec![atom("ok"), value])
}

pub fn error(message: &str) -> Term {
    tuple(vec![atom("error"), string(message)])
}

pub fn topic_partition(topic: &str, partition: i32) -> Term {
    tuple(vec![string(topic), integer(partition.into())])
}
