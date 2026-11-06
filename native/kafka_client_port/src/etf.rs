//! Direct encoder of the Erlang external term format, used on the hot path of the consumer.
//!
//! `eetf` needs an allocated tree of terms, including a copy of each payload. Records are instead
//! written straight into the output buffer, which leaves a single copy of the payload.

const VERSION: u8 = 131;
const SMALL_INTEGER: u8 = 97;
const INTEGER: u8 = 98;
const SMALL_TUPLE: u8 = 104;
const NIL: u8 = 106;
const LIST: u8 = 108;
const BINARY: u8 = 109;
const SMALL_BIG: u8 = 110;
const SMALL_ATOM_UTF8: u8 = 119;

const LENGTH_SIZE: usize = 4;

/// Appends a message, consisting of a four byte length, followed by the term.
pub fn frame(buffer: &mut Vec<u8>, encode: impl FnOnce(&mut Vec<u8>)) {
    let start = buffer.len();
    buffer.extend_from_slice(&[0; LENGTH_SIZE]);
    buffer.push(VERSION);
    encode(buffer);

    let length = u32::try_from(buffer.len() - start - LENGTH_SIZE).expect("message too large");
    buffer[start..start + LENGTH_SIZE].copy_from_slice(&length.to_be_bytes());
}

pub fn tuple_header(buffer: &mut Vec<u8>, arity: u8) {
    buffer.extend_from_slice(&[SMALL_TUPLE, arity]);
}

pub fn list_header(buffer: &mut Vec<u8>, length: usize) {
    let length = u32::try_from(length).expect("list too large");
    buffer.push(LIST);
    buffer.extend_from_slice(&length.to_be_bytes());
}

pub fn nil_list(buffer: &mut Vec<u8>) {
    buffer.push(NIL);
}

pub fn atom(buffer: &mut Vec<u8>, name: &str) {
    let length = u8::try_from(name.len()).expect("atom too long");
    buffer.extend_from_slice(&[SMALL_ATOM_UTF8, length]);
    buffer.extend_from_slice(name.as_bytes());
}

pub fn binary(buffer: &mut Vec<u8>, value: &[u8]) {
    let length = u32::try_from(value.len()).expect("binary too large");
    buffer.push(BINARY);
    buffer.extend_from_slice(&length.to_be_bytes());
    buffer.extend_from_slice(value);
}

pub fn optional_binary(buffer: &mut Vec<u8>, value: Option<&[u8]>) {
    match value {
        Some(value) => binary(buffer, value),
        None => atom(buffer, "nil"),
    }
}

pub fn integer(buffer: &mut Vec<u8>, value: i64) {
    if let Ok(value) = u8::try_from(value) {
        buffer.extend_from_slice(&[SMALL_INTEGER, value]);
    } else if let Ok(value) = i32::try_from(value) {
        buffer.push(INTEGER);
        buffer.extend_from_slice(&value.to_be_bytes());
    } else {
        let magnitude = value.unsigned_abs().to_le_bytes();
        let length = magnitude
            .iter()
            .rposition(|byte| *byte != 0)
            .map_or(0, |index| index + 1);
        let length_byte = u8::try_from(length).expect("magnitude has at most eight bytes");

        buffer.extend_from_slice(&[SMALL_BIG, length_byte, u8::from(value < 0)]);
        buffer.extend_from_slice(&magnitude[..length]);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::term;
    use eetf::Term;

    fn encoded(encode: impl FnOnce(&mut Vec<u8>)) -> Term {
        let mut buffer = Vec::new();
        frame(&mut buffer, encode);

        let length = u32::from_be_bytes(buffer[..LENGTH_SIZE].try_into().unwrap()) as usize;
        assert_eq!(length, buffer.len() - LENGTH_SIZE);
        Term::decode(&buffer[LENGTH_SIZE..]).unwrap()
    }

    #[test]
    fn integers_match_eetf() {
        for value in [
            0,
            1,
            255,
            256,
            -1,
            i64::from(i32::MAX),
            i64::from(i32::MIN),
            i64::from(i32::MAX) + 1,
            i64::from(i32::MIN) - 1,
            1_793_926_215_920,
            -1_793_926_215_920,
            i64::MAX,
            i64::MIN,
        ] {
            assert_eq!(
                encoded(|buffer| integer(buffer, value)),
                normalize(term::integer(value))
            );
        }
    }

    // `eetf` keeps small values in a distinct type when decoding, so both sides pass through it.
    fn normalize(value: Term) -> Term {
        let mut bytes = Vec::new();
        value.encode(&mut bytes).unwrap();
        Term::decode(&bytes[..]).unwrap()
    }

    #[test]
    fn nested_terms_match_eetf() {
        let actual = encoded(|buffer| {
            tuple_header(buffer, 4);
            atom(buffer, "record");
            binary(buffer, b"payload");
            list_header(buffer, 1);
            tuple_header(buffer, 2);
            binary(buffer, b"key");
            optional_binary(buffer, None);
            nil_list(buffer);
            nil_list(buffer);
        });

        let expected = term::tuple(vec![
            term::atom("record"),
            term::string("payload"),
            term::list(vec![term::tuple(vec![term::string("key"), term::nil()])]),
            term::list(vec![]),
        ]);

        assert_eq!(actual, normalize(expected));
    }
}
