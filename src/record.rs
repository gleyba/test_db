use crate::errors::*;

use flexbuffers::{FlexBufferType, Reader};
use json::number::Number;
use std::cmp::Ordering;
use std::fmt::{self, Debug, Formatter, Write};
use std::hash::Hash;
use std::marker::PhantomData;

pub(crate) type RecordIteratorItem<'a, 'de> = &'a (dyn RecordRef<'de> + 'a);

pub(crate) trait RecordIterator<'de> {
    fn next(&mut self) -> ApiResult<Option<RecordIteratorItem<'_, 'de>>>;
}

pub(crate) type BoxedRecordIterator<'a, 'de> = Box<dyn RecordIterator<'de> + 'a>;

pub struct RecordIterWrapper<'a, 'de: 'a> {
    inner: BoxedRecordIterator<'a, 'de>,
}

impl<'a, 'de: 'a> RecordIterWrapper<'a, 'de> {
    pub(crate) fn new(inner: BoxedRecordIterator<'a, 'de>) -> Self {
        Self { inner }
    }
    pub fn next(&mut self) -> ApiResult<Option<RecordIteratorItem<'_, 'de>>> {
        self.inner.next()
    }
}

#[derive(Debug, Clone)]
pub enum ValueRef<'a> {
    UInteger(u64),
    Integer(i64),
    Float(f64),
    Str(&'a str),
    Null,
}

impl<'a> ValueRef<'a> {
    pub(crate) fn from_reader(reader: &Reader<'a>) -> ApiResult<Self> {
        let result = match reader.flexbuffer_type() {
            FlexBufferType::String => Self::Str(reader.as_str()),
            FlexBufferType::UInt => Self::UInteger(reader.as_u64()),
            FlexBufferType::Int => Self::Integer(reader.as_i64()),
            FlexBufferType::Float => Self::Float(reader.as_f64()),
            FlexBufferType::Null => Self::Null,
            _ => {
                return invalid_data_ae!(
                    "from_reader: unknown flexbuf type: {:?}",
                    reader.flexbuffer_type()
                );
            }
        };
        Ok(result)
    }

    pub fn is_null(&self) -> bool {
        match self {
            Self::Null => true,
            _ => false,
        }
    }

    pub fn as_str(&self) -> ApiResult<&str> {
        match *self {
            Self::Str(s) => Ok(s),
            _ => invalid_data_ae!("not str"),
        }
    }

    pub fn as_int(&self) -> ApiResult<i64> {
        match *self {
            Self::Integer(v) => Ok(v),
            Self::UInteger(v) => Ok(v as i64),
            Self::Null => Ok(0),
            _ => invalid_data_ae!("not uint"),
        }
    }

    pub fn as_uint(&self) -> ApiResult<u64> {
        match *self {
            Self::Integer(v) => Ok(v as u64),
            Self::UInteger(v) => Ok(v),
            Self::Null => Ok(0),
            _ => invalid_data_ae!("not uint"),
        }
    }

    pub(crate) fn ord_ref(self) -> ValueOrdRef<'a> {
        match self {
            Self::UInteger(x) => ValueOrdRef::Number(json::number::Number::from(x)),
            Self::Integer(x) => ValueOrdRef::Number(json::number::Number::from(x)),
            Self::Float(x) => ValueOrdRef::Number(json::number::Number::from(x)),
            Self::Str(x) => ValueOrdRef::Str(x),
            Self::Null => ValueOrdRef::Null,
        }
    }
}

impl<'a> fmt::Display for ValueRef<'a> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::UInteger(x) => write!(f, "{}", x),
            Self::Integer(x) => write!(f, "{}", x),
            Self::Float(x) => write!(f, "{}", x),
            Self::Str(x) => write!(f, "{}", x),
            Self::Null => fmt::Result::Ok(()),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum ValueOrdRef<'a> {
    Number(json::number::Number),
    Str(&'a str),
    Null,
}

impl<'a> ValueOrdRef<'a> {
    pub(crate) fn as_value_ref(&self) -> ValueRef<'a> {
        match self {
            Self::Number(n) => {
                let (c, m, e) = n.as_parts();
                if e == 0 {
                    return if c {
                        ValueRef::UInteger(m)
                    } else {
                        ValueRef::Integer(-(m as i64))
                    };
                }
                ValueRef::Float(f64::from(n.clone()))
            }
            Self::Str(x) => ValueRef::Str(x),
            Self::Null => ValueRef::Null,
        }
    }
}

impl Hash for ValueOrdRef<'_> {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        match self {
            Self::Null => "".hash(state),
            Self::Str(s) => s.hash(state),
            Self::Number(n) => {
                let (c, m, e) = n.as_parts();
                c.hash(state);
                m.hash(state);
                e.hash(state);
            }
        }
    }
}

impl Eq for ValueOrdRef<'_> {}

impl PartialOrd for ValueOrdRef<'_> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for ValueOrdRef<'_> {
    fn cmp(&self, other: &Self) -> Ordering {
        match (self, other) {
            (Self::Null, Self::Null) => Ordering::Equal,
            (_, Self::Null) => Ordering::Greater,
            (Self::Null, _) => Ordering::Less,
            (Self::Str(s1), Self::Str(s2)) => s1.cmp(s2),
            (Self::Number(n1), Self::Number(n2)) => ord_number(n1, n2),
            (Self::Str(s1), Self::Number(n2)) => ord_str_number(s1, n2),
            (Self::Number(n1), Self::Str(s2)) => ord_str_number(s2, n1).reverse(),
        }
    }
}

#[inline]
fn ord_number(n1: &Number, n2: &Number) -> Ordering {
    if n1.is_zero() && n2.is_zero() || n1.is_nan() && n2.is_nan() {
        return Ordering::Equal;
    }

    let (c1, m1, e1) = n1.as_parts();
    let (c2, m2, e2) = n2.as_parts();

    match (c1, c2) {
        (true, false) => return Ordering::Greater,
        (false, true) => return Ordering::Less,
        _ => (),
    }

    let e_diff = e1 - e2;
    if e_diff == 0 {
        m1.cmp(&m2)
    } else if e_diff > 0 {
        let power = decimal_power(e_diff as u16);
        m1.wrapping_mul(power).cmp(&m2)
    } else {
        let power = decimal_power(-e_diff as u16);
        let wr_mul = m2.wrapping_mul(power);
        m1.cmp(&wr_mul)
    }
}

#[inline]
fn decimal_power(mut e: u16) -> u64 {
    static CACHED: [u64; 20] = [
        1,
        10,
        100,
        1000,
        10000,
        100000,
        1000000,
        10000000,
        100000000,
        1000000000,
        10000000000,
        100000000000,
        1000000000000,
        10000000000000,
        100000000000000,
        1000000000000000,
        10000000000000000,
        100000000000000000,
        1000000000000000000,
        10000000000000000000,
    ];

    if e < 20 {
        CACHED[e as usize]
    } else {
        let mut pow = 1u64;
        while e >= 20 {
            pow = pow.saturating_mul(CACHED[(e % 20) as usize]);
            e /= 20;
        }

        pow
    }
}

fn ord_str_number(_: &str, _: &Number) -> Ordering {
    Ordering::Greater
}

impl<'a> fmt::Display for ValueOrdRef<'a> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::Number(x) => write!(f, "{}", x),
            Self::Str(x) => write!(f, "{}", x),
            Self::Null => fmt::Result::Ok(()),
        }
    }
}

pub trait RecordRef<'a> {
    fn len(&self) -> usize;
    fn value_at(&self, idx: usize) -> ApiResult<ValueRef<'a>>;
}

impl<'a> RecordRef<'a> for ValueRef<'a> {
    fn len(&self) -> usize {
        1
    }

    fn value_at(&self, idx: usize) -> ApiResult<ValueRef<'a>> {
        if idx != 0 {
            return invalid_data_ae!("out of bounds");
        }
        Ok(self.clone())
    }
}

impl Debug for &dyn RecordRef<'_> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.to_csv().unwrap_or_default())
    }
}

pub trait ToCSV {
    fn to_csv(&self) -> ApiResult<String>;
}

impl ToCSV for &dyn RecordRef<'_> {
    fn to_csv(&self) -> ApiResult<String> {
        let mut result = String::new();
        for idx in 0..self.len() {
            if idx == self.len() - 1 {
                write!(&mut result, "{}", self.value_at(idx)?)?;
            } else {
                write!(&mut result, "{},", self.value_at(idx)?)?;
            }
        }
        Ok(result)
    }
}

pub(crate) struct OneRecordIterator<'a, T: RecordRef<'a>> {
    record: T,
    is_first_call: bool,
    _phantom: PhantomData<&'a ()>,
}

impl<'a, T: RecordRef<'a>> OneRecordIterator<'a, T> {
    pub(crate) fn new(record: T) -> Self {
        Self {
            record,
            is_first_call: true,
            _phantom: PhantomData,
        }
    }
}

impl<'a, T: RecordRef<'a>> RecordIterator<'a> for OneRecordIterator<'a, T> {
    fn next(&mut self) -> ApiResult<Option<RecordIteratorItem<'_, 'a>>> {
        if !self.is_first_call {
            return Ok(None);
        }
        self.is_first_call = false;
        Ok(Some(&self.record))
    }
}

#[test]
fn test_value_ord_ref() {
    let values = vec![
        ValueRef::Integer(-1),
        ValueRef::Float(-1.5),
        ValueRef::UInteger(1),
        ValueRef::Integer(1),
        ValueRef::Float(1.45),
        ValueRef::Float(1.5),
        ValueRef::Float(1.65),
        ValueRef::Integer(-2),
        ValueRef::Float(-2.5),
        ValueRef::UInteger(2),
        ValueRef::Integer(2),
        ValueRef::Float(2.5),
        ValueRef::Integer(-100500),
        ValueRef::UInteger(100500),
        ValueRef::Integer(100500),
        ValueRef::Integer(-3),
        ValueRef::UInteger(3),
        ValueRef::Integer(3),
        ValueRef::Integer(0),
    ];

    let mut set = std::collections::BTreeSet::new();
    for v in values.iter() {
        set.insert(v.clone().ord_ref());
    }
    for v in set.iter() {
        println!("{:?} - {}", v, v);
    }
}

#[inline]
pub(crate) fn parse_number(num_str: &str) -> ApiResult<Number> {
    let res = if let Ok(num) = num_str.parse::<u64>() {
        Number::from(num)
    } else if let Ok(num) = num_str.parse::<i64>() {
        Number::from(num)
    } else if let Ok(num) = num_str.parse::<f64>() {
        Number::from(num)
    } else {
        return invalid_data_ae!("can't parse number from str: {}", num_str);
    };
    Ok(res)
}
