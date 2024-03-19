use std::str::Utf8Error;

use cxx::{vector::VectorElement, CxxString, CxxVector};

use super::errors::CxxConvertError;

pub trait ConvertStrategy<T, U> {
    fn convert(&self, item: &T) -> Result<U, CxxConvertError>;
}

// u8->u8, u32->u32
pub struct CxxElementStrategy;

impl<T> ConvertStrategy<T, T> for CxxElementStrategy
where
    T: Clone,
{
    fn convert(&self, item: &T) -> Result<T, CxxConvertError> {
        Ok(item.clone())
    }
}

impl ConvertStrategy<CxxString, String> for CxxElementStrategy {
    fn convert(&self, item: &CxxString) -> Result<String, CxxConvertError> {
        let result: Result<String, Utf8Error> = item.to_str().map(|t| t.to_string());
        result.map_err(CxxConvertError::Utf8Error)
    }
}

// CxxVector -> Vec
pub struct CxxVectorStrategy<T>
where
    T: Clone + VectorElement,
{
    _marker: std::marker::PhantomData<T>,
}

impl<T> CxxVectorStrategy<T>
where
    T: Clone + VectorElement,
{
    pub fn new() -> Self {
        CxxVectorStrategy {
            _marker: std::marker::PhantomData,
        }
    }
}
impl<T> ConvertStrategy<CxxVector<T>, Vec<T>> for CxxVectorStrategy<T>
where
    T: Clone + VectorElement,
{
    fn convert(&self, items: &CxxVector<T>) -> Result<Vec<T>, CxxConvertError> {
        Ok(items.into_iter().map(|item| item.clone()).collect())
    }
}

// CxxVector<String> 转换为 Vec<String>
pub struct CxxVectorStringStrategy;

impl ConvertStrategy<CxxVector<CxxString>, Vec<String>> for CxxVectorStringStrategy {
    fn convert(&self, items: &CxxVector<CxxString>) -> Result<Vec<String>, CxxConvertError> {
        items
            .iter()
            .map(|item| {
                item.to_str()
                    .map(|t| t.to_string())
                    .map_err(CxxConvertError::Utf8Error)
            })
            .collect()
    }
}

pub struct Converter<T, U, S>
where
    S: ConvertStrategy<T, U>,
{
    strategy: S,
    _marker_t: std::marker::PhantomData<T>,
    _marker_u: std::marker::PhantomData<U>,
}

impl<T, U, S> Converter<T, U, S>
where
    S: ConvertStrategy<T, U>,
    // T: VectorElement,
{
    pub fn new(strategy: S) -> Self {
        Converter {
            strategy,
            _marker_t: std::marker::PhantomData,
            _marker_u: std::marker::PhantomData,
        }
    }

    pub fn convert(&self, item: &T) -> Result<U, CxxConvertError> {
        self.strategy.convert(item)
    }
}
