use crate::errors::*;
use crate::query::*;
use crate::record::ValueRef;
use flexbuffers::VectorReader;

pub(crate) fn create_filter(
    limit: Option<usize>,
    selection: Option<Selection>,
) -> ApiResult<Option<Box<dyn Filter>>> {
    let result: Box<dyn Filter> = match (limit, selection) {
        (Some(l), Some(s)) => Box::new(CompositeFilter::new(vec![
            Box::new(LimitFilter::new(l)),
            Box::new(SelectionFilter::new(s)),
        ])),
        (Some(l), None) => Box::new(LimitFilter::new(l)),
        (None, Some(s)) => Box::new(SelectionFilter::new(s)),
        _ => return Ok(None),
    };
    Ok(Some(result))
}

pub(crate) enum FilterRes {
    NeedStop,
    NeedPass,
    NeedProcess,
}

pub(crate) trait Filter {
    fn filter(&mut self, record: &flexbuffers::VectorReader) -> ApiResult<FilterRes>;
}

struct LimitFilter {
    limit: usize,
    cur_count: usize,
}

impl LimitFilter {
    fn new(limit: usize) -> Self {
        Self {
            limit,
            cur_count: 0,
        }
    }
}

impl Filter for LimitFilter {
    fn filter(&mut self, _: &VectorReader) -> ApiResult<FilterRes> {
        self.cur_count += 1;
        if self.cur_count > self.limit {
            return Ok(FilterRes::NeedStop);
        }
        Ok(FilterRes::NeedProcess)
    }
}

struct SelectionFilter {
    selection: Selection,
}

impl SelectionFilter {
    fn new(selection: Selection) -> Self {
        Self { selection }
    }
}

impl Filter for SelectionFilter {
    fn filter(&mut self, value_reader: &VectorReader) -> ApiResult<FilterRes> {
        let result = match &self.selection {
            Selection::BinaryOp(col_id, op, value) => {
                let reader = value_reader.index(*col_id)?;
                let rv = ValueRef::from_reader(&reader)?;
                let v = value.as_ord_ref();
                match op {
                    BinaryOpType::Eq => {
                        if rv.ord_ref().eq(&v) {
                            FilterRes::NeedProcess
                        } else {
                            FilterRes::NeedPass
                        }
                    }
                }
            }
        };
        Ok(result)
    }
}

struct CompositeFilter {
    inner: Vec<Box<dyn Filter>>,
}

impl CompositeFilter {
    fn new(inner: Vec<Box<dyn Filter>>) -> Self {
        Self { inner }
    }
}

impl Filter for CompositeFilter {
    fn filter(&mut self, value: &VectorReader) -> ApiResult<FilterRes> {
        for f in &mut self.inner {
            match f.filter(value)? {
                FilterRes::NeedStop => return Ok(FilterRes::NeedStop),
                FilterRes::NeedPass => return Ok(FilterRes::NeedPass),
                _ => (),
            }
        }
        Ok(FilterRes::NeedProcess)
    }
}
