use crate::errors::*;
use crate::query::*;
use crate::record::*;

use flexbuffers::{Reader, VectorReader};
use indexmap::{IndexMap, IndexSet};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::marker::PhantomData;

pub(crate) fn create_aggregator<'ret, 'de: 'ret>(
    headers: &Vec<String>,
    projections: Vec<Projection>,
    group_by: Option<GroupBy>,
    order_by: Option<OrderBy>,
) -> ApiResult<Box<dyn Aggregator<'de> + 'ret>> {
    let result = base_aggregator(headers, &projections, group_by)
        .map(|a| apply_order_by(a, headers, &projections, order_by))?;

    Ok(result?)
}

type ColumnSet = IndexSet<usize>;

fn base_aggregator<'de>(
    headers: &Vec<String>,
    projections: &Vec<Projection>,
    group_by: Option<GroupBy>,
) -> ApiResult<AnyBaseAggregator<'de>> {
    if projections.is_empty() {
        return invalid_data_ae!("empty projections");
    }

    let mut count = None;
    let mut columns_set: ColumnSet = Default::default();

    validate_projections(&headers, projections, &mut count, &mut columns_set)?;

    if let Some(group_by) = group_by {
        return group_by_aggregator(group_by, projections, headers);
    }

    if count.is_some() {
        return count_aggregator(columns_set);
    }

    Ok(AnyBaseAggregator::Columns(ColumnsAggregator::new(
        columns_set,
    )))
}

fn validate_projections(
    headers: &Vec<String>,
    projections: &Vec<Projection>,
    has_count: &mut Option<usize>,
    columns_set: &mut ColumnSet,
) -> ApiResult<()> {
    for (idx, p) in projections.iter().enumerate() {
        match p.ptype {
            ProjectionType::Count => {
                if has_count.is_some() {
                    return invalid_data_ae!("duplicate count()");
                }
                *has_count = Some(idx);
            }
            ProjectionType::Column(column_idx) => {
                if headers.len() - 1 < column_idx {
                    return invalid_data_ae!(
                        "column idx {} projection while there are only {} columns",
                        column_idx,
                        headers.len()
                    );
                }

                if !columns_set.insert(column_idx) {
                    return invalid_data_ae!("duplicate column \"{}\" projection", headers[idx]);
                }
            }
        }
    }
    Ok(())
}

fn group_by_aggregator<'de>(
    group_by: GroupBy,
    projections: &Vec<Projection>,
    headers: &Vec<String>,
) -> ApiResult<AnyBaseAggregator<'de>> {
    let (column_id, signature) = match group_by {
        GroupBy::ProjectionId(pid) => validate_group_by_projection(pid, projections)?,
        GroupBy::Column(c) => validate_group_by_column(c, projections, headers)?,
    };

    Ok(AnyBaseAggregator::GroupBy(GroupByAggregator::new(
        column_id, signature,
    )))
}

fn validate_group_by_column(
    column: String,
    projections: &Vec<Projection>,
    headers: &Vec<String>,
) -> ApiResult<(usize, Vec<GroupBySigType>)> {
    let mut signature = Vec::new();
    let mut column_id = None;

    for p in projections.iter() {
        match p.ptype {
            ProjectionType::Column(c) => {
                if p.name != column {
                    if c > headers.len() - 1 {
                        return invalid_data_ae!("out of bounds");
                    }
                    let h = &headers[c];
                    if !column.eq(h) {
                        return invalid_data_ae!(
                            "can't combine column projection {} with group by {}",
                            h,
                            column
                        );
                    }
                }

                column_id = Some(c);
                signature.push(GroupBySigType::GroupByProjection);
            }
            ProjectionType::Count => {
                signature.push(GroupBySigType::Fun(FunctionAggregatorType::Count))
            }
        }
    }

    guard!(let Some(column_id) = column_id else {
        return invalid_data_ae!("Column for group_by not detected");
    });

    Ok((column_id, signature))
}

fn validate_group_by_projection(
    projection_id: usize,
    projections: &Vec<Projection>,
) -> ApiResult<(usize, Vec<GroupBySigType>)> {
    let mut signature = Vec::new();
    let mut column_id = None;

    if projections.len() < projection_id {
        return invalid_data_ae!(
            "can't group_by on {}, there is only {} projections",
            projection_id,
            projections.len()
        );
    }

    for (i, p) in projections.iter().enumerate() {
        if i == projection_id - 1 {
            match p.ptype {
                ProjectionType::Column(c) => {
                    column_id = Some(c);
                    signature.push(GroupBySigType::GroupByProjection);
                }
                _ => return invalid_data_ae!("Can't group by on not column"),
            }
        } else {
            match p.ptype {
                ProjectionType::Count => {
                    signature.push(GroupBySigType::Fun(FunctionAggregatorType::Count))
                }
                _ => {
                    return invalid_data_ae!(
                        "Another column in projection which should be grouped by"
                    )
                }
            }
        }
    }

    guard!(let Some(column_id) = column_id else {
        return invalid_data_ae!("Column for group_by not detected");
    });

    Ok((column_id, signature))
}

fn count_aggregator<'de>(columns_set: ColumnSet) -> ApiResult<AnyBaseAggregator<'de>> {
    if !columns_set.is_empty() {
        return invalid_data_ae!(
            "there is count() projection combined with another ones, but no group_by set"
        );
    }
    Ok(AnyBaseAggregator::Function(FunctionAggregator::new(
        FunctionAggregatorType::Count,
    )))
}

enum OrderByAggType {
    ProjectionId(usize),
    ColumnId(usize),
}

fn apply_order_by<'ret, 'de: 'ret>(
    aggregator: AnyBaseAggregator<'de>,
    headers: &Vec<String>,
    projections: &Vec<Projection>,
    order_by: Option<OrderBy>,
) -> ApiResult<Box<dyn Aggregator<'de> + 'ret>> {
    guard!(let Some(order_by) = order_by else { return Ok(aggregator.boxed()); });
    let order_by_type = validate_order_type(order_by.id, headers, projections)?;
    boxed_order_by_aggregator(aggregator, order_by_type, order_by.asc)
}

fn validate_order_type(
    id: OrderByIdType,
    headers: &Vec<String>,
    projections: &Vec<Projection>,
) -> ApiResult<OrderByAggType> {
    match &id {
        OrderByIdType::ProjectionId(pid) => {
            if projections.len() < *pid {
                return invalid_data_ae!(
                    "can't order by {}, there is only {} projections",
                    pid,
                    projections.len()
                );
            }
            return Ok(OrderByAggType::ProjectionId(*pid - 1));
        }
        OrderByIdType::Column(c) => {
            for (idx, _) in projections.iter().enumerate().filter(|(_, p)| p.name.eq(c)) {
                return Ok(OrderByAggType::ProjectionId(idx));
            }

            for (idx, _) in headers.iter().enumerate().filter(|(_, h)| *h == c) {
                for (pidx, _) in projections
                    .iter()
                    .enumerate()
                    .filter(|(_, p)| match p.ptype {
                        ProjectionType::Column(c) => c == idx,
                        _ => false,
                    })
                {
                    return Ok(OrderByAggType::ProjectionId(pidx));
                }
                return Ok(OrderByAggType::ColumnId(idx));
            }
        }
    }
    return invalid_data_ae!("wrong order_by id: {:?}", id);
}

pub(crate) trait Aggregator<'de> {
    fn aggregate(&mut self, value: &flexbuffers::VectorReader<'de>) -> ApiResult<()>;
    fn iter(&self) -> BoxedRecordIterator<'_, 'de>;
}

enum AnyBaseAggregator<'de> {
    Function(FunctionAggregator<'de>),
    Columns(ColumnsAggregator<'de>),
    GroupBy(GroupByAggregator<'de>),
}

impl<'de> AnyBaseAggregator<'de> {
    fn boxed<'ret>(self) -> Box<dyn Aggregator<'de> + 'ret>
    where
        'de: 'ret,
    {
        match self {
            Self::Function(a) => Box::new(a),
            Self::Columns(a) => Box::new(a),
            Self::GroupBy(a) => Box::new(a),
        }
    }
}

#[derive(Copy, Clone)]
enum FunctionAggregatorType {
    Count,
}

enum FunctionAggregatorInner {
    Count(u64),
}

struct FunctionAggregator<'de> {
    inner: FunctionAggregatorInner,
    _phatom: PhantomData<&'de ()>,
}

impl<'de> FunctionAggregator<'de> {
    fn new(fun: FunctionAggregatorType) -> Self {
        let inner = match fun {
            FunctionAggregatorType::Count => FunctionAggregatorInner::Count(0),
        };
        Self {
            inner,
            _phatom: PhantomData,
        }
    }

    fn value(&self) -> ValueRef<'de> {
        match self.inner {
            FunctionAggregatorInner::Count(c) => ValueRef::UInteger(c),
        }
    }
}

impl<'de> Aggregator<'de> for FunctionAggregator<'de> {
    fn aggregate(&mut self, _: &VectorReader<'de>) -> ApiResult<()> {
        match &mut self.inner {
            FunctionAggregatorInner::Count(c) => *c += 1,
        }
        Ok(())
    }

    fn iter(&self) -> BoxedRecordIterator<'_, 'de> {
        Box::new(OneRecordIterator::new(self.value()))
    }
}

trait OrderByCompatibleAggregator<'de>: Aggregator<'de> {
    fn aggregate_with_idx(&mut self, value: &VectorReader<'de>) -> ApiResult<usize>;
    fn record_random_iter(&self) -> Box<dyn RecordRandomIterator<'de> + '_>;
    fn query_order_by_value(&self, idx: usize, proj_idx: usize) -> ApiResult<ValueRef<'de>>;
}

trait RecordRandomIterator<'de> {
    fn at(&mut self, idx: usize) -> ApiResult<&dyn RecordRef<'de>>;
}

struct ColumnsAggregator<'de> {
    columns: ColumnSet,
    aggregated: Vec<Vec<Reader<'de>>>,
}

impl<'de> ColumnsAggregator<'de> {
    fn new(columns: ColumnSet) -> Self {
        Self {
            columns,
            aggregated: Vec::new(),
        }
    }
}

impl<'de> Aggregator<'de> for ColumnsAggregator<'de> {
    fn aggregate(&mut self, value: &VectorReader<'de>) -> ApiResult<()> {
        let mut record = Vec::with_capacity(self.columns.len());
        for col_idx in self.columns.iter() {
            let reader = value.index(*col_idx)?;
            record.push(reader);
        }
        self.aggregated.push(record);
        Ok(())
    }

    fn iter(&self) -> BoxedRecordIterator<'_, 'de> {
        Box::new(ColumnsRecordIterator::new(self.aggregated.iter()))
    }
}

impl<'de> OrderByCompatibleAggregator<'de> for ColumnsAggregator<'de> {
    fn aggregate_with_idx(&mut self, value: &VectorReader<'de>) -> ApiResult<usize> {
        self.aggregate(value)?;
        if self.aggregated.is_empty() {
            return invalid_data_ae!("nothing aggregated");
        }
        Ok(self.aggregated.len() - 1)
    }

    fn record_random_iter(&self) -> Box<dyn RecordRandomIterator<'de> + '_> {
        Box::new(ColumnsRecordRandomIterator::new(&self.aggregated))
    }

    fn query_order_by_value(&self, idx: usize, proj_idx: usize) -> ApiResult<ValueRef<'de>> {
        ColumnsRecordRandomIterator::new(&self.aggregated)
            .at(idx)?
            .value_at(proj_idx)
    }
}

struct ColumnsRecordRandomIterator<'a, 'de: 'a> {
    aggregated: &'a Vec<Vec<Reader<'de>>>,
}

impl<'a, 'de: 'a> ColumnsRecordRandomIterator<'a, 'de> {
    fn new(aggregated: &'a Vec<Vec<Reader<'de>>>) -> Self {
        Self { aggregated }
    }
}

impl<'a, 'de: 'a> RecordRandomIterator<'de> for ColumnsRecordRandomIterator<'a, 'de> {
    fn at(&mut self, idx: usize) -> ApiResult<&dyn RecordRef<'de>> {
        if self.aggregated.len() < idx {
            return invalid_data_ae!("out of bounds");
        }
        Ok(&self.aggregated[idx])
    }
}

struct ColumnsRecordIterator<'a, 'de: 'a> {
    iter: std::slice::Iter<'a, Vec<Reader<'de>>>,
}

impl<'a, 'de: 'a> ColumnsRecordIterator<'a, 'de> {
    fn new(iter: std::slice::Iter<'a, Vec<Reader<'de>>>) -> Self {
        Self { iter }
    }
}

impl<'a, 'de: 'a> RecordIterator<'de> for ColumnsRecordIterator<'a, 'de> {
    fn next(&mut self) -> ApiResult<Option<RecordIteratorItem<'_, 'de>>> {
        if let Some(res) = self.iter.next() {
            return Ok(Some(res));
        }
        Ok(None)
    }
}

impl<'de> RecordRef<'de> for Vec<Reader<'de>> {
    fn len(&self) -> usize {
        self.len()
    }
    fn value_at(&self, idx: usize) -> ApiResult<ValueRef<'de>> {
        guard!(let Some(reader) = self.get(idx) else {
            return invalid_data_ae!("out of bounds");
        });
        let result = ValueRef::from_reader(reader)?;
        Ok(result)
    }
}

struct GroupByAggregator<'de> {
    column_id: usize,
    signature: Vec<GroupBySigType>,
    aggregated: IndexMap<ValueOrdRef<'de>, Vec<GroupByItem<'de>>>,
}

enum GroupBySigType {
    Fun(FunctionAggregatorType),
    GroupByProjection,
}

impl GroupBySigType {
    fn as_new_item<'de>(&self) -> GroupByItem<'de> {
        match *self {
            Self::Fun(ft) => GroupByItem::Fun(FunctionAggregator::new(ft)),
            Self::GroupByProjection => GroupByItem::GroupByProjection,
        }
    }
}

enum GroupByItem<'de> {
    Fun(FunctionAggregator<'de>),
    GroupByProjection,
}

impl<'de> GroupByAggregator<'de> {
    fn new(column_id: usize, signature: Vec<GroupBySigType>) -> Self {
        Self {
            column_id,
            signature,
            aggregated: Default::default(),
        }
    }
}

impl<'de> Aggregator<'de> for GroupByAggregator<'de> {
    fn aggregate(&mut self, value: &VectorReader<'de>) -> ApiResult<()> {
        let _ = self.aggregate_with_idx(value)?;
        Ok(())
    }

    fn iter(&self) -> BoxedRecordIterator<'_, 'de> {
        Box::new(GroupByIterator::new(self.aggregated.iter()))
    }
}

impl<'de> OrderByCompatibleAggregator<'de> for GroupByAggregator<'de> {
    fn aggregate_with_idx(&mut self, value: &VectorReader<'de>) -> ApiResult<usize> {
        let reader = value.index(self.column_id)?;
        let key = ValueRef::from_reader(&reader)?;
        let entry = self.aggregated.entry(key.ord_ref());
        let idx = entry.index();
        let sig = &self.signature;
        let v = entry.or_insert_with(|| sig.iter().map(|s| s.as_new_item()).collect());
        for a in v.iter_mut() {
            match a {
                GroupByItem::Fun(fun) => fun.aggregate(value)?,
                _ => (),
            }
        }
        Ok(idx)
    }

    fn record_random_iter(&self) -> Box<dyn RecordRandomIterator<'de> + '_> {
        Box::new(GroupByRandomIterator::new(&self.aggregated))
    }

    fn query_order_by_value(&self, idx: usize, proj_idx: usize) -> ApiResult<ValueRef<'de>> {
        GroupByRandomIterator::new(&self.aggregated)
            .at(idx)?
            .value_at(proj_idx)
    }
}

struct GroupByRandomIterator<'a, 'de: 'a> {
    aggregated: &'a IndexMap<ValueOrdRef<'de>, Vec<GroupByItem<'de>>>,
    cur_item: Option<(&'a ValueOrdRef<'de>, &'a Vec<GroupByItem<'de>>)>,
}

impl<'a, 'de: 'a> GroupByRandomIterator<'a, 'de> {
    fn new(aggregated: &'a IndexMap<ValueOrdRef<'de>, Vec<GroupByItem<'de>>>) -> Self {
        Self {
            aggregated,
            cur_item: None,
        }
    }
}

impl<'a, 'de: 'a> RecordRandomIterator<'de> for GroupByRandomIterator<'a, 'de> {
    fn at(&mut self, idx: usize) -> ApiResult<&dyn RecordRef<'de>> {
        self.cur_item = self.aggregated.get_index(idx);
        guard!(let Some(cur) = self.cur_item.as_ref() else {
            return invalid_data_ae!("out of bounds");
        });
        Ok(cur)
    }
}

type GroupByInnerIter<'a, 'de> = indexmap::map::Iter<'a, ValueOrdRef<'de>, Vec<GroupByItem<'de>>>;

struct GroupByIterator<'a, 'de: 'a> {
    iter: GroupByInnerIter<'a, 'de>,
    cur_item: Option<(&'a ValueOrdRef<'de>, &'a Vec<GroupByItem<'de>>)>,
}

impl<'a, 'de> GroupByIterator<'a, 'de> {
    fn new(iter: GroupByInnerIter<'a, 'de>) -> Self {
        Self {
            iter,
            cur_item: None,
        }
    }
}

impl<'a, 'de> RecordIterator<'de> for GroupByIterator<'a, 'de> {
    fn next(&mut self) -> ApiResult<Option<RecordIteratorItem<'_, 'de>>> {
        guard!(let Some(next) = self.iter.next() else { return Ok(None); });
        self.cur_item = Some(next);
        guard!(let Some(cur) = self.cur_item.as_ref() else { return Ok(None); });
        Ok(Some(cur))
    }
}

impl<'de> RecordRef<'de> for (&ValueOrdRef<'de>, &Vec<GroupByItem<'de>>) {
    fn len(&self) -> usize {
        self.1.len()
    }

    fn value_at(&self, idx: usize) -> ApiResult<ValueRef<'de>> {
        if idx > self.1.len() - 1 {
            return invalid_data_ae!("out of bounds");
        }
        let result = match &self.1[idx] {
            GroupByItem::Fun(f) => f.value(),
            GroupByItem::GroupByProjection => self.0.as_value_ref(),
        };
        Ok(result)
    }
}

struct OrderByAggregator<'de, T: OrderByCompatibleAggregator<'de>> {
    inner: T,
    order_by_type: OrderByAggType,
    asc: bool,
    rec_values: HashMap<usize, ValueOrdRef<'de>>,
    ordered_recs: BTreeMap<ValueOrdRef<'de>, HashSet<usize>>,
}

fn boxed_order_by_aggregator<'ret, 'de: 'ret>(
    inner: AnyBaseAggregator<'de>,
    order_by_type: OrderByAggType,
    asc: bool,
) -> ApiResult<Box<dyn Aggregator<'de> + 'ret>> {
    match inner {
        AnyBaseAggregator::Function(_) => {
            invalid_data_ae!("can't group by with functional projection only")
        }
        AnyBaseAggregator::Columns(a) => Ok(OrderByAggregator::new(a, order_by_type, asc)),
        AnyBaseAggregator::GroupBy(a) => match order_by_type {
            OrderByAggType::ColumnId(_) => {
                return invalid_data_ae!("order_by column is not in group_by projection");
            }
            _ => Ok(OrderByAggregator::new(a, order_by_type, asc)),
        },
    }
}

impl<'de, T: OrderByCompatibleAggregator<'de>> OrderByAggregator<'de, T> {
    fn new(inner: T, order_by_type: OrderByAggType, asc: bool) -> Box<Self> {
        Box::new(Self {
            inner,
            order_by_type,
            asc,
            rec_values: HashMap::new(),
            ordered_recs: BTreeMap::new(),
        })
    }

    fn key_value_for(
        &self,
        value: &VectorReader<'de>,
        last_idx: usize,
    ) -> ApiResult<ValueOrdRef<'de>> {
        let result = match self.order_by_type {
            OrderByAggType::ColumnId(c) => {
                let reader = value.index(c)?;
                ValueRef::from_reader(&reader)?.ord_ref()
            }
            OrderByAggType::ProjectionId(p) => {
                self.inner.query_order_by_value(last_idx, p)?.ord_ref()
            }
        };
        Ok(result)
    }

    fn check_need_insert(
        &mut self,
        key_value: &ValueOrdRef<'de>,
        last_idx: usize,
    ) -> ApiResult<bool> {
        let entry = self.rec_values.entry(last_idx.clone());
        use std::collections::hash_map::Entry;

        let result = match entry {
            Entry::Occupied(ref entry) if entry.get().eq(&key_value) => false,
            Entry::Occupied(mut entry) => {
                match self.ordered_recs.entry(entry.get().clone()) {
                    std::collections::btree_map::Entry::Occupied(mut old_entry) => {
                        let old_set = old_entry.get_mut();
                        if old_set.len() == 1 {
                            old_entry.remove();
                        } else {
                            old_set.remove(&last_idx);
                        }
                    }
                    _ => return invalid_data_ae!("query consistency corruption"),
                }
                entry.insert(key_value.clone());
                true
            }
            Entry::Vacant(entry) => {
                entry.insert(key_value.clone());
                true
            }
        };
        Ok(result)
    }
}

impl<'de, T: OrderByCompatibleAggregator<'de>> Aggregator<'de> for OrderByAggregator<'de, T> {
    fn aggregate(&mut self, value: &VectorReader<'de>) -> ApiResult<()> {
        let last_idx = self.inner.aggregate_with_idx(value)?;
        let key_value = self.key_value_for(value, last_idx)?;

        if self.check_need_insert(&key_value, last_idx.clone())? {
            if !self
                .ordered_recs
                .entry(key_value)
                .or_insert_with(|| HashSet::new())
                .insert(last_idx)
            {
                return invalid_data_ae!("query consistency corruption");
            }
        }
        Ok(())
    }

    fn iter(&self) -> BoxedRecordIterator<'_, 'de> {
        if self.asc {
            Box::new(OrderByIterator::new(
                self.ordered_recs.iter(),
                self.inner.record_random_iter(),
            ))
        } else {
            Box::new(OrderByIterator::new(
                self.ordered_recs.iter().rev(),
                self.inner.record_random_iter(),
            ))
        }
    }
}

type OrderedIdsIterItem<'a, 'de> = (&'a ValueOrdRef<'de>, &'a HashSet<usize>);

struct OrderByIterator<'a, 'de: 'a, I>
where
    I: Iterator<Item = OrderedIdsIterItem<'a, 'de>>,
{
    inner_iter: I,
    rec_rand_iter: Box<dyn RecordRandomIterator<'de> + 'a>,
    rec_ids_iter: Option<std::collections::hash_set::Iter<'a, usize>>,
}

impl<'a, 'de: 'a, I> OrderByIterator<'a, 'de, I>
where
    I: Iterator<Item = OrderedIdsIterItem<'a, 'de>>,
{
    fn new(inner_iter: I, rec_rand_iter: Box<dyn RecordRandomIterator<'de> + 'a>) -> Self {
        Self {
            inner_iter,
            rec_rand_iter,
            rec_ids_iter: None,
        }
    }
}

impl<'a, 'de: 'a, I> RecordIterator<'de> for OrderByIterator<'a, 'de, I>
where
    I: Iterator<Item = OrderedIdsIterItem<'a, 'de>>,
{
    fn next(&mut self) -> ApiResult<Option<RecordIteratorItem<'_, 'de>>> {
        loop {
            if let Some(rec_ids_iter) = self.rec_ids_iter.as_mut() {
                if let Some(idx) = rec_ids_iter.next() {
                    return Ok(Some(self.rec_rand_iter.at(*idx)?));
                }
            }
            if let Some((_, ids_set)) = self.inner_iter.next() {
                self.rec_ids_iter = Some(ids_set.iter());
                continue;
            }
            return Ok(None);
        }
    }
}

#[test]
fn test_column_set() {
    let mut set: ColumnSet = Default::default();
    assert!(set.insert(0));
    assert!(!set.insert(0));
    assert!(set.insert(1));
    assert!(set.insert(2));
    assert!(set.insert(3));
    assert!(set.insert(4));
    assert!(set.insert(5));

    let mut iter = set.into_iter();
    assert_eq!(iter.next().unwrap(), 0);
    assert_eq!(iter.next().unwrap(), 1);
    assert_eq!(iter.next().unwrap(), 2);
    assert_eq!(iter.next().unwrap(), 3);
    assert_eq!(iter.next().unwrap(), 4);
    assert_eq!(iter.next().unwrap(), 5);
    assert_eq!(iter.next(), None);
}
