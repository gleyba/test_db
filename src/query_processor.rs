use crate::aggregator::*;
use crate::errors::*;
use crate::filter::*;
use crate::query::*;
use crate::record::*;

pub struct QueryProcessor<'agg, 'de: 'agg> {
    projection_headers: Vec<String>,
    filter: Option<Box<dyn Filter>>,
    aggregator: Box<dyn Aggregator<'de> + 'agg>,
}

impl<'agg, 'de: 'agg> QueryProcessor<'agg, 'de> {
    pub fn new(query: Query, headers: Vec<String>) -> ApiResult<Self> {
        let select = query.select()?;
        let projections = Projection::compose_projections(select, &headers)?;
        let group_by = query.get_if_group_by()?;
        let order_by = query.get_if_order_by()?;

        let projection_headers = projections
            .iter()
            .map(|p| p.name.clone())
            .collect::<Vec<String>>();

        let aggregator = create_aggregator(&headers, projections, group_by, order_by)?;

        let limit = query.get_if_limit()?;
        let selection = parse_if_has_selection(select, &headers)?;

        let filter = create_filter(limit, selection)?;

        Ok(Self {
            projection_headers,
            filter,
            aggregator,
        })
    }

    pub fn process_record(&mut self, record: &flexbuffers::VectorReader<'de>) -> ApiResult<bool> {
        if let Some(filter) = self.filter.as_mut() {
            match filter.filter(record)? {
                FilterRes::NeedStop => return Ok(false),
                FilterRes::NeedPass => return Ok(true),
                FilterRes::NeedProcess => (),
            }
        }

        self.aggregator.aggregate(record)?;
        Ok(true)
    }

    pub fn headers_csv(&self) -> String {
        self.projection_headers.join(",")
    }

    pub fn iter(&self) -> RecordIterWrapper<'_, 'de> {
        RecordIterWrapper::new(self.aggregator.iter())
    }
}
