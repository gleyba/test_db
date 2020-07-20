use crate::errors::*;
use crate::record::{parse_number, ValueOrdRef};
use json::number::Number;
use sqlparser::{ast, dialect::Dialect, parser::Parser};

#[derive(Clone)]
pub(crate) struct Projection {
    pub(crate) name: String,
    pub(crate) ptype: ProjectionType,
}

#[derive(Clone)]
pub(crate) enum ProjectionType {
    Column(usize),
    Count,
}

impl Projection {
    pub(crate) fn compose_projections(
        select: &Box<ast::Select>,
        headers: &Vec<String>,
    ) -> ApiResult<Vec<Projection>> {
        let mut projections = Vec::new();
        projections.reserve(select.projection.len());
        for p in select.projection.iter() {
            match p {
                ast::SelectItem::UnnamedExpr(e) => Self::add_projection_from_expr(
                    &select.projection,
                    &e,
                    None,
                    headers,
                    &mut projections,
                )?,
                ast::SelectItem::ExprWithAlias { expr: e, alias: i } => {
                    Self::add_projection_from_expr(
                        &select.projection,
                        &e,
                        Some(&i.value),
                        headers,
                        &mut projections,
                    )?
                }
                ast::SelectItem::Wildcard => {
                    let e = ast::Expr::Wildcard;
                    Self::add_projection_from_expr(
                        &select.projection,
                        &e,
                        None,
                        headers,
                        &mut projections,
                    )?
                }
                _ => return invalid_data_ae!("unsupported projection: {:?}", p),
            };
        }
        if projections.is_empty() {
            return invalid_data_ae!("no any projections");
        }

        Ok(projections)
    }

    fn column(name: String, idx: usize) -> Self {
        Self {
            name,
            ptype: ProjectionType::Column(idx),
        }
    }

    fn count(name: String) -> Self {
        Self {
            name,
            ptype: ProjectionType::Count,
        }
    }

    fn add_projection_from_expr(
        query_projections: &Vec<ast::SelectItem>,
        expr: &ast::Expr,
        alias: Option<&String>,
        headers: &Vec<String>,
        projections: &mut Vec<Projection>,
    ) -> ApiResult<()> {
        match expr {
            ast::Expr::Wildcard => {
                if query_projections.len() != 1 {
                    return invalid_data_ae!("wildcard combined with other projections");
                }
                if alias.is_some() {
                    return invalid_data_ae!("alias for wildcard");
                }
                headers
                    .iter()
                    .enumerate()
                    .map(|(id, h)| Self::column(h.clone(), id))
                    .for_each(|p| projections.push(p));
            }
            ast::Expr::Identifier(i) => {
                projections.push(Self::projection_from_column_name(&i.value, alias, headers)?);
            }
            ast::Expr::CompoundIdentifier(c) => {
                projections.push(Self::projection_from_column_name(
                    &last_ident(c)?.value,
                    alias,
                    headers,
                )?);
            }
            ast::Expr::Function(f) => {
                if f.name.0.len() != 1 || f.name.0.first().unwrap().value.to_lowercase() != "count"
                {
                    return invalid_data_ae!("unsupported function projection: {:?}", f);
                }
                projections.push(Self::count(
                    alias.cloned().unwrap_or_else(|| "count(*)".to_owned()),
                ));
            }
            _ => return invalid_data_ae!("unsupported expression in projection: {:?}", expr),
        };
        Ok(())
    }

    fn projection_from_column_name(
        column: &String,
        alias: Option<&String>,
        headers: &Vec<String>,
    ) -> ApiResult<Projection> {
        for (i, h) in headers.iter().enumerate() {
            if h == column {
                return Ok(Self::column(alias.unwrap_or(column).clone(), i));
            }
        }
        return invalid_data_ae!("can't find column with name {}", column);
    }
}

#[derive(Debug)]
pub struct Query(Box<ast::Query>);

impl Query {
    pub fn from_query_str(query_sql: &str) -> ApiResult<Self> {
        let ast = Self::parse_query(query_sql)?;
        Self::validate_query(&ast)?;
        Ok(Self(ast))
    }

    fn parse_query(query_sql: &str) -> ApiResult<Box<ast::Query>> {
        let dialect = TestDialect {};
        let ast = Parser::parse_sql(&dialect, query_sql)?;
        if ast.len() != 1 {
            return invalid_data_ae!("expected just 1 sql query");
        }
        guard!(let ast::Statement::Query(query) = ast.into_iter().next().unwrap()
        else { return invalid_data_ae!("expected query request"); });

        Ok(query)
    }

    fn validate_query(query: &Box<ast::Query>) -> ApiResult<()> {
        if !query.ctes.is_empty() {
            return invalid_data_ae!("no ctes supported");
        }

        if query.offset.is_some() {
            return invalid_data_ae!("offset not supported");
        }

        if query.fetch.is_some() {
            return invalid_data_ae!("fetch not supported");
        }

        guard!(let ast::SetExpr::Select(select) = &query.body
            else { return invalid_data_ae!("only ordinal select supported"); });

        if select.from.len() != 1 {
            return invalid_data_ae!("only one from supported");
        }

        Ok(())
    }

    pub(crate) fn select(&self) -> ApiResult<&Box<ast::Select>> {
        match &self.0.body {
            ast::SetExpr::Select(select) => Ok(select),
            _ => return invalid_data_ae!("cant't unwrap select"),
        }
    }

    pub fn get_table_name(&self) -> ApiResult<TableRef> {
        let select = self.select()?;

        let from = match select.from.first() {
            Some(from) => from,
            _ => return invalid_data_ae!("no suitable table source"),
        };

        let (name_parts, alias) = match &from.relation {
            ast::TableFactor::Table {
                name,
                alias,
                args: _,
                with_hints: _,
            } => (name, alias),
            _ => return invalid_data_ae!("no suitable table source"),
        };

        let name = match name_parts.0.last() {
            Some(ident) => &ident.value,
            _ => return invalid_data_ae!("no suitable table source"),
        };

        let res = TableRef {
            name: name.clone(),
            alias: alias.as_ref().map(|a| a.name.value.clone()),
        };

        Ok(res)
    }

    pub(crate) fn get_if_group_by(&self) -> ApiResult<Option<GroupBy>> {
        let select = self.select()?;
        if select.group_by.is_empty() {
            return Ok(None);
        }
        if select.group_by.len() > 1 {
            return invalid_data_ae!("only one group_by supported: {:?}", select.group_by);
        }
        match select.group_by.first().unwrap() {
            ast::Expr::Value(v) => {
                guard!(let Some(num) = parse_ast_number(v) else {
                    return invalid_data_ae!("only numeric group_by supported: {:?}", select.group_by);
                });
                return Ok(Some(GroupBy::ProjectionId(num)));
            }
            ast::Expr::Identifier(i) => return Ok(Some(GroupBy::Column(i.value.clone()))),
            ast::Expr::CompoundIdentifier(vi) => {
                return Ok(Some(GroupBy::Column(last_ident(vi)?.value.clone())))
            }
            _ => {}
        }

        return invalid_data_ae!("unsupported group_by expression: {:?}", select.group_by);
    }

    pub(crate) fn get_if_order_by(&self) -> ApiResult<Option<OrderBy>> {
        if self.0.order_by.is_empty() {
            return Ok(None);
        }
        if self.0.order_by.len() > 1 {
            return invalid_data_ae!("only one order_by supported: {:?}", self.0.order_by);
        }

        let order_by = self.0.order_by.first().unwrap();
        match &order_by.expr {
            ast::Expr::Value(v) => {
                guard!(let Some(num) = parse_ast_number(v) else {
                    return invalid_data_ae!("only numeric order_by supported: {:?}", order_by);
                });
                return Ok(Some(OrderBy {
                    id: OrderByIdType::ProjectionId(num),
                    asc: order_by.asc.unwrap_or(true),
                }));
            }
            ast::Expr::CompoundIdentifier(vi) => {
                return Ok(Some(OrderBy {
                    id: OrderByIdType::Column(last_ident(vi)?.value.clone()),
                    asc: order_by.asc.unwrap_or(true),
                }));
            }
            _ => {}
        }
        return invalid_data_ae!("unsupported order_by expression: {:?}", order_by);
    }

    pub(crate) fn get_if_limit(&self) -> ApiResult<Option<usize>> {
        if self.0.limit.is_none() {
            return Ok(None);
        }

        let limit = self.0.limit.as_ref().unwrap();
        match limit {
            ast::Expr::Value(v) => {
                if let Some(num) = parse_ast_number(v) {
                    return Ok(Some(num));
                }
            }
            _ => {}
        }
        return invalid_data_ae!("unsupported limit expression: {:?}", limit);
    }
}

#[derive(Debug)]
pub(crate) enum OrderByIdType {
    ProjectionId(usize),
    Column(String),
}

pub(crate) struct OrderBy {
    pub(crate) id: OrderByIdType,
    pub(crate) asc: bool,
}

#[derive(Debug)]
pub(crate) enum GroupBy {
    ProjectionId(usize),
    Column(String),
}

pub(crate) fn parse_if_has_selection(
    select: &Box<ast::Select>,
    headers: &Vec<String>,
) -> ApiResult<Option<Selection>> {
    guard!(let Some(selection) = &select.selection else { return Ok(None)});
    Ok(Some(parse_selection(selection, headers)?))
}

fn parse_selection(expr: &ast::Expr, headers: &Vec<String>) -> ApiResult<Selection> {
    match expr {
        ast::Expr::Nested(n) => parse_selection(n.as_ref(), headers),
        ast::Expr::BinaryOp { left, op, right } => {
            parse_binary_op(left, op.clone(), right, headers)
        }
        _ => return invalid_data_ae!("Unsupported selection expression: {:?}", expr),
    }
}

fn parse_binary_op(
    left: &Box<ast::Expr>,
    op: ast::BinaryOperator,
    right: &Box<ast::Expr>,
    headers: &Vec<String>,
) -> ApiResult<Selection> {
    Ok(Selection::BinaryOp(
        parse_column(left.as_ref(), headers)?,
        parse_selection_op_type(op)?,
        parse_selection_value(right.as_ref())?,
    ))
}

fn parse_column(expr: &ast::Expr, headers: &Vec<String>) -> ApiResult<usize> {
    let col_name = match expr {
        ast::Expr::CompoundIdentifier(vi) => last_ident(vi)?,
        _ => return invalid_data_ae!("unsupported column selection expr: {:?}", expr),
    };

    for (i, h) in headers.iter().enumerate() {
        if col_name.value.eq(h) {
            return Ok(i);
        }
    }

    invalid_data_ae!("invalid column selection expr: {:?}", expr)
}

fn parse_selection_op_type(op: ast::BinaryOperator) -> ApiResult<BinaryOpType> {
    let result = match op {
        ast::BinaryOperator::Eq => BinaryOpType::Eq,
        _ => return invalid_data_ae!("unsupported op in selection: {:?}", op),
    };
    Ok(result)
}

fn parse_selection_value(expr: &ast::Expr) -> ApiResult<SelectionValue> {
    let result = match expr {
        ast::Expr::Identifier(i) => SelectionValue::String(i.value.clone()),
        ast::Expr::Value(v) => match v {
            ast::Value::Number(num_str) => SelectionValue::Number(parse_number(num_str)?),
            _ => return invalid_data_ae!("unsupported selection value expr: {:?}", expr),
        },
        _ => return invalid_data_ae!("unsupported selection value expr: {:?}", expr),
    };
    Ok(result)
}

#[derive(Debug)]
pub(crate) enum Selection {
    BinaryOp(usize, BinaryOpType, SelectionValue),
}

#[derive(Debug)]
pub(crate) enum SelectionValue {
    String(String),
    Number(Number),
}

impl SelectionValue {
    pub(crate) fn as_ord_ref(&self) -> ValueOrdRef {
        match self {
            Self::String(s) => ValueOrdRef::Str(s.as_ref()),
            Self::Number(n) => ValueOrdRef::Number(*n),
        }
    }
}

#[derive(Debug)]
pub(crate) enum BinaryOpType {
    Eq,
}

fn parse_ast_number(value: &ast::Value) -> Option<usize> {
    match value {
        ast::Value::Number(v) => match v.parse::<usize>() {
            Ok(v) => Some(v),
            _ => None,
        },
        _ => None,
    }
}

fn last_ident(idents: &Vec<sqlparser::ast::Ident>) -> ApiResult<&sqlparser::ast::Ident> {
    guard!(let Some(i) = idents.last() else {
        return invalid_data_ae!("wrong compound identifier: {:?}", idents)
    });
    Ok(i)
}

pub struct TableRef {
    pub name: String,
    pub alias: Option<String>,
}

#[derive(Debug, Default)]
pub struct TestDialect;

impl Dialect for TestDialect {
    fn is_delimited_identifier_start(&self, ch: char) -> bool {
        ch == '"' || ch == '`'
    }

    fn is_identifier_start(&self, ch: char) -> bool {
        (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') || ch == '_' || ch == '#' || ch == '@'
    }

    fn is_identifier_part(&self, ch: char) -> bool {
        (ch >= 'a' && ch <= 'z')
            || (ch >= 'A' && ch <= 'Z')
            || (ch >= '0' && ch <= '9')
            || ch == '@'
            || ch == '$'
            || ch == '#'
            || ch == '_'
    }
}
