#![feature(async_closure)]

#[macro_use]
mod errors;
#[macro_use]
mod misc_utils;

mod aggregator;
mod csv_utils;
mod db;
mod filter;
mod logger;
mod query;
mod query_processor;
mod record;

use csv_utils::*;
use db::*;
use errors::*;
use misc_utils::*;
use query::*;
use query_processor::*;
use record::*;

#[macro_use]
extern crate guard;

#[macro_use]
extern crate rocket;

// #[macro_use]
// extern crate slog;

use mimalloc::MiMalloc;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

use futures::stream::StreamExt;
use logger::SyncLogger;
use rocket::{config::LoggingLevel, data::ToByteUnit, request::Form, Data};
use rocket_contrib::serve::StaticFiles;
use sloggers::{
    terminal::{Destination, TerminalLoggerBuilder},
    types::Severity,
    Build,
};
use std::borrow::BorrowMut;
use std::fmt::Write;
use std::sync::Arc;
use std::time::Instant;

//`import` is async endpoint,
// we need to await body chunks and also unique writer lock
// without locking
#[post("/import/<table>", data = "<data>")]
async fn import(
    /*log: SyncLogger,*/ db: DBHandle,
    table: String,
    data: Data,
) -> ApiResult<String> {
    let start = Instant::now();
    // Open write transaction, only one write txn is possible at the moment,
    // so we should await in case if second write txn exist and acquire lock
    let mut write_lock = db.mutation().await;
    let mutable_db = write_lock.borrow_mut();

    mutable_db
        .mutable_table(table.clone(), async move |mut txn| -> ApiResult<String> {
            // Erase table if there is previous import
            txn.drop()?;

            // Transform request Body into stream of bytes
            let mut bs = into_bytes_stream(data.open(16.megabytes()));
            guard!(let Some(bytes) = bs.next().await else { return invalid_data_ae!("empty body") });

            // Construct csv reader object with first chunk of data and try parse headers
            let mut reader = CSVImportReader::from_first_chunk(bytes?)?;
            // debug!(log, "headers parsed: {:?}", reader.headers());

            let mut records_imported: usize = 0;

            // Write headers
            let headers = reader.headers();
            txn.append(string_vec_to_flex(headers))?;

            let headers_count = headers.len();
            loop {
                // Start parsing csv records
                let mut iter = reader.parse_records();
                while let Some(value) = iter.next() {
                    let record = value?;
                    if headers_count != record.len() {
                        return invalid_data_ae!(
                    "headers and fields count mismatch, expected {}, got {}",
                    headers_count,
                    record.len()
                );
                    }
                    // Convert parsed record to flexbuffers and append to table
                    txn.append(record.as_flexbuffer())?;
                    records_imported += 1;
                }

                // Await for next chunk and add to cvs reader
                guard!(let Some(bytes) = bs.next().await else { break });
                reader.add_chunk(bytes?);
            }

            txn.commit()?;

            Ok(format!(
                "{} records sucessfully imported, duration: {:?}",
                records_imported,
                start.elapsed()
            ))
        })
        .await
}

#[derive(FromForm)]
struct SQLQueryString {
    sql: String,
}

//`query` is sync endpoint, as there are no locks on critical path
#[get("/query?<sql..>")]
fn query(db: DBHandle, sql: Option<Form<SQLQueryString>>) -> ApiResult<String> {
    let start = Instant::now();
    guard!(let Some(sql) = sql else { return invalid_data_ae!("query is empty"); });
    // Parsing query string
    let query = Query::from_query_str(sql.sql.as_str())?;
    // Get table reference from query
    let table_ref = query.get_table_name()?;
    // Open read transaction
    let table = db.table(table_ref.name);
    let txn = table.open_transaction()?;
    // And acquire cursor
    let mut cursor = txn.cursor_on_start()?;
    // Getting headers from first record
    let headers = flex_to_string_vec(cursor.data()?);
    // Initialize query processor
    let mut processor = QueryProcessor::new(query, headers)?;
    while let Some(rec) = cursor.next()? {
        if !processor.process_record(&rec)? {
            break;
        }
    }
    // Iterate over results
    let mut result = String::new();
    writeln!(result, "{}", processor.headers_csv())?;
    let mut iter = processor.iter();
    while let Some(record) = iter.next()? {
        writeln!(result, "{}", record.to_csv()?)?;
    }
    writeln!(result, "duration: {:?}", start.elapsed())?;
    Ok(result)
}

async fn run() -> ApiResult<()> {
    let mut rocket = rocket::ignite();
    let config = rocket.inspect().await.config();

    let logger = TerminalLoggerBuilder::new()
        // .level(Severity::Debug)
        .level(match config.log_level {
            LoggingLevel::Debug => Severity::Debug,
            _ => Severity::Critical,
        })
        .destination(Destination::Stderr)
        .build()
        .unwrap();

    let logger = SyncLogger(Arc::new(logger));

    let db = db::DB::new("~/.db_test").unwrap();
    let db = DBHandle(Arc::new(db));

    rocket
        .mount("/", routes![import, query])
        .mount("/test", StaticFiles::from("static"))
        .manage(logger)
        .manage(db)
        .launch()
        .await?;

    Ok(())
}

fn main() {
    let mut runtime = tokio::runtime::Builder::new()
        .core_threads(4)
        .threaded_scheduler()
        .enable_all()
        .build()
        .unwrap();

    runtime.block_on(run()).unwrap();
    println!("*********")
}
