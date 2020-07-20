### Description

Write simple columnar database in any language which:

1. Supports REST API
2. Stores data in any columnar format.
3. Supports Import of CSV files into table. REST path: POST `/import/:table`
4. [https://www.kaggle.com/hanselhansel/donorschoose?select=Donors.csv](https://www.kaggle.com/hanselhansel/donorschoose?select=Donors.csv) should be used as test data source to load
5. Supports COUNT aggregation function, GROUP BY for any set of columns, ORDER BY and simple equals filter. REST path: GET `/query?sql=`
6. Queries to use for tests

    ```
    SELECT
      `donors`."Donor State" `donors__donor_state`,
      count(*) `donors__count`FROM
      test.donors AS `donors`GROUP BY
      1
    ORDER BY
      2 DESC
    LIMIT
      10000
    ```

    ```
    SELECT
      count(*) `donors__count`FROM
      test.donors AS `donors`WHERE
      (`donors`."Donor City" = "San Francisco")
    LIMIT
      10000
    ```


### Implementation notes

Column format is schema-less, on binary level it is [flexbuffers](https://docs.rs/flexbuffers/0.1.1/flexbuffers/).

Columnar data format implemented upon low-level embedded key-value database [libmdbx](http://erthink.github.io/libmdbx/).
It has exceptional performance, and in conjunction with flexbuffers allows zero-copy data access. 

SQL query processing fully implemented in `src/aggregator.rs`.
But as query parser extern [sqlparser](https://crates.io/crates/sqlparser) crate was used, see wrapper in `src/query.rs`.

For REST api, [Rocket](https://github.com/SergioBenitez/Rocket.git) was used, see `src/main.rs`.
Right now, Rocket is in process of migration from synchronous to asynchronous (tokio)[https://github.com/tokio-rs/tokio] runtime.
And, as I wanted everything to be asynchronous with power of async/await, I used master branch of Rocket.

And, obviously, everything is written in Rust language, in it's `safe` subset. 
You can find `unsafe` invocations only in code related to pure C database integration, see `src/db.rs`.

#### Overview

### Performance

On my macbook, upload 118 mb csv with 273874 records:

    duration: 323.809854ms
    
SELECT * FROM donors

    duration: 225.591343ms
    
SELECT donors."Donor State" FROM donors GROUP BY 1 ORDER BY 1

    duration: 56.239348ms

#### Configuration

Rustup:

    curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh

Rust nightly:

    rustup install nightly
    rustup default nightly
    rustup override set nightly
    
Run with:

    cargo run --release
    
Default port is 8000, could be changed in `Rocket.toml`

### Testing notes

#### Cleanup

To clean database:

    rm -rf ~/.db_test
    
#### Curl

To upload Donors.csv with curl:
    
    curl --data-binary "@Donors.csv" -X POST http://0.0.0.0:8000/import/donors
    
First test query with curl:

    curl "http://0.0.0.0:8000/query?sql=SELECT%0A%20%20%60donors%60.%22Donor%20State%22%20%60donors__donor_state%60%2C%0A%20%20count%28%2A%29%20%60donors__count%60FROM%0A%20%20test.donors%20AS%20%60donors%60GROUP%20BY%0A%20%201%0AORDER%20BY%0A%20%202%20DESC%0ALIMIT%0A%20%2010000"

Second test query with curl:

    curl "http://0.0.0.0:8000/query?sql=SELECT%0A%20%20count%28%2A%29%20%60donors__count%60FROM%0A%20%20test.donors%20AS%20%60donors%60WHERE%0A%20%20%28%60donors%60.%22Donor%20City%22%20%3D%20%22San%20Francisco%22%29%0ALIMIT%0A%20%2010000"
    
#### Query test 
    
Also, there is simple query test page available at http://0.0.0.0:8000/test
    
![Alt text](img/1.png?raw=true "Title")

#### Docker

If, for some reason, cargo build not working, there is docker container available:

    docker-compose up --build

