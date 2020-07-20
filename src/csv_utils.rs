use bytes::BufMut;
use tokio::io::Result;

pub struct CSVImportReader {
    cur_bytes: bytes::Bytes,
    reader: CSVReader,
    headers: Vec<String>,
}

impl CSVImportReader {
    pub fn from_first_chunk(data: bytes::Bytes) -> Result<Self> {
        let mut result = Self {
            cur_bytes: data,
            reader: CSVReader::new(),
            headers: Vec::new(),
        };
        result.parse_headers()?;
        Ok(result)
    }

    #[inline]
    pub fn headers(&self) -> &Vec<String> {
        &self.headers
    }

    fn parse_headers(&mut self) -> Result<()> {
        if self.reader.parse_record(self.cur_bytes.as_ref())? != ParseRecordRes::Done {
            return invalid_data_e!("can't parse headers: broken row");
        }
        let mut prev_pos = 0;
        self.headers = self
            .reader
            .out_fields
            .iter()
            .take(self.reader.nend)
            .map(|pos| {
                let slice = &self.reader.out[prev_pos..*pos];
                let header = String::from_utf8_lossy(slice);
                prev_pos = *pos;
                header.to_string()
            })
            .collect();
        Ok(())
    }

    pub fn parse_records(&mut self) -> RecordIter {
        RecordIter::new(
            &self.cur_bytes.as_ref()[self.reader.all_nin..],
            &mut self.reader,
        )
    }

    pub fn add_chunk(&mut self, data: bytes::Bytes) {
        // println!("**** Add new chunk ****");
        let tail_count = self.cur_bytes.len() - self.reader.all_nin;
        if tail_count > 0 {
            let mut bytes = bytes::BytesMut::with_capacity(tail_count);
            let tail = &self.cur_bytes.as_ref()[self.reader.all_nin..];
            bytes.put(tail);
            bytes.put(data);
            self.cur_bytes = bytes.freeze();
        } else {
            self.cur_bytes = data;
        }
        // println!("{}", String::from_utf8_lossy(self.cur_bytes.as_ref()));
        self.reader.flush();
    }
}

const S_OUT_BYTES_COUNT: usize = 8192;
const S_OUT_MAX_FIELDS_COUNT: usize = 100;

struct CSVReader {
    rdr: csv_core::Reader,
    out: [u8; S_OUT_BYTES_COUNT],
    out_fields: [usize; S_OUT_MAX_FIELDS_COUNT],
    all_nin: usize,
    nin: usize,
    nout: usize,
    nend: usize,
}

#[derive(PartialEq)]
enum ParseRecordRes {
    NeedMoreData,
    Done,
}

impl CSVReader {
    fn new() -> Self {
        Self {
            rdr: csv_core::Reader::new(),
            out: [0; S_OUT_BYTES_COUNT],
            out_fields: [0; S_OUT_MAX_FIELDS_COUNT],
            all_nin: 0,
            nin: 0,
            nout: 0,
            nend: 0,
        }
    }

    fn parse_record(&mut self, data: &[u8]) -> Result<ParseRecordRes> {
        let (pres, nin, nout, nend) =
            self.rdr
                .read_record(data, &mut self.out, &mut self.out_fields);

        let res = match pres {
            csv_core::ReadRecordResult::InputEmpty => ParseRecordRes::NeedMoreData,
            csv_core::ReadRecordResult::End => ParseRecordRes::NeedMoreData,
            csv_core::ReadRecordResult::Record => ParseRecordRes::Done,
            _ => return invalid_data_e!("too long csv record"),
        };

        if res == ParseRecordRes::Done {
            self.all_nin += nin;
            self.nin = nin;
            self.nout = nout;
            self.nend = nend;
        }
        Ok(res)
    }

    fn flush(&mut self) {
        self.rdr = csv_core::Reader::new();
        self.all_nin = 0;
        self.nin = 0;
        self.nout = 0;
        self.nend = 0;
    }
}

pub struct RecordIter<'a> {
    cur_buf: &'a [u8],
    reader: &'a mut CSVReader,
}

impl<'a> RecordIter<'a> {
    fn new(cur_buf: &'a [u8], reader: &'a mut CSVReader) -> Self {
        Self { cur_buf, reader }
    }

    pub fn next(&mut self) -> Option<Result<CSVRecord>> {
        let parse_rec_res = self.reader.parse_record(self.cur_buf);
        if parse_rec_res.is_err() {
            return Some(Err(parse_rec_res.err().unwrap()));
        }
        if parse_rec_res.unwrap() != ParseRecordRes::Done {
            return None;
        }
        self.cur_buf = &self.cur_buf[self.reader.nin..];
        Some(Ok(CSVRecord::new(self.reader)))
    }
}

pub struct CSVRecord<'a> {
    reader: &'a CSVReader,
}

impl<'a> CSVRecord<'a> {
    fn new(reader: &'a CSVReader) -> Self {
        Self { reader }
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.reader.nend
    }

    pub fn as_flexbuffer(&self) -> Vec<u8> {
        let mut builder = flexbuffers::Builder::default();
        let mut vec = builder.start_vector();
        let mut prev_pos = 0;
        // println!(" --- RECORD ---");
        self.reader
            .out_fields
            .iter()
            .take(self.reader.nend)
            .for_each(|pos| {
                let slice: &'a [u8] = &self.reader.out[prev_pos..*pos];
                let value = String::from_utf8_lossy(slice);
                prev_pos = *pos;
                // println!("{}", value);
                if value.len() == 0 {
                    vec.push(());
                } else if let Ok(num) = value.parse::<u64>() {
                    vec.push(num);
                } else if let Ok(num) = value.parse::<i64>() {
                    vec.push(num);
                } else if let Ok(num) = value.parse::<f64>() {
                    vec.push(num);
                } else {
                    vec.push(value.as_ref());
                }
            });
        vec.end_vector();
        builder.take_buffer()
    }
}
