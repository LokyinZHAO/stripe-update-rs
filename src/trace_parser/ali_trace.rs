use std::{fs::File, io::BufRead};

use crate::{trace_parser::Trace, SUResult};

pub struct AliTraceParser {
    trace_file: std::io::BufReader<File>,
    buf: String,
}

impl AliTraceParser {
    pub fn open(path: &std::path::Path) -> SUResult<Self> {
        Ok(AliTraceParser {
            trace_file: std::io::BufReader::new(std::fs::File::open(path)?),
            buf: String::default(),
        })
    }
}

impl Iterator for AliTraceParser {
    type Item = Trace;

    fn next(&mut self) -> Option<Self::Item> {
        self.buf.clear();
        loop {
            return match self.trace_file.read_line(&mut self.buf) {
                Ok(0) => None, // EOF
                Ok(_) => {
                    if self.buf.chars().all(|ch| char::is_ascii_whitespace(&ch)) {
                        // skip whitespace line
                        continue;
                    }
                    // trace format:
                    // device_id, operation, offset, len, timestamp
                    let split = self.buf.split(',').collect::<Vec<_>>();
                    if split.len() != 5 {
                        continue;
                    }
                    let op = split[1].parse().unwrap();
                    let offset: usize = split[2].parse().unwrap();
                    let size: usize = split[3].parse().unwrap();
                    return Some(Trace { offset, size, op });
                }
                Err(_) => None,
            };
        }
    }
}

impl super::TraceParser for AliTraceParser {}

#[test]
fn test_alitrace() {
    use super::Operation;
    let dir = tempfile::tempdir().unwrap();
    let f_path = {
        let mut dir = dir.path().to_owned();
        dir.push("TEST.csv");
        dir
    };
    let mut f = std::fs::File::create(f_path.as_path()).unwrap();
    const FILE_DATA: &'static str = "0,W,99243462656,4096,1577808000218468

0,R,101600702464,4096,1577808000218510


0,W,275691458560,1024,1577808000218545

1,4

    ";
    std::io::Write::write_all(&mut f, FILE_DATA.as_bytes()).unwrap();
    drop(f);
    let mut parser = AliTraceParser::open(f_path.as_path()).unwrap();
    assert_eq!(
        parser.next(),
        Some(Trace {
            offset: 99243462656,
            size: 4096,
            op: Operation::Write
        })
    );
    assert_eq!(
        parser.next(),
        Some(Trace {
            offset: 101600702464,
            size: 4096,
            op: Operation::Read
        })
    );
    assert_eq!(
        parser.next(),
        Some(Trace {
            offset: 275691458560,
            size: 1024,
            op: Operation::Write
        })
    );
    assert_eq!(parser.next(), None);
}
