use std::str::FromStr;

use crate::SUError;

mod ali_trace;

pub use ali_trace::AliTraceParser;

#[derive(Debug, PartialEq, Eq)]
pub enum Operation {
    Read,
    Write,
}

impl FromStr for Operation {
    type Err = SUError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "W" | "w" => Ok(Operation::Write),
            "R" | "r" => Ok(Operation::Read),
            _ => Err(SUError::invalid_arg("invalid str to parse to operation")),
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
pub struct Trace {
    pub offset: usize,
    pub size: usize,
    pub op: Operation,
}

pub trait TraceParser: Iterator<Item = Trace> {}
