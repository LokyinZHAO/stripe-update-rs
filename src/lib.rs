pub mod config;
pub mod erasure_code;
pub mod storage;
pub mod trace_parser;

#[derive(Debug, thiserror::Error)]
pub enum SUError {
    #[error("[kind: io, info:{0}]")]
    Io(#[from] std::io::Error),
    #[error("[kind: invalid argument, info:{0}]")]
    InvalidArg(String),
    #[error("[kind: out of range, info:{0}]")]
    Range(String),
    #[error("[kind: uncategorized, info: {0}")]
    Uncategorized(Box<dyn std::error::Error>),
    #[error("[kind: other, info: {0}]")]
    Other(String),
}

impl SUError {
    pub fn invalid_arg(e: impl ToString) -> Self {
        Self::InvalidArg(e.to_string())
    }

    pub fn uncategorized(e: Box<dyn std::error::Error>) -> Self {
        Self::Uncategorized(e)
    }

    pub fn other(e: impl ToString) -> Self {
        Self::Other(e.to_string())
    }

    pub fn out_of_range(
        obj_name: &str,
        valid_range: std::ops::Range<usize>,
        illegal_range: std::ops::Range<usize>,
    ) -> Self {
        Self::Range(format!(
            "{obj_name}[{}..{}) is out of range [{}..{})",
            illegal_range.start, illegal_range.end, valid_range.start, valid_range.end
        ))
    }

    pub fn range_not_match(
        obj_name: &str,
        valid_range: std::ops::Range<usize>,
        illegal_range: std::ops::Range<usize>,
    ) -> Self {
        Self::Range(format!(
            "{obj_name}[{}..{}) does not match range [{}..{})",
            illegal_range.start, illegal_range.end, valid_range.start, valid_range.end
        ))
    }

    pub fn into_io_err(self) -> Option<std::io::Error> {
        if let SUError::Io(io_err) = self {
            Some(io_err)
        } else {
            None
        }
    }
}

pub type SUResult<T> = std::result::Result<T, SUError>;
