#[derive(Debug, thiserror::Error)]
pub enum SUError {
    #[error("[kind: io, info:{0}]")]
    Io(#[from] std::io::Error),
    #[error("[kind: invalid argument, info:{0}]")]
    InvalidArg(String),
    #[error("[kind: out of range, info:{0}]")]
    Range(String),
    #[error("[kind: erasure code, info:{0}]")]
    ErasureCode(String),
    #[error("[kind: other, info: {0}]")]
    Other(String),
}

impl SUError {
    pub(crate) fn invalid_arg(e: impl ToString) -> Self {
        Self::InvalidArg(e.to_string())
    }

    pub(crate) fn other(e: impl ToString) -> Self {
        Self::Other(e.to_string())
    }

    pub(crate) fn out_of_range(
        source_location: impl ToString,
        valid_range: std::ops::Range<usize>,
        illegal_range: std::ops::Range<usize>,
    ) -> Self {
        Self::Range(format!(
            "error: {{[{}..{}) is out of range [{}..{})}}, at: {{[{}]}}",
            illegal_range.start,
            illegal_range.end,
            valid_range.start,
            valid_range.end,
            source_location.to_string()
        ))
    }

    pub(crate) fn range_not_match(
        source_location: impl ToString,
        valid_range: std::ops::Range<usize>,
        illegal_range: std::ops::Range<usize>,
    ) -> Self {
        Self::Range(format!(
            "error: {{[{}..{}) does not match range [{}..{})}}, at: {{[{}]}}",
            illegal_range.start,
            illegal_range.end,
            valid_range.start,
            valid_range.end,
            source_location.to_string()
        ))
    }

    pub(crate) fn erasure_code(source_location: (&str, u32, u32), errstr: impl ToString) -> Self {
        Self::ErasureCode(format!(
            "error: {{{}}}, at: {{{}:{}:{}}}",
            errstr.to_string(),
            source_location.0,
            source_location.1,
            source_location.2
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
