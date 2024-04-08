use smallvec::SmallVec;

pub mod coordinator;
pub mod worker;

mod messages;

#[derive(Debug, PartialEq, Eq, Clone)]
struct Ranges(range_collections::RangeSet<[usize; 2]>);

impl Default for Ranges {
    fn default() -> Self {
        Self::empty()
    }
}

impl Ranges {
    /// Make an empty range
    fn empty() -> Self {
        Self(range_collections::RangeSet::empty())
    }

    /// Get a vector of existing ranges
    fn to_ranges(&self) -> Vec<std::ops::Range<usize>> {
        self.0
            .boundaries()
            .chunks_exact(2)
            .map(|bound| bound[0]..bound[1])
            .collect()
    }

    /// Get the total length of the existing ranges.
    fn len(&self) -> usize {
        self.0
            .boundaries()
            .chunks_exact(2)
            .map(|bound| bound[1] - bound[0])
            .sum()
    }
}

impl serde::Serialize for Ranges {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let bounds = self.0.boundaries();
        bounds.serialize(serializer)
    }
}

impl<'de> serde::Deserialize<'de> for Ranges {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        use serde::de::Error;
        let boundaries: SmallVec<[usize; 2]> = serde::de::Deserialize::deserialize(deserializer)?;
        Ok(Ranges(
            range_collections::RangeSet::new(boundaries)
                .ok_or_else(|| Error::custom("invalid boundaries"))?,
        ))
    }
}

#[derive(Debug, Clone, Copy, serde::Serialize, serde::Deserialize)]
struct WorkerID(usize);

impl std::fmt::Display for WorkerID {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

type MessageQueueKey = String;

fn progress_style_template(msg: Option<&str>) -> indicatif::ProgressStyle {
    match msg {
        Some(msg) => indicatif::ProgressStyle::with_template(
            format!("{msg} [{{elapsed_precise}}] {{bar:30.white}} {{pos:>7}}/{{len:7}}").as_str(),
        )
        .unwrap(),
        None => indicatif::ProgressStyle::with_template(
            "[{elapsed_precise}] {bar:30.cyan/blue} {pos:>7}/{len:7}",
        )
        .unwrap(),
    }
}

fn dev_display(dev: &std::path::Path) -> String {
    let mut display = dev.display().to_string();
    if dev.is_symlink() {
        display += format!(" -> {}", std::fs::read_link(dev).unwrap().display()).as_str();
    }
    display
}

fn format_request_queue_key(id: WorkerID) -> MessageQueueKey {
    format!("c-{}", id.0)
}

fn format_response_queue_key() -> MessageQueueKey {
    "w-0".to_string()
}

fn _parse_request_queue_key(key: &MessageQueueKey) -> Option<WorkerID> {
    if key.starts_with("c-") {
        key[2..].parse().ok().map(WorkerID)
    } else {
        None
    }
}
