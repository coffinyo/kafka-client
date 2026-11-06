//! Aggregates acknowledged offsets, which the poller periodically commits to Kafka.

use async_trait::async_trait;
use confluent_kafka::common::{Error, TopicPartition};
use confluent_kafka::consumer::{OffsetAndMetadata, OffsetCommitCallback};
use std::collections::{HashMap, HashSet};
use std::time::{Duration, Instant};

type Offsets = HashMap<TopicPartition, OffsetAndMetadata>;

pub struct Commits {
    pending: HashMap<TopicPartition, i64>,
    interval: Duration,
    last_commit: Option<Instant>,
}

impl Commits {
    pub fn new(interval: Duration) -> Self {
        Self {
            pending: HashMap::new(),
            interval,
            last_commit: None,
        }
    }

    pub fn add(&mut self, partition: TopicPartition, offset: i64) {
        self.pending.insert(partition, offset + 1);
    }

    /// Returns the offsets to commit, if the commit interval has elapsed and there are any.
    pub fn take_due(&mut self, assignment: &HashSet<TopicPartition>) -> Option<Offsets> {
        let now = Instant::now();

        if self
            .last_commit
            .is_some_and(|last| now - last < self.interval)
        {
            return None;
        }

        self.pending
            .retain(|partition, _offset| assignment.contains(partition));

        if self.pending.is_empty() {
            return None;
        }

        self.last_commit = Some(now);
        Some(to_offsets(self.pending.drain()))
    }

    /// Returns the offsets of the partitions which are being revoked.
    pub fn take_revoked(&mut self, partitions: &[TopicPartition]) -> Offsets {
        to_offsets(
            partitions
                .iter()
                .filter_map(|partition| self.pending.remove_entry(partition)),
        )
    }

    pub fn remove(&mut self, partitions: &[TopicPartition]) {
        for partition in partitions {
            self.pending.remove(partition);
        }
    }
}

fn to_offsets(offsets: impl Iterator<Item = (TopicPartition, i64)>) -> Offsets {
    offsets
        .map(|(partition, offset)| {
            let offset = OffsetAndMetadata::new(offset).expect("offsets are never negative");
            (partition, offset)
        })
        .collect()
}

/// Commit failures are not fatal, the offsets are covered by the next commit, or the records are
/// processed again.
pub struct LogFailure;

#[async_trait]
impl OffsetCommitCallback for LogFailure {
    async fn on_complete(&self, _offsets: &Offsets, error: Option<&Error>) -> Result<(), Error> {
        if let Some(error) = error {
            eprintln!("kafka_client_port: commit failed: {error}");
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn partition(number: i32) -> TopicPartition {
        TopicPartition::new("topic", number)
    }

    fn assignment() -> HashSet<TopicPartition> {
        HashSet::from([partition(0), partition(1)])
    }

    #[test]
    fn commits_the_offset_following_the_last_ack() {
        let mut commits = Commits::new(Duration::ZERO);
        commits.add(partition(0), 4);
        commits.add(partition(0), 5);

        let offsets = commits.take_due(&assignment()).unwrap();
        assert_eq!(offsets[&partition(0)].offset(), 6);
        assert!(commits.take_due(&assignment()).is_none());
    }

    #[test]
    fn respects_the_interval() {
        let mut commits = Commits::new(Duration::from_secs(60));
        commits.add(partition(0), 1);
        assert!(commits.take_due(&assignment()).is_some());

        commits.add(partition(0), 2);
        assert!(commits.take_due(&assignment()).is_none());
    }

    #[test]
    fn drops_unassigned_partitions() {
        let mut commits = Commits::new(Duration::ZERO);
        commits.add(partition(5), 1);
        assert!(commits.take_due(&assignment()).is_none());
    }

    #[test]
    fn revoked_offsets_are_taken_once() {
        let mut commits = Commits::new(Duration::ZERO);
        commits.add(partition(0), 1);
        commits.add(partition(1), 1);

        assert_eq!(commits.take_revoked(&[partition(0)]).len(), 1);
        assert!(commits.take_revoked(&[partition(0)]).is_empty());
        assert_eq!(commits.take_due(&assignment()).unwrap().len(), 1);
    }
}
