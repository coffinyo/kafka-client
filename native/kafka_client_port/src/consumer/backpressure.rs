//! Tracks the records sent to Elixir which are not yet processed, and decides which partitions
//! must be paused. Records are kept in one queue per partition. If a queue becomes large (in
//! length or in total byte size) the partition is paused, and it's resumed once the queue is small
//! enough.

use confluent_kafka::common::TopicPartition;
use std::collections::{HashMap, HashSet, VecDeque};

const PAUSE_THRESHOLD_BYTES: usize = 1_000_000;
const PAUSE_THRESHOLD_MESSAGES: usize = 1000;
const RESUME_THRESHOLD_BYTES: usize = 500_000;
const RESUME_THRESHOLD_MESSAGES: usize = 500;

#[derive(Default)]
pub struct Backpressure {
    paused: HashSet<TopicPartition>,
    queues: HashMap<TopicPartition, Queue>,
}

impl Backpressure {
    pub fn records_polled(
        &mut self,
        partition: &TopicPartition,
        sizes: impl IntoIterator<Item = usize>,
    ) {
        let queue = self.queues.entry(partition.clone()).or_default();
        sizes.into_iter().for_each(|size| queue.record_polled(size));

        if queue.should_pause() {
            self.paused.insert(partition.clone());
        }
    }

    pub fn record_processed(&mut self, partition: &TopicPartition) {
        if let Some(queue) = self.queues.get_mut(partition) {
            queue.record_processed();

            if queue.should_resume() {
                self.paused.remove(partition);
            }
        }
    }

    /// Forgets partitions which are not assigned, and returns the ones which must stay paused.
    ///
    /// A paused partition stays in this set until its queue drains. The caller pauses them all
    /// before every poll, which ensures that a partition remains paused after a rebalance.
    pub fn retain_assigned(&mut self, assignment: &HashSet<TopicPartition>) -> Vec<TopicPartition> {
        self.queues
            .retain(|partition, _queue| assignment.contains(partition));
        self.paused
            .retain(|partition| assignment.contains(partition));
        self.paused.iter().cloned().collect()
    }

    pub fn remove_partitions(&mut self, partitions: &[TopicPartition]) {
        for partition in partitions {
            self.paused.remove(partition);
        }
    }
}

#[derive(Default)]
struct Queue {
    sizes: VecDeque<usize>,
    total_bytes: usize,
}

impl Queue {
    fn record_polled(&mut self, size: usize) {
        self.sizes.push_back(size);
        self.total_bytes += size;
    }

    fn record_processed(&mut self) {
        if let Some(size) = self.sizes.pop_front() {
            self.total_bytes -= size;
        }
    }

    fn should_pause(&self) -> bool {
        self.sizes.len() >= 2
            && (self.total_bytes >= PAUSE_THRESHOLD_BYTES
                || self.sizes.len() >= PAUSE_THRESHOLD_MESSAGES)
    }

    fn should_resume(&self) -> bool {
        self.sizes.len() < 2
            || (self.total_bytes <= RESUME_THRESHOLD_BYTES
                && self.sizes.len() <= RESUME_THRESHOLD_MESSAGES)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn partition() -> TopicPartition {
        TopicPartition::new("topic", 0)
    }

    fn assignment() -> HashSet<TopicPartition> {
        HashSet::from([partition()])
    }

    #[test]
    fn pauses_on_message_count_and_resumes_at_half() {
        let mut backpressure = Backpressure::default();

        backpressure.records_polled(&partition(), vec![1; PAUSE_THRESHOLD_MESSAGES]);

        assert_eq!(
            backpressure.retain_assigned(&assignment()),
            vec![partition()]
        );

        for _ in 0..(PAUSE_THRESHOLD_MESSAGES - RESUME_THRESHOLD_MESSAGES - 1) {
            backpressure.record_processed(&partition());
        }

        assert_eq!(
            backpressure.retain_assigned(&assignment()),
            vec![partition()]
        );

        backpressure.record_processed(&partition());
        assert!(backpressure.retain_assigned(&assignment()).is_empty());
    }

    #[test]
    fn a_single_large_message_does_not_pause() {
        let mut backpressure = Backpressure::default();
        backpressure.records_polled(&partition(), [PAUSE_THRESHOLD_BYTES * 2]);
        assert!(backpressure.retain_assigned(&assignment()).is_empty());

        backpressure.records_polled(&partition(), [1]);
        assert_eq!(
            backpressure.retain_assigned(&assignment()),
            vec![partition()]
        );
    }

    #[test]
    fn forgets_unassigned_partitions() {
        let mut backpressure = Backpressure::default();

        backpressure.records_polled(&partition(), vec![1; PAUSE_THRESHOLD_MESSAGES]);

        assert!(backpressure.retain_assigned(&HashSet::new()).is_empty());
    }
}
