//! Consumer program, which runs the poller loop and dispatches the polled records to Elixir.
//!
//! Elixir sends back acks, which are used for backpressure and commits. See
//! `KafkaClient.Consumer.Poller` for the description of the behaviour.

mod backpressure;
mod commits;

use crate::driver::{Command, Commands};
use crate::etf;
use crate::output::Output;
use crate::term::{Value, atom, list, topic_partition, tuple};
use async_trait::async_trait;
use backpressure::Backpressure;
use bytes::Bytes;
use commits::Commits;
use confluent_kafka::common::header::{Header, Headers};
use confluent_kafka::common::serialization::BytesDeserializer;
use confluent_kafka::common::{Error, TopicPartition};
use confluent_kafka::consumer::{
    Consumer, ConsumerConfig, ConsumerHandle, ConsumerRebalanceListener, ConsumerRecord,
    ConsumerRecords, KafkaConsumer,
};
use eetf::Term;
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

// Records hold `Bytes` to avoid copying the payloads, which are encoded straight from the fetched
// data into the output buffer.
type Client = KafkaConsumer<Bytes, Bytes>;
type Record = ConsumerRecord<Bytes, Bytes>;

const DEFAULT_POLL_DURATION_MS: u64 = 10;
const DEFAULT_COMMIT_INTERVAL_MS: u64 = 5000;
const OFFSET_POSITION: i64 = 0;
const TIMESTAMP_POSITION: i64 = 1;

/// Polled records are sent to Elixir in chunks of this size, instead of waiting for the entire
/// poll to be encoded, so Elixir can start processing as soon as possible.
const CHUNK_SIZE_BYTES: usize = 64 * 1024;

/// Settings of the classic group protocol. With KIP-848 these are managed by the broker, and the
/// client refuses to start if they are provided.
const CLASSIC_PROTOCOL_PARAMS: [&str; 3] = [
    "heartbeat.interval.ms",
    "session.timeout.ms",
    "partition.assignment.strategy",
];

/// Java class settings, which have no meaning in this client.
const JAVA_CLASS_PARAMS: [&str; 2] = ["key.deserializer", "value.deserializer"];

struct Subscription {
    topic: String,
    partition: i32,
    position_type: Option<i64>,
    position: Option<i64>,
}

/// State which is used by both the poller loop and the rebalance listener.
struct State {
    commits: Commits,
    backpressure: Backpressure,
}

type SharedState = Arc<Mutex<State>>;

fn lock(state: &SharedState) -> MutexGuard<'_, State> {
    state.lock().expect("state lock poisoned")
}

pub async fn run(mut commands: Commands, output: Output, args: Vec<Value>) -> Result<(), Error> {
    let params = client_params(args[0].to_config());
    let is_anonymous = !params.contains_key("group.id");
    let subscriptions: Vec<Subscription> = args[1].as_list().iter().map(subscription).collect();

    let poll_duration = poller_param(&args[2], "poll_duration", DEFAULT_POLL_DURATION_MS);
    let commit_interval = poller_param(&args[2], "commit_interval", DEFAULT_COMMIT_INTERVAL_MS);

    let config = ConsumerConfig::new(&params)?;
    let mut consumer = Client::new(config, BytesDeserializer::new(), BytesDeserializer::new())?;

    let state = Arc::new(Mutex::new(State {
        commits: Commits::new(Duration::from_millis(commit_interval)),
        backpressure: Backpressure::default(),
    }));

    // Offsets which must be reached before the `caught_up` notification is emitted. Used only by
    // anonymous consumers.
    let mut end_offsets = None;

    if is_anonymous {
        end_offsets = Some(start_anonymous(&mut consumer, &output, &subscriptions).await?);
        maybe_emit_caught_up(&mut end_offsets, &output);
    } else {
        let listener = Arc::new(Listener {
            consumer: consumer.handle(),
            state: Arc::clone(&state),
            output: output.clone(),
        });

        let topics = distinct_topics(&subscriptions);
        consumer
            .subscribe_with_topics_listener(topics, listener)
            .await?;
    }

    let poll_duration = Duration::from_millis(poll_duration);

    loop {
        // commands issued by Elixir, such as ack or stop
        while let Ok(command) = commands.try_recv() {
            match command.name.as_str() {
                "ack" => ack(&command, &state, is_anonymous, &mut end_offsets, &output),
                "stop" => {
                    if !is_anonymous {
                        flush_commits(&mut consumer, &state, true).await?;
                    }

                    return consumer.close().await;
                }
                other => panic!("unknown command {other}"),
            }
        }

        // Backpressure and commits are collected while handling Elixir commands. Now we're
        // flushing the final state (pauses and commits).
        flush_backpressure(&mut consumer, &state).await?;

        if !is_anonymous {
            flush_commits(&mut consumer, &state, false).await?;
        }

        let records = match consumer.poll(poll_duration).await {
            Ok(records) => records,
            Err(e) if e.is_retriable_error() => continue,
            Err(e) => return Err(e),
        };

        emit_records(&records, &state, &output).await;
    }
}

async fn emit_records(
    records: &ConsumerRecords<Bytes, Bytes>,
    state: &SharedState,
    output: &Output,
) {
    let mut chunk = Vec::with_capacity(CHUNK_SIZE_BYTES);

    for partition in records.partitions() {
        let partition_records = records.records_with_partition(partition);
        let sizes = partition_records
            .iter()
            .map(|record| record.value().map_or(0, Bytes::len));
        lock(state).backpressure.records_polled(partition, sizes);

        for record in partition_records {
            encode_record(&mut chunk, record);

            if chunk.len() >= CHUNK_SIZE_BYTES {
                let full_chunk =
                    std::mem::replace(&mut chunk, Vec::with_capacity(CHUNK_SIZE_BYTES));
                output.emit_records(full_chunk).await;
            }
        }
    }

    if !chunk.is_empty() {
        output.emit_records(chunk).await;
    }
}

fn client_params(mut params: HashMap<String, String>) -> HashMap<String, String> {
    for param in CLASSIC_PROTOCOL_PARAMS {
        if params.remove(param).is_some() {
            eprintln!(
                "kafka_client_port: ignoring `{param}`, which is not supported by the KIP-848 \
                 group protocol"
            );
        }
    }

    for param in JAVA_CLASS_PARAMS {
        params.remove(param);
    }

    // this is the only protocol supported by the client, even if the consumer is anonymous
    params.insert("group.protocol".to_string(), "consumer".to_string());
    params
}

fn poller_param(params: &Value, name: &str, default: u64) -> u64 {
    params.get(name).opt_i64().map_or(default, |value| {
        u64::try_from(value).expect("negative poller param")
    })
}

fn subscription(value: &Value) -> Subscription {
    let fields = value.as_list();

    Subscription {
        topic: fields[0].as_str().to_string(),
        partition: fields[1].as_i32(),
        position_type: fields[2].opt_i64(),
        position: fields[3].opt_i64(),
    }
}

fn distinct_topics(subscriptions: &[Subscription]) -> Vec<String> {
    let topics: HashSet<&String> = subscriptions.iter().map(|s| &s.topic).collect();
    topics.into_iter().cloned().collect()
}

fn ack(
    command: &Command,
    state: &SharedState,
    is_anonymous: bool,
    end_offsets: &mut Option<HashMap<TopicPartition, i64>>,
    output: &Output,
) {
    let mut state = lock(state);

    for record in command.args[0].as_list() {
        let fields = record.as_list();
        let partition = TopicPartition::new(fields[0].as_str(), fields[1].as_i32());
        let offset = fields[2].as_i64();

        state.backpressure.record_processed(&partition);

        if !is_anonymous {
            state.commits.add(partition, offset);
        } else if let Some(offsets) = end_offsets {
            if offsets.get(&partition).is_some_and(|end| end - 1 <= offset) {
                offsets.remove(&partition);
            }

            maybe_emit_caught_up(end_offsets, output);
        }
    }
}

fn maybe_emit_caught_up(end_offsets: &mut Option<HashMap<TopicPartition, i64>>, output: &Output) {
    if end_offsets.as_ref().is_some_and(HashMap::is_empty) {
        output.emit(atom("caught_up"));
        *end_offsets = None;
    }
}

async fn flush_backpressure(consumer: &mut Client, state: &SharedState) -> Result<(), Error> {
    let assignment = consumer.assignment();
    let paused = lock(state).backpressure.retain_assigned(&assignment);

    if !paused.is_empty() {
        consumer.pause(&paused).await?;
    }

    // This is equivalent to resuming every unpaused partition, as the Java port does, but it
    // avoids a round trip to the background thread on each iteration of the poller loop.
    let desired: HashSet<&TopicPartition> = paused.iter().collect();
    let resumed: Vec<TopicPartition> = consumer
        .paused()
        .into_iter()
        .filter(|partition| !desired.contains(partition))
        .collect();

    if !resumed.is_empty() {
        consumer.resume(&resumed).await?;
    }

    Ok(())
}

async fn flush_commits(
    consumer: &mut Client,
    state: &SharedState,
    sync: bool,
) -> Result<(), Error> {
    let assignment = consumer.assignment();

    let Some(offsets) = lock(state).commits.take_due(&assignment) else {
        return Ok(());
    };

    if sync {
        consumer.commit_sync_with_offsets(offsets).await
    } else {
        consumer
            .commit_async_with_offsets_callback(offsets, Arc::new(commits::LogFailure))
            .await
    }
}

/// Assigns the requested partitions, and moves to the requested positions. Returns the end offsets
/// of partitions which have records that must be processed to catch up.
async fn start_anonymous(
    consumer: &mut Client,
    output: &Output,
    subscriptions: &[Subscription],
) -> Result<HashMap<TopicPartition, i64>, Error> {
    let assignments = assign_partitions(consumer, subscriptions).await?;
    seek_to_positions(consumer, subscriptions).await?;

    // Rebalance callbacks are not invoked on manual assignment, so the notification is emitted
    // here, to keep the behaviour consistent with consumer groups.
    output.emit(rebalance_event("assigned", &assignments));

    let mut end_offsets = HashMap::new();

    for (partition, end_offset) in consumer.end_offsets(&assignments).await? {
        if end_offset != consumer.position(&partition).await? {
            end_offsets.insert(partition, end_offset);
        }
    }

    Ok(end_offsets)
}

async fn assign_partitions(
    consumer: &mut Client,
    subscriptions: &[Subscription],
) -> Result<Vec<TopicPartition>, Error> {
    let mut assignments = Vec::new();

    for subscription in subscriptions {
        if subscription.partition >= 0 {
            assignments.push(TopicPartition::new(
                subscription.topic.as_str(),
                subscription.partition,
            ));
        } else {
            assignments.extend(all_partitions(consumer, &subscription.topic).await?);
        }
    }

    consumer.assign(assignments.clone()).await?;
    Ok(assignments)
}

async fn all_partitions(consumer: &mut Client, topic: &str) -> Result<Vec<TopicPartition>, Error> {
    let partitions = consumer.partitions_for(topic).await?;
    Ok(partitions
        .iter()
        .map(|info| TopicPartition::new(topic, info.partition()))
        .collect())
}

async fn subscribed_partitions(
    consumer: &mut Client,
    subscription: &Subscription,
) -> Result<Vec<TopicPartition>, Error> {
    if subscription.partition >= 0 {
        Ok(vec![TopicPartition::new(
            subscription.topic.as_str(),
            subscription.partition,
        )])
    } else {
        all_partitions(consumer, &subscription.topic).await
    }
}

async fn seek_to_positions(
    consumer: &mut Client,
    subscriptions: &[Subscription],
) -> Result<(), Error> {
    let mut timestamps = HashMap::new();

    for subscription in subscriptions {
        let (Some(position_type), Some(position)) =
            (subscription.position_type, subscription.position)
        else {
            continue;
        };

        for partition in subscribed_partitions(consumer, subscription).await? {
            match position_type {
                OFFSET_POSITION => consumer.seek_with_offset(partition, position).await?,
                TIMESTAMP_POSITION => {
                    timestamps.insert(partition, position);
                }
                other => panic!("unknown position type {other}"),
            }
        }
    }

    if !timestamps.is_empty() {
        for (partition, offset) in consumer.offsets_for_times(timestamps).await? {
            if let Some(offset) = offset {
                consumer
                    .seek_with_offset(partition, offset.offset())
                    .await?;
            }
        }
    }

    Ok(())
}

/// Appends the `{:record, topic, partition, offset, timestamp, headers, key, value}` message.
fn encode_record(buffer: &mut Vec<u8>, record: &Record) {
    etf::frame(buffer, |buffer| {
        etf::tuple_header(buffer, 8);
        etf::atom(buffer, "record");
        etf::binary(buffer, record.topic().as_bytes());
        etf::integer(buffer, record.partition().into());
        etf::integer(buffer, record.offset());
        etf::integer(buffer, record.timestamp());
        encode_headers(buffer, record);
        etf::optional_binary(buffer, record.key().map(Bytes::as_ref));
        etf::optional_binary(buffer, record.value().map(Bytes::as_ref));
    });
}

fn encode_headers(buffer: &mut Vec<u8>, record: &Record) {
    let count = record.headers().iter().count();

    if count == 0 {
        etf::nil_list(buffer);
        return;
    }

    etf::list_header(buffer, count);

    for header in record.headers().iter() {
        etf::tuple_header(buffer, 2);
        etf::binary(buffer, header.key().as_bytes());
        etf::optional_binary(buffer, header.value());
    }

    etf::nil_list(buffer);
}

fn rebalance_event(event: &str, partitions: &[TopicPartition]) -> Term {
    let partitions = partitions
        .iter()
        .map(|partition| topic_partition(partition.topic(), partition.partition()))
        .collect();

    tuple(vec![atom(event), list(partitions)])
}

struct Listener {
    consumer: ConsumerHandle,
    state: SharedState,
    output: Output,
}

#[async_trait]
impl ConsumerRebalanceListener for Listener {
    async fn on_partitions_assigned(&self, partitions: &[TopicPartition]) -> Result<(), Error> {
        self.output.emit(rebalance_event("assigned", partitions));
        Ok(())
    }

    async fn on_partitions_revoked(&self, partitions: &[TopicPartition]) -> Result<(), Error> {
        let offsets = lock(&self.state).commits.take_revoked(partitions);

        // We're losing the partitions, but there's still time to commit. A failure here is not
        // tragic, it means that some records will be processed again, so we keep going.
        if !offsets.is_empty() {
            let timeout = Duration::from_secs(5);
            let _ = self
                .consumer
                .commit_sync_with_offsets_timeout(offsets, timeout)
                .await;
        }

        lock(&self.state).backpressure.remove_partitions(partitions);
        self.output.emit(rebalance_event("unassigned", partitions));
        Ok(())
    }

    async fn on_partitions_lost(&self, partitions: &[TopicPartition]) -> Result<(), Error> {
        // the partitions are already gone, so we can't commit
        let mut state = lock(&self.state);
        state.commits.remove(partitions);
        state.backpressure.remove_partitions(partitions);
        drop(state);

        self.output.emit(rebalance_event("unassigned", partitions));
        Ok(())
    }
}
