//! Producer program. See `KafkaClient.Producer`.

use crate::driver::{Command, Commands};
use crate::output::Output;
use crate::term::{Value, atom, bytes, error, float, integer, list, map, nil, ok, string, tuple};
use confluent_kafka::common::header::{RecordHeader, RecordHeaders};
use confluent_kafka::common::serialization::ByteArraySerializer;
use confluent_kafka::common::{Error, Metric, MetricValue};
use confluent_kafka::producer::{
    KafkaProducer, Producer, ProducerConfig, ProducerRecord, ProducerRecordOptionsBuilder,
    RecordMetadata, callback,
};
use eetf::Term;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

type Client = KafkaProducer<Vec<u8>, Vec<u8>>;

/// Java class settings, which have no meaning in this client.
const JAVA_CLASS_PARAMS: [&str; 2] = ["key.serializer", "value.serializer"];

/// The Rust client hashes keys with CRC32 by default. The Java client uses murmur2, so we default
/// to murmur2 to keep each key on the same partition as before.
const JAVA_COMPATIBLE_PARTITIONER: &str = "Murmur2RandomPartitioner";

pub async fn run(mut commands: Commands, output: Output, args: Vec<Value>) -> Result<(), Error> {
    let config = ProducerConfig::new(&client_params(args[0].to_config()))?;
    let producer = Client::new(
        config,
        ByteArraySerializer::new(),
        ByteArraySerializer::new(),
    )?;

    while let Some(command) = commands.recv().await {
        match command.name.as_str() {
            "send" => send(&producer, &output, command).await,
            "partitions_for" => {
                let response = partitions_for(&producer, &command).await;
                output.emit_call_response(&command, response);
            }
            "metrics" => output.emit_call_response(&command, ok(metrics(&producer))),
            "stop" => break,
            other => panic!("unknown command {other}"),
        }
    }

    producer.flush().await?;
    producer.close().await
}

fn client_params(mut params: HashMap<String, String>) -> HashMap<String, String> {
    for param in JAVA_CLASS_PARAMS {
        params.remove(param);
    }

    if params.remove("partitioner.class").is_some() {
        eprintln!("kafka_client_port: `partitioner.class` is not supported, ignoring it");
    }

    params
        .entry("partitioner.type".to_string())
        .or_insert_with(|| JAVA_COMPATIBLE_PARTITIONER.to_string());

    params
}

async fn send(producer: &Client, output: &Output, command: Command) {
    let mut args = command.args.into_iter();
    let record = args.next().expect("missing record");
    let callback_id = args.next().and_then(Value::into_bytes);

    let completed = Arc::new(AtomicBool::new(false));

    let on_completion = callback_id.clone().map(|id| {
        let output = output.clone();
        let completed = Arc::clone(&completed);

        Box::new(callback::new(move |metadata, error| {
            completed.store(true, Ordering::SeqCst);
            output.emit(completion(&id, delivery_result(metadata, error)));
        })) as Box<dyn callback::Callback>
    });

    let result = match build_record(record) {
        Ok(record) => producer
            .send_with_callback(record, on_completion)
            .await
            .map(|_| ()),
        Err(e) => Err(e),
    };

    if let Err(e) = result {
        match callback_id {
            // Elixir tracks the size of the record until the completion, so it must always arrive.
            Some(id) if !completed.load(Ordering::SeqCst) => {
                output.emit(completion(&id, delivery_error(&e)));
            }
            _ => eprintln!("kafka_client_port: send failed: {e}"),
        }
    }
}

fn completion(id: &[u8], payload: Term) -> Term {
    tuple(vec![atom("on_completion"), bytes(id.to_vec()), payload])
}

fn delivery_result(metadata: Option<&RecordMetadata>, error: Option<&Error>) -> Term {
    // On failure the metadata is present, but it holds sentinel values, so the error goes first.
    if let Some(error) = error {
        return delivery_error(error);
    }

    let metadata = metadata.expect("delivery succeeded without metadata");

    tuple(vec![
        atom("ok"),
        integer(metadata.partition().into()),
        integer(metadata.offset()),
        integer(metadata.timestamp()),
    ])
}

fn delivery_error(e: &Error) -> Term {
    if e.is_timeout_error() || e.is_local_timeout_error() {
        tuple(vec![atom("error"), atom("timeout")])
    } else {
        error(e.message())
    }
}

fn build_record(mut record: Value) -> Result<ProducerRecord<Vec<u8>, Vec<u8>>, Error> {
    let headers: Vec<RecordHeader> = record
        .take("headers")
        .into_list()
        .into_iter()
        .map(|header| {
            let mut header = header.into_list().into_iter();
            let key = header.next().expect("missing header key");
            let value = header.next().expect("missing header value");
            RecordHeader::new(key.as_str().to_string(), value.into_bytes())
        })
        .collect();

    let options = ProducerRecordOptionsBuilder::new()
        .set_topic(record.get("topic").as_str().to_string())
        .set_partition(record.get("partition").opt_i32())
        .set_timestamp(record.get("timestamp").opt_i64())
        .set_key(record.take("key").into_bytes())
        .set_value(record.take("value").into_bytes())
        .set_headers(Some(RecordHeaders::with_header_iter(headers)))
        .build()?;

    ProducerRecord::with_options(options).map_err(Error::LocalIllegalArgument)
}

async fn partitions_for(producer: &Client, command: &Command) -> Term {
    match producer.partitions_for(command.args[0].as_str()).await {
        Ok(partitions) => ok(list(
            partitions
                .iter()
                .map(|p| integer(p.partition().into()))
                .collect(),
        )),
        Err(e) => error(e.message()),
    }
}

fn metrics(producer: &Client) -> Term {
    let entries = producer
        .metrics()
        .snapshot()
        .iter()
        .map(|(name, metric)| {
            let value = match metric.metric_value() {
                MetricValue::Double(value) => float(value),
                MetricValue::Long(value) => integer(value),
                MetricValue::Int(value) => integer(value.into()),
                MetricValue::String(value) => string(&value),
                #[allow(unreachable_patterns)]
                _ => nil(),
            };

            (string(name.name()), value)
        })
        .collect();

    map(entries)
}
