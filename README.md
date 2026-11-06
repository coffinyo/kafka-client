# KafkaClient

Elixir wrapper around an official Kafka client. Two backends are available: the [Apache Kafka Java client](https://javadoc.io/doc/org.apache.kafka/kafka-clients/latest/index.html), which is the default, and the [Confluent Rust client](https://github.com/confluentinc/kafka-clients/tree/master/rust), which removes the need for a JVM. See [Backends](#backends).

This wrapper is created to overcome deficiencies in the existing BEAM Kafka clients. Adapting those clients was considered, but it was estimated that implementing some of the features would be tricky to implement and maintain, since Kafka delegates a lot of complexity to the client, especially in the consumer part.

Therefore, this library takes a different approach. Instead of spending a lot of effort working on the BEAM client, we reuse the official Kafka client, which is developed, tested, and released together with Kafka itself. This gives us the easy access to the latest & greatest features without imposing a high development and maintenance burden.

## Features

- exposes the official client API to Elixir
- finer-grained concurrency on top of the consumer API
- at least once processing guarantees
- batching and backpressure
- topic consuming (consume all partitions of a topic)
- draining on shutdown

## Downsides

- only a small portion of API is exposed
- requires a Java runtime, unless the Rust backend is used
- adds a latency overhead due to extra hop between the port program and BEAM
- still in early days, so bugs may exist, and the API might change radically

## Usage

The required tools and versions are specified in the .tool-versions file. You can use the [asdf version manager](https://asdf-vm.com/) to install them. You can also use newer versions in your own projects (unless there are some breaking changes).

You also need to run Kafka. You can start it with the included `docker-compose.yml` file.

This library is currently not published to hex.pm, so you need to add it as a github dependency:

```elixir
# in mix exs
defp deps do
  [
    {:kafka_client, github: "happening-oss/kafka-client"},
    ...
  ]
end
```

## Examples

### Producing

```elixir
KafkaClient.Producer.start_link(servers: ["localhost:9092"], name: :my_producer)

# fire-and-forget
KafkaClient.Producer.send(
  :my_producer,
  %{topic: "some_topic", key: "key", value: "value"}
)

# sync multi-produce
KafkaClient.Producer.sync_send(:my_producer, [record1, record2, ...])
|> Enum.each(&IO.inspect/1)
```

### Concurrent consuming

```elixir
KafkaClient.Consumer.start_link(
  servers: ["localhost:9092"],
  group_id: "mygroup",
  subscriptions: ["topic1", "topic2", ...],

  poll_duration: 10,
  commit_interval: :timer.seconds(5),

  # These parameters are passed directly to the underlying client.
  # See https://docs.confluent.io/platform/current/installation/configuration/consumer-configs.html
  consumer_params: %{
    "max.poll.interval.ms" => 1000,
  },

  handler: fn
    {:assigned, partitions} -> ...
    {:unassigned, partitions} -> ...

    # invoked only when group_id is nil or not provided
    :caught_up -> ...

    # Invoked in separate process for different partitions.
    # Records on the same partition are processed manually.
    {:records, records} -> ...
  end
)
```

### Stream

```elixir
# Anonymous consuming (no consumer group)
KafkaClient.Consumer.Stream.new([group_id: nil, ...])

# Stop once all the records have been processed. For this to work, it is important to ack each record.
|> Stream.take_while(&(&1 != :caught_up))

# Take only the record notifications (i.e. ignore assigned/unassigned).
|> Stream.filter(&match?({:record, record}, &1))

# Process each record
|> Enum.each(fn {:record, record} ->
  do_something_with(record)

  # don't forget to ack after the record is processed
  KafkaClient.Consumer.Poller.ack(record)
end)
```

See documentation of the following modules for detailed description:

- [KafkaClient.Consumer](lib/kafka_client/consumer.ex)
- [KafkaClient.Consumer.Poller](lib/kafka_client/consumer/poller.ex)
- [KafkaClient.Consumer.Stream](lib/kafka_client/consumer/stream.ex)

## Backends

The client runs in a separate OS process, which is started and supervised by this library. The backend decides which program that is. The Elixir API is the same for both backends.

| | Java (default) | Rust |
|---|---|---|
| Runtime requirement | JVM | none |
| Build requirement | Maven | Rust 1.95+ and a C compiler |
| Brokers for consumer groups | any supported version | Kafka 4.0+ or Confluent Platform 8.0+ (KIP-848) |
| SASL | all mechanisms | `PLAIN` only |
| TLS key material | JKS, PKCS12, PEM | PEM only, with unencrypted private keys |

Anonymous consumers (without a `:group_id`) don't use the group protocol, so they are not subject to the broker requirement above.

### Selecting the Rust backend

The `KAFKA_CLIENT_BACKEND` OS environment variable selects the program to build, and it must be set when the library is compiled. The running application reads the `:backend` setting:

```elixir
# in config/config.exs
config :kafka_client, backend: :rust
```

```bash
KAFKA_CLIENT_BACKEND=rust mix deps.compile kafka_client --force
```

The compiled program is placed in `priv`, so it's included in releases, and the runtime image doesn't need Rust or Java.

### Differences of the Rust backend

- Consumer groups always use the KIP-848 protocol. `heartbeat.interval.ms`, `session.timeout.ms`, and `partition.assignment.strategy` are ignored with a warning, because these settings are now managed by the broker (e.g. `group.consumer.heartbeat.interval.ms`).
- Group state can also be `:assigning`, `:reconciling`, or `:not_ready`.
- `partitioner.class` is not supported, and it's ignored with a warning. `partitioner.type` can be used to choose a built-in partitioner. The default, `Murmur2RandomPartitioner`, places each key on the same partition as the Java client does. The other partitioners place keys differently, so changing it breaks per-key ordering for existing topics.
- Metric names of `KafkaClient.Producer.metrics/1` come from the Rust client, so verify them before relying on specific names.

Moving an existing consumer group from the Java to the Rust backend changes the group protocol. Verify the upgrade path on a test cluster first, in particular if you want to run both backends in the same group during a rolling deploy.

### Authentication

Credentials are provided with the standard Kafka settings in `:consumer_params`, `:producer_params`, and `:admin_params`.

```elixir
# SASL_SSL with PLAIN
%{
  "security.protocol" => "SASL_SSL",
  "sasl.mechanism" => "PLAIN",
  "sasl.jaas.config" =>
    ~s(org.apache.kafka.common.security.plain.PlainLoginModule required username="user" password="#{password}";),

  # optional if the broker certificate is signed by a public CA
  "ssl.truststore.type" => "PEM",
  "ssl.truststore.location" => "/path/to/ca.pem"
}

# TLS with a client certificate. The file holds the private key and the certificate chain.
%{
  "security.protocol" => "SSL",
  "ssl.truststore.type" => "PEM",
  "ssl.truststore.location" => "/path/to/ca.pem",
  "ssl.keystore.type" => "PEM",
  "ssl.keystore.location" => "/path/to/client.pem"
}
```

SCRAM, OAUTHBEARER (including AWS MSK IAM), and GSSAPI are not supported by the Rust client, so these require the Java backend.

The Java backend passes these settings on the command line of its OS process, where other processes on the host can read them. The Rust backend sends them through the port.

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md)
