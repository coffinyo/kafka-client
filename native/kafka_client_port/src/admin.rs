//! Admin program. See `KafkaClient.Admin`.

use crate::driver::{Command, Commands};
use crate::output::Output;
use crate::term::{self, Value, atom, boolean, error, integer, list, map, nil, ok, string, tuple};
use confluent_kafka::admin::{
    Admin, AdminClient, AdminClientConfig, ListConsumerGroupOffsetsOptions,
    ListConsumerGroupOffsetsSpec, ListGroupsOptions, NewTopic, OffsetSpec,
};
use confluent_kafka::common::config::ConfigResource;
use confluent_kafka::common::config::config_resource::Type;
use confluent_kafka::common::{Error, TopicPartition};
use eetf::Term;
use std::collections::HashMap;

pub async fn run(mut commands: Commands, output: Output, args: Vec<Value>) -> Result<(), Error> {
    let config = AdminClientConfig::new(&args[0].to_config())?;
    let admin = AdminClient::create(config)?;

    while let Some(command) = commands.recv().await {
        if command.name == "stop" {
            break;
        }

        let response = handle(admin.as_ref(), &command).await?;
        output.emit_call_response(&command, response);
    }

    Ok(())
}

/// Errors of `list_topics` are propagated, which stops the program. Other functions report errors
/// to the caller. This is the behaviour of the Java port.
async fn handle(admin: &dyn Admin, command: &Command) -> Result<Term, Error> {
    let args = &command.args;

    match command.name.as_str() {
        "list_topics" => {
            let topics = admin.list_topics().names().get().await?;
            Ok(list(topics.iter().map(|topic| string(topic)).collect()))
        }
        "describe_topics" => Ok(response(describe_topics(admin, &args[0]).await)),
        "describe_topics_config" => Ok(response(describe_topics_config(admin, &args[0]).await)),
        "list_end_offsets" => Ok(response(
            list_offsets(admin, &args[0], OffsetSpec::latest).await,
        )),
        "list_earliest_offsets" => Ok(response(
            list_offsets(admin, &args[0], OffsetSpec::earliest).await,
        )),
        "list_consumer_groups" => Ok(response(list_consumer_groups(admin).await)),
        "describe_consumer_groups" => Ok(response(describe_consumer_groups(admin, &args[0]).await)),
        "delete_consumer_groups" => Ok(delete_consumer_groups(admin, &args[0]).await),
        "list_consumer_group_offsets" => {
            Ok(response(list_consumer_group_offsets(admin, &args[0]).await))
        }
        "create_topics" => Ok(unit_response(create_topics(admin, &args[0]).await)),
        "delete_topics" => Ok(unit_response(delete_topics(admin, &args[0]).await)),
        other => panic!("unknown command {other}"),
    }
}

fn response(result: Result<Term, Error>) -> Term {
    result.map_or_else(|e| error(e.message()), ok)
}

fn unit_response(result: Result<(), Error>) -> Term {
    result.map_or_else(|e| error(e.message()), |()| atom("ok"))
}

fn strings(value: &Value) -> Vec<String> {
    value
        .as_list()
        .iter()
        .map(|item| item.as_str().to_string())
        .collect()
}

fn to_topic_partition(value: &Value) -> TopicPartition {
    let tuple = value.as_list();
    TopicPartition::new(tuple[0].as_str(), tuple[1].as_i32())
}

fn from_topic_partition(partition: &TopicPartition) -> Term {
    term::topic_partition(partition.topic(), partition.partition())
}

fn state_atom(state: impl ToString) -> Term {
    atom(&state.to_string().to_lowercase())
}

async fn describe_topics(admin: &dyn Admin, topics: &Value) -> Result<Term, Error> {
    let result = admin.describe_topics_with_topic_names(&strings(topics));
    let descriptions = result
        .all_topic_names()
        .expect("topic names were requested")
        .get()
        .await?;

    let entries = descriptions
        .iter()
        .map(|(topic, description)| {
            let partitions = description
                .partitions()
                .iter()
                .map(|partition| integer(partition.partition().into()))
                .collect();

            (string(topic), list(partitions))
        })
        .collect();

    Ok(map(entries))
}

async fn describe_topics_config(admin: &dyn Admin, topics: &Value) -> Result<Term, Error> {
    let resources: Vec<ConfigResource> = strings(topics)
        .into_iter()
        .map(|topic| ConfigResource::new(Type::Topic, topic))
        .collect();

    let configs = admin.describe_configs(&resources).all().get().await?;

    let entries = configs
        .iter()
        .map(|(resource, config)| {
            let params = config
                .entries()
                .map(|entry| {
                    map(vec![
                        (atom("name"), string(entry.name())),
                        (atom("value"), entry.value().map_or_else(nil, string)),
                        (atom("is_default"), boolean(entry.is_default())),
                    ])
                })
                .collect();

            (string(resource.name()), list(params))
        })
        .collect();

    Ok(map(entries))
}

async fn list_offsets(
    admin: &dyn Admin,
    topic_partitions: &Value,
    spec: fn() -> OffsetSpec,
) -> Result<Term, Error> {
    let request: HashMap<TopicPartition, OffsetSpec> = topic_partitions
        .as_list()
        .iter()
        .map(|item| (to_topic_partition(item), spec()))
        .collect();

    let offsets = admin.list_offsets(&request).all().get().await?;

    let entries = offsets
        .iter()
        .map(|(partition, info)| (from_topic_partition(partition), integer(info.offset())))
        .collect();

    Ok(map(entries))
}

async fn list_consumer_groups(admin: &dyn Admin) -> Result<Term, Error> {
    let groups = admin
        .list_groups_with_options(ListGroupsOptions::for_consumer_groups())
        .valid()
        .get()
        .await?;

    let groups = groups
        .iter()
        .map(|group| {
            let state = group
                .group_state()
                .map_or_else(|| atom("undefined"), state_atom);
            tuple(vec![string(group.group_id()), state])
        })
        .collect();

    Ok(list(groups))
}

async fn describe_consumer_groups(admin: &dyn Admin, groups: &Value) -> Result<Term, Error> {
    let descriptions = admin
        .describe_consumer_groups(&strings(groups))
        .all()
        .get()
        .await?;

    let entries = descriptions
        .iter()
        .map(|(group, description)| {
            let members = description
                .members()
                .iter()
                .map(|member| {
                    let assignments = member
                        .assignment()
                        .topic_partitions()
                        .iter()
                        .map(from_topic_partition)
                        .collect();

                    tuple(vec![string(member.consumer_id()), list(assignments)])
                })
                .collect();

            let description = map(vec![
                (atom("members"), list(members)),
                (atom("state"), state_atom(description.group_state())),
            ]);

            (string(group), description)
        })
        .collect();

    Ok(map(entries))
}

/// The outcome is reported per group, so a failure of one group doesn't fail the entire call.
async fn delete_consumer_groups(admin: &dyn Admin, groups: &Value) -> Term {
    let mut entries = Vec::new();

    for (group, future) in admin
        .delete_consumer_groups(&strings(groups))
        .deleted_groups()
    {
        let result = match future.get().await {
            Ok(()) => atom("ok"),
            Err(e) => error(e.message()),
        };

        entries.push((string(&group), result));
    }

    ok(map(entries))
}

async fn list_consumer_group_offsets(admin: &dyn Admin, specs: &Value) -> Result<Term, Error> {
    let specs: HashMap<String, ListConsumerGroupOffsetsSpec> = specs
        .as_map()
        .iter()
        .map(|(group, topic_partitions)| {
            let partitions = topic_partitions
                .as_list()
                .iter()
                .map(to_topic_partition)
                .collect();
            let spec = ListConsumerGroupOffsetsSpec::new().set_topic_partitions(Some(partitions));
            (group.as_str().to_string(), spec)
        })
        .collect();

    let offsets = admin
        .list_consumer_group_offsets_with_group_specs_options(
            &specs,
            ListConsumerGroupOffsetsOptions::new(),
        )
        .all()
        .get()
        .await?;

    let entries = offsets
        .iter()
        .map(|(group, partitions)| {
            let partitions = partitions
                .iter()
                .map(|(partition, offset)| {
                    let offset = offset.as_ref().map_or_else(nil, |o| integer(o.offset()));
                    (from_topic_partition(partition), offset)
                })
                .collect();

            (string(group), map(partitions))
        })
        .collect();

    Ok(map(entries))
}

async fn create_topics(admin: &dyn Admin, topics: &Value) -> Result<(), Error> {
    let topics: Vec<NewTopic> = topics
        .as_list()
        .iter()
        .map(|topic| {
            let topic = topic.as_list();
            NewTopic::with_num_partitions_replication_factor(
                topic[0].as_str(),
                topic[1].opt_i32(),
                None,
            )
        })
        .collect();

    admin.create_topics(&topics).all().get().await
}

async fn delete_topics(admin: &dyn Admin, topics: &Value) -> Result<(), Error> {
    admin
        .delete_topics_with_string_slice(&strings(topics))
        .all()
        .get()
        .await
}
