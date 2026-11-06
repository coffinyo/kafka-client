//! Port program which exposes the Confluent Rust Kafka client to Elixir.
//!
//! This is a drop-in replacement for the Java program in `port/`, and it speaks the same protocol
//! with `KafkaClient.GenPort`:
//!
//! - The command line argument selects the program (`admin`, `consumer`, or `producer`).
//! - Commands are read from the file descriptor 3, and messages are written to the descriptor 4.
//!   Each message is an Erlang term, prefixed with a four byte big-endian length.
//! - The first message read is the list of program arguments. This is the one difference from the
//!   Java port, which takes them from the command line. The arguments may hold credentials, and
//!   the command line of a process can be seen by other processes.
//!
//! The program fails fast. Any panic or unrecoverable error stops the OS process with a non-zero
//! status, which `GenPort` reports as a port crash, leaving recovery to the Elixir supervisor.

mod admin;
mod consumer;
mod driver;
mod etf;
mod output;
mod producer;
mod term;

use confluent_kafka::common::Error;
use driver::Commands;
use output::Output;
use std::process::exit;
use term::Value;

fn main() {
    std::panic::set_hook(Box::new(|info| {
        eprintln!("kafka_client_port panic: {info}");
        exit(101);
    }));

    let program = std::env::args().nth(1).expect("missing program name");
    let (args, commands) = driver::start_input();
    let output = Output::start();

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("failed to start the runtime");

    let result = runtime.block_on(run(&program, commands, output.clone(), args));
    output.close();

    match result {
        Ok(()) => exit(0),
        Err(error) => {
            eprintln!("kafka_client_port error: {error}");
            exit(1);
        }
    }
}

async fn run(
    program: &str,
    commands: Commands,
    output: Output,
    args: Vec<Value>,
) -> Result<(), Error> {
    match program {
        "admin" => admin::run(commands, output, args).await,
        "consumer" => consumer::run(commands, output, args).await,
        "producer" => producer::run(commands, output, args).await,
        other => panic!("unknown program {other}"),
    }
}
