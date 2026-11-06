//! Input side of the port. Commands are read on a dedicated thread, which allows the program to
//! notice that Elixir has closed the port even while the worker is busy.

use crate::term::Value;
use std::fs::File;
use std::io::{ErrorKind, Read};
use std::os::fd::FromRawFd;
use std::process::exit;
use std::thread;
use std::time::Duration;
use tokio::sync::mpsc::{Receiver, Sender, channel};

const INPUT_FD: i32 = 3;
const STOP_TIMEOUT: Duration = Duration::from_secs(5);

/// The maximum number of commands waiting for the worker. Once the queue is full the program stops
/// reading from the port, which suspends the Elixir process that sends the commands. This bounds
/// the memory of the program when Elixir produces faster than the client can send.
const COMMAND_QUEUE_CAPACITY: usize = 4096;

pub type Commands = Receiver<Command>;

#[derive(Debug)]
pub struct Command {
    pub name: String,
    pub args: Vec<Value>,
    pub reference: Option<String>,
}

/// Reads the program arguments, which are the first message sent by Elixir, and then starts
/// reading commands.
pub fn start_input() -> (Vec<Value>, Commands) {
    // SAFETY: the descriptor is allocated by the BEAM for this program, and this is its only owner.
    let mut input = unsafe { File::from_raw_fd(INPUT_FD) };

    let Some(frame) = read_frame(&mut input) else {
        // the port owner has terminated before the program started
        exit(0);
    };

    let args = Value::decode(&frame).as_list().to_vec();
    let (sender, receiver) = channel(COMMAND_QUEUE_CAPACITY);

    thread::Builder::new()
        .name("port-input".to_string())
        .spawn(move || read_commands(input, &sender))
        .expect("failed to start the input thread");

    (args, receiver)
}

fn read_commands(mut input: File, commands: &Sender<Command>) {
    while let Some(frame) = read_frame(&mut input) {
        // the worker has finished if the send fails, and the process is about to stop
        let _ = commands.blocking_send(decode_command(&frame));
    }

    // The port owner has terminated. Try to stop gracefully, and give up after the timeout.
    let _ = commands.blocking_send(Command {
        name: "stop".to_string(),
        args: Vec::new(),
        reference: None,
    });

    thread::sleep(STOP_TIMEOUT);
    exit(0);
}

fn read_frame(input: &mut File) -> Option<Vec<u8>> {
    let mut header = [0; 4];

    if let Err(error) = input.read_exact(&mut header) {
        assert!(
            error.kind() == ErrorKind::UnexpectedEof,
            "read failed: {error}"
        );
        return None;
    }

    let mut frame = vec![0; u32::from_be_bytes(header) as usize];
    input.read_exact(&mut frame).ok()?;
    Some(frame)
}

fn decode_command(frame: &[u8]) -> Command {
    let Value::Tuple(mut elements) = Value::decode(frame) else {
        panic!("invalid command");
    };

    let [name, args, reference] = elements.as_mut_slice() else {
        panic!("invalid command");
    };

    Command {
        name: name.as_str().to_string(),
        args: args.as_list().to_vec(),
        reference: (*reference != Value::Null).then(|| reference.as_str().to_string()),
    }
}
