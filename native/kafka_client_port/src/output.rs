//! Output side of the port. Messages are written on a dedicated thread, so that writing to the
//! port doesn't block the worker.
//!
//! All messages go through a single queue, because their order matters. For example, a record of a
//! partition must not arrive after the notification that the partition has been unassigned.

use crate::driver::Command;
use crate::etf;
use crate::term::{atom, string, tuple};
use eetf::Term;
use std::fs::File;
use std::io::{BufWriter, Write};
use std::os::fd::FromRawFd;
use std::process::exit;
use std::sync::Arc;
use std::sync::mpsc::{Receiver, Sender, channel};
use std::thread;
use std::time::{Duration, Instant};
use tokio::sync::Semaphore;

const OUTPUT_FD: i32 = 4;
const BUFFER_SIZE: usize = 1 << 16;
const CLOSE_TIMEOUT: Duration = Duration::from_secs(1);

/// The maximum size of the encoded records waiting to be written. The poller loop waits when the
/// limit is reached, which bounds the memory of the program if Elixir can't keep up.
const RECORDS_BUDGET_BYTES: usize = 4 * 1024 * 1024;

enum Message {
    Term(Term),
    Records {
        frames: Vec<u8>,
        permits: u32,
        emitted_at: Instant,
    },
    Close(Sender<()>),
}

#[derive(Clone)]
pub struct Output {
    messages: Sender<Message>,
    budget: Arc<Semaphore>,
}

impl Output {
    pub fn start() -> Self {
        let (messages, receiver) = channel();
        let budget = Arc::new(Semaphore::new(RECORDS_BUDGET_BYTES));
        let writer_budget = Arc::clone(&budget);

        thread::Builder::new()
            .name("port-output".to_string())
            .spawn(move || write_messages(&receiver, &writer_budget))
            .expect("failed to start the output thread");

        Self { messages, budget }
    }

    pub fn emit(&self, term: Term) {
        self.send(Message::Term(term));
    }

    /// Emits already encoded records, followed by the measurements of their transfer to Elixir.
    pub async fn emit_records(&self, frames: Vec<u8>) {
        // a chunk larger than the budget still has to go through, so it takes the entire budget
        let permits = frames.len().min(RECORDS_BUDGET_BYTES);
        let permits = u32::try_from(permits).expect("budget fits into u32");

        self.budget
            .acquire_many(permits)
            .await
            .expect("the budget is never closed")
            .forget();

        self.send(Message::Records {
            frames,
            permits,
            emitted_at: Instant::now(),
        });
    }

    pub fn emit_call_response(&self, command: &Command, response: Term) {
        let reference = command.reference.as_deref().expect("command is not a call");
        self.emit(tuple(vec![
            atom("$kafka_consumer_response"),
            string(reference),
            response,
        ]));
    }

    /// Waits until the pending messages are written, so the final notifications are not lost.
    pub fn close(&self) {
        let (done, receiver) = channel();
        self.send(Message::Close(done));
        let _ = receiver.recv_timeout(CLOSE_TIMEOUT);
    }

    fn send(&self, message: Message) {
        // the output thread stops the entire process if it fails, so it's always running
        self.messages.send(message).expect("output thread stopped");
    }
}

fn write_messages(messages: &Receiver<Message>, budget: &Semaphore) {
    // SAFETY: the descriptor is allocated by the BEAM for this program, and this is its only owner.
    let file = unsafe { File::from_raw_fd(OUTPUT_FD) };
    let mut output = BufWriter::with_capacity(BUFFER_SIZE, file);
    let mut encoded = Vec::new();

    while let Ok(message) = messages.recv() {
        let mut next = Some(message);

        // Write everything that is queued, and only then flush. A single write then carries
        // multiple messages when the program is busy, and an idle program doesn't delay any.
        while let Some(message) = next {
            match message {
                Message::Term(term) => write_term(&mut output, &mut encoded, &term),
                Message::Records {
                    frames,
                    permits,
                    emitted_at,
                } => {
                    write_records(&mut output, &mut encoded, &frames, emitted_at);
                    budget.add_permits(permits as usize);
                }
                Message::Close(done) => {
                    flush(&mut output);
                    let _ = done.send(());
                }
            }

            next = messages.try_recv().ok();
        }

        flush(&mut output);
    }
}

fn write_records(
    output: &mut impl Write,
    scratch: &mut Vec<u8>,
    frames: &[u8],
    emitted_at: Instant,
) {
    let sending_at = Instant::now();
    write_bytes(output, frames);
    let sent_at = Instant::now();

    scratch.clear();

    etf::frame(scratch, |buffer| {
        etf::tuple_header(buffer, 3);
        etf::atom(buffer, "metrics");
        etf::integer(buffer, nanos(sent_at - sending_at));
        etf::integer(buffer, nanos(sent_at - emitted_at));
    });

    write_bytes(output, scratch);
}

fn write_term(output: &mut impl Write, scratch: &mut Vec<u8>, term: &Term) {
    scratch.clear();
    term.encode(&mut *scratch)
        .expect("failed to encode the term");

    let length = u32::try_from(scratch.len()).expect("message too large");
    write_bytes(output, &length.to_be_bytes());
    write_bytes(output, scratch);
}

fn write_bytes(output: &mut impl Write, bytes: &[u8]) {
    if let Err(error) = output.write_all(bytes) {
        fail(&error);
    }
}

fn flush(output: &mut impl Write) {
    if let Err(error) = output.flush() {
        fail(&error);
    }
}

fn fail(error: &std::io::Error) -> ! {
    eprintln!("kafka_client_port output error: {error}");
    exit(1);
}

fn nanos(duration: Duration) -> i64 {
    i64::try_from(duration.as_nanos()).unwrap_or(i64::MAX)
}
