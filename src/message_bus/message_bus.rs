use std::collections::HashMap;
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use std::thread;

use crate::message_bus::{Envelope, Message};

/// A subscriber must **always** follow these rules to remain deterministic:
/// - No internal sleeping (wait until `tick()` when `at` has passed)
/// - No async runtime (need to talk to the internet, or a DB? Kick out to another subscriber)
/// - No random number generation (unless you seed it with the start message)
/// - Never care about the tick interval, always operate from time deltas
///
/// ## Tips
///
/// The [MessageBus] runs all subscriber ticks and receives in a single thread for
/// each loop. If you need to do something CPU or IO heavy:
/// - Accept the envelopes
/// - Send them to another thread or async runtime, and return from [Subscriber::receive] immediately
/// - On the next [Subscriber::tick] or [Subscriber::receive], return any envelopes that are ready to be delivered to another subscriber
///
/// For example, if you have an io_uring [Subscriber], on [Subscriber::receive] you would enqueue the IO operation,
/// have a background thread polling for completions, and on [Subscriber::tick] or [Subscriber::receive] you would return any envelopes
/// destined back to the caller.
pub trait Subscriber: Send + 'static {
    fn receive(&mut self, msg: Box<dyn Message>, at: std::time::SystemTime) -> Vec<Envelope>;
    fn tick(&mut self, at: std::time::SystemTime) -> Vec<Envelope>;
}

/// Internal no-op envelope used to wake the receiver during shutdown
struct NopEnvelope;

impl Message for NopEnvelope {}

pub struct MessageBus {
    subscribers: HashMap<String, Box<dyn Subscriber>>,
    msg_rx: Option<flume::Receiver<Envelope>>,
    msg_tx: flume::Sender<Envelope>,
    tick_interval: std::time::Duration,
    shutdown: Arc<AtomicBool>,
    handle: Option<thread::JoinHandle<()>>,
}

impl MessageBus {
    pub fn new(tick_interval: std::time::Duration) -> Self {
        let (msg_tx, msg_rx) = flume::unbounded();
        Self {
            subscribers: HashMap::new(),
            msg_rx: Some(msg_rx),
            msg_tx,
            tick_interval,
            shutdown: Arc::new(AtomicBool::new(false)),
            handle: None,
        }
    }

    pub fn start(&mut self) -> flume::Sender<Envelope> {
        println!("Starting MessageBus");
        // launch thread to handle message sending
        let rx = self.msg_rx.take().expect("MessageBus already started");
        let tx = self.msg_tx.clone();
        let tick_interval = self.tick_interval;
        let shutdown = self.shutdown.clone();
        let subscribers = std::mem::take(&mut self.subscribers);

        let handle = thread::spawn(move || {
            Self::process_messages(rx, tx, subscribers, tick_interval, shutdown);
        });

        self.handle = Some(handle);
        self.msg_tx.clone()
    }

    pub fn subscribe(&mut self, destination: String, sender: Box<dyn Subscriber>) {
        self.subscribers.insert(destination, sender);
    }

    pub fn publish(&mut self, envelope: Envelope) {
        self.msg_tx.send(envelope).unwrap();
    }

    fn process_messages(
        rx: flume::Receiver<Envelope>,
        tx: flume::Sender<Envelope>,
        mut subscribers: HashMap<String, Box<dyn Subscriber>>,
        tick_interval: std::time::Duration,
        shutdown: Arc<AtomicBool>,
    ) {
        println!("Processing messages");
        let start_time = std::time::SystemTime::now();
        let mut next_tick = start_time + tick_interval;

        // Handle initial tick
        for subscriber in subscribers.values_mut() {
            let envelopes = subscriber.tick(start_time);
            for envelope in envelopes {
                tx.send(envelope).unwrap();
            }
        }

        loop {
            println!("Processing messages loop");
            if shutdown.load(Ordering::SeqCst) {
                break;
            }
            // If we've passed the scheduled tick time, catch up (handle multiple if needed)
            let now = std::time::SystemTime::now();
            if now >= next_tick {
                while next_tick <= std::time::SystemTime::now() {
                    let at = next_tick;
                    for (name, subscriber) in subscribers.iter_mut() {
                        println!("Ticking {}", name);
                        let envelopes = subscriber.tick(at);
                        for envelope in envelopes {
                            tx.send(envelope).unwrap();
                        }
                    }
                    next_tick += tick_interval;
                }
                continue;
            }

            let timeout = next_tick.duration_since(now).unwrap_or(tick_interval); // if time moves backwards, or we don't make the next tick, we set to the default duration
            // TODO: warn log if time moves backwards or takes too long
            match rx.recv_timeout(timeout) {
                Ok(envelope) => {
                    let Some(subscriber) = subscribers.get_mut(&envelope.destination) else {
                        continue;
                    };
                    let at = std::time::SystemTime::now();
                    let envelopes = subscriber.receive(envelope.message, at);
                    for envelope in envelopes {
                        tx.send(envelope).unwrap();
                    }
                }
                Err(flume::RecvTimeoutError::Timeout) => {
                    // On exact timeout boundary, loop will run tick next iteration
                    continue;
                }
                Err(flume::RecvTimeoutError::Disconnected) => break,
            }
        }
    }

    pub fn stop(&mut self) {
        // idempotent
        if self.shutdown.swap(true, Ordering::SeqCst) {
            return;
        }

        // Wake the worker if it's blocked on recv_timeout by publishing a nop
        let _ = self.msg_tx.send(Envelope {
            message: Box::new(NopEnvelope),
            destination: "".to_string(),
        });

        // Join the worker thread if present
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}

impl Drop for MessageBus {
    fn drop(&mut self) {
        self.stop();
    }
}
