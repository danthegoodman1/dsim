use std::collections::HashMap;
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use std::thread;

use crate::message_bus::{Envelope, Message, PublishHook, NoOpHook};

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

pub struct MessageBus<H: PublishHook = NoOpHook> {
    subscribers: HashMap<String, Box<dyn Subscriber>>,
    msg_rxs: Option<Vec<flume::Receiver<Envelope>>>,
    msg_txs: Vec<flume::Sender<Envelope>>,
    tick_interval: std::time::Duration,
    shutdown: Arc<AtomicBool>,
    handle: Option<thread::JoinHandle<()>>,
    hook: Option<H>,
}

impl MessageBus<NoOpHook> {
    /// Creates a new MessageBus with the given tick interval and number of queues.
    ///
    /// By default, you must have at least one queue. If you pass 0, it will default to 1.
    ///
    /// The queue priority is in increasing order, so the [0] queue has the lowest priority,
    /// and the [queues - 1] queue has the highest priority.
    ///
    /// If an [Envelope] is sent with a priority higher than the available queue index, it will fall
    /// down to the highest priority queue.
    pub fn new(tick_interval: std::time::Duration, queues: usize) -> Self {
        Self::with_hook(tick_interval, queues, NoOpHook)
    }
}

impl<H: PublishHook> MessageBus<H> {
    /// Creates a new MessageBus with the given tick interval, number of queues, and publish hook.
    ///
    /// By default, you must have at least one queue. If you pass 0, it will default to 1.
    ///
    /// The queue priority is in increasing order, so the [0] queue has the lowest priority,
    /// and the [queues - 1] queue has the highest priority.
    ///
    /// If an [Envelope] is sent with a priority higher than the available queue index, it will fall
    /// down to the highest priority queue.
    pub fn with_hook(tick_interval: std::time::Duration, queues: usize, hook: H) -> Self {
        let mut queues = queues;
        if queues == 0 {
            queues = 1;
        }
        let (msg_txs, msg_rxs): (Vec<_>, Vec<_>) = (0..queues).map(|_| flume::unbounded()).unzip();

        Self {
            subscribers: HashMap::new(),
            msg_rxs: Some(msg_rxs),
            msg_txs,
            tick_interval,
            shutdown: Arc::new(AtomicBool::new(false)),
            handle: None,
            hook: Some(hook),
        }
    }

    pub fn start(&mut self) -> Vec<flume::Sender<Envelope>> {
        println!("Starting MessageBus");
        // launch thread to handle message sending
        let rx = self.msg_rxs.take().expect("MessageBus already started");
        let tx = self.msg_txs.clone();
        let tick_interval = self.tick_interval;
        let shutdown = self.shutdown.clone();
        let subscribers = std::mem::take(&mut self.subscribers);
        let hook = self.hook.take().expect("MessageBus already started");

        let handle = thread::spawn(move || {
            Self::process_messages(rx, tx, subscribers, tick_interval, shutdown, hook);
        });

        self.handle = Some(handle);
        self.msg_txs.clone()
    }

    pub fn subscribe(&mut self, destination: String, sender: Box<dyn Subscriber>) {
        self.subscribers.insert(destination, sender);
    }

    pub fn publish(&mut self, envelope: Envelope) {
        let priority = envelope.priority.min(self.msg_txs.len() - 1);
        self.msg_txs[priority].send(envelope).unwrap();
    }

    fn process_messages(
        rxs: Vec<flume::Receiver<Envelope>>,
        txs: Vec<flume::Sender<Envelope>>,
        mut subscribers: HashMap<String, Box<dyn Subscriber>>,
        tick_interval: std::time::Duration,
        shutdown: Arc<AtomicBool>,
        hook: H,
    ) {
        println!("Processing messages");
        let start_time = std::time::SystemTime::now();
        let mut next_tick = start_time + tick_interval;

        // Handle initial tick
        for subscriber in subscribers.values_mut() {
            let envelopes = subscriber.tick(start_time);
            for envelope in envelopes {
                hook.on_publish(&envelope, start_time);
                let priority = envelope.priority.min(txs.len() - 1);
                txs[priority].send(envelope).unwrap();
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
                            hook.on_publish(&envelope, at);
                            let priority = envelope.priority.min(txs.len() - 1);
                            txs[priority].send(envelope).unwrap();
                        }
                    }
                    next_tick += tick_interval;
                }
                continue;
            }

            let timeout = next_tick.duration_since(now).unwrap_or(tick_interval); // if time moves backwards, or we don't make the next tick, we set to the default duration
            // TODO: warn log if time moves backwards or takes too long

            // First, try all queues in decreasing priority order (non-blocking)
            let mut envelope_opt = None;
            for i in (0..rxs.len()).rev() {
                if let Ok(envelope) = rxs[i].try_recv() {
                    envelope_opt = Some(envelope);
                    break;
                }
            }

            // If no message found, select on all queues with timeout
            if envelope_opt.is_none() {
                let mut selector = flume::Selector::new();
                for rx in &rxs {
                    selector = selector.recv(rx, |result| result);
                }

                match selector.wait_timeout(timeout) {
                    Ok(Ok(envelope)) => envelope_opt = Some(envelope),
                    Ok(Err(_)) => break, // Disconnected
                    Err(_) => continue,  // Timeout
                }
            }

            // Process the envelope if we got one
            if let Some(envelope) = envelope_opt {
                let Some(subscriber) = subscribers.get_mut(&envelope.destination) else {
                    continue;
                };
                let at = std::time::SystemTime::now();
                let envelopes = subscriber.receive(envelope.message, at);
                for envelope in envelopes {
                    hook.on_publish(&envelope, at);
                    let priority = envelope.priority.min(txs.len() - 1);
                    txs[priority].send(envelope).unwrap();
                }
            }
        }
    }

    pub fn stop(&mut self) {
        // idempotent
        if self.shutdown.swap(true, Ordering::SeqCst) {
            return;
        }

        // Wake the worker if it's blocked on recv_timeout by publishing a nop (high priority)
        let _ = self.msg_txs[self.msg_txs.len() - 1].send(Envelope {
            message: Box::new(NopEnvelope),
            destination: "".to_string(),
            priority: 0,
        });

        // Join the worker thread if present
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}

impl<H: PublishHook> Drop for MessageBus<H> {
    fn drop(&mut self) {
        self.stop();
    }
}
