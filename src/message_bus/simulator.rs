use std::collections::HashMap;

use crate::message_bus::{Envelope, Subscriber};

pub enum SimulatorEvent {
    Envelope(Envelope, std::time::Instant),
    Tick(std::time::Instant),
}

pub struct Simulator {
    subscribers: HashMap<String, Box<dyn Subscriber>>,
    events: Vec<SimulatorEvent>,
}

/// Simulator will run any envelopes and ticks in order to deterministically replay a sequence of events.
/// Emitted envelopes from subscribers will be dropped, the only envelopes and ticks that will be processed
/// are the ones in the events vector.
impl Simulator {
    pub fn new(
        subscribers: HashMap<String, Box<dyn Subscriber>>,
        events: Vec<SimulatorEvent>,
    ) -> Self {
        Self {
            subscribers,
            events,
        }
    }

    pub fn run(self) {
        let mut subscribers = self.subscribers;
        for event in self.events {
            match event {
                SimulatorEvent::Envelope(envelope, at) => {
                    let subscriber = subscribers.get_mut(&envelope.destination).unwrap();
                    subscriber.receive(envelope.message, at);
                }
                SimulatorEvent::Tick(at) => {
                    for subscriber in subscribers.values_mut() {
                        subscriber.tick(at);
                    }
                }
            }
        }
    }
}
