use std::collections::{HashMap, VecDeque};

use crate::message_bus::{Envelope, Subscriber};

pub enum SimulatorEvent {
    Envelope(Envelope, std::time::SystemTime),
    Tick(std::time::SystemTime),
}

pub struct Simulator {
    subscribers: HashMap<String, Box<dyn Subscriber>>,
    events: VecDeque<SimulatorEvent>,
    time: std::time::SystemTime,
}

impl Simulator {
    pub fn new(
        subscribers: HashMap<String, Box<dyn Subscriber>>,
        initial_time: std::time::SystemTime,
        initial_events: Vec<SimulatorEvent>,
    ) -> Self {
        Self {
            subscribers,
            events: initial_events.into(),
            time: initial_time,
        }
    }

    /// Steps the simluator by some duration, looping through all of the subscribers to
    /// run their tick, then receive for anything in the queue.
    ///
    /// Returns the new time after the step.
    pub fn step(&mut self, step_by: std::time::Duration) -> std::time::SystemTime {
        let subscribers = &mut self.subscribers;
        let events = std::mem::take(&mut self.events); // we are replacing this later anyway
        let mut new_events = VecDeque::new();

        // First we process all of the ticks
        for subscriber in subscribers.values_mut() {
            let envelopes = subscriber.tick(self.time);
            for envelope in envelopes {
                new_events.push_back(SimulatorEvent::Envelope(envelope, self.time));
            }
        }

        // Then we increment the time to simulate the passing of time
        self.time += step_by;

        // Then we process all of the events in the queue
        for event in events {
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
        // Reset the events queue
        self.events = new_events;
        self.time
    }

    pub fn step_to(
        &mut self,
        time: std::time::SystemTime,
        step_by: std::time::Duration,
    ) -> std::time::SystemTime {
        // Keep stepping until we reach the target time
        while self.time < time {
            // Calculate the remaining time to reach the target
            let remaining_time = time
                .duration_since(self.time)
                .unwrap_or(std::time::Duration::ZERO);

            // Step by the minimum of step_by and remaining time
            let actual_step = step_by.min(remaining_time);

            // If no time remains, break out
            if actual_step.is_zero() {
                break;
            }

            self.step(actual_step);
        }

        self.time
    }
}
