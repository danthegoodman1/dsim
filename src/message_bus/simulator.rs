use std::collections::{HashMap, VecDeque};

use crate::message_bus::{Envelope, Subscriber};

pub enum SimulatorEvent {
    Envelope(Envelope, std::time::SystemTime),
    Tick(std::time::SystemTime),
}

pub struct Simulator {
    subscribers: HashMap<String, Box<dyn Subscriber>>,
    events: Vec<VecDeque<SimulatorEvent>>,
    time: std::time::SystemTime,
}

impl Simulator {
    /// Creates a new simulator with the given subscribers, initial time, and initial events.
    ///
    /// The number of queues is determined by the length of the initial_events vector,
    /// and this must match the number of queues in a [crate::message_bus::MessageBus] to accurately simulate
    /// the message bus.
    pub fn new(
        subscribers: HashMap<String, Box<dyn Subscriber>>,
        initial_time: std::time::SystemTime,
        initial_events: Vec<Vec<SimulatorEvent>>,
    ) -> Self {
        let mut events: Vec<VecDeque<SimulatorEvent>> = initial_events
            .into_iter()
            .map(|events| events.into())
            .collect();
        // Ensure at least one priority queue exists
        if events.is_empty() {
            events.push(VecDeque::new());
        }
        Self {
            subscribers,
            events,
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
        let num_queues = events.len();
        let mut new_events: Vec<VecDeque<SimulatorEvent>> =
            (0..num_queues).map(|_| VecDeque::new()).collect();

        // First we process all of the ticks
        for subscriber in subscribers.values_mut() {
            let envelopes = subscriber.tick(self.time);
            for envelope in envelopes {
                let priority = envelope.priority.min(num_queues - 1);
                new_events[priority].push_back(SimulatorEvent::Envelope(envelope, self.time));
            }
        }

        // Then we increment the time to simulate the passing of time
        self.time += step_by;

        // Then we process all of the events in the queue, in decreasing priority order (highest first)
        for queue in events.into_iter().rev() {
            for event in queue {
                match event {
                    SimulatorEvent::Envelope(envelope, at) => {
                        let subscriber = subscribers.get_mut(&envelope.destination).unwrap();
                        let envelopes = subscriber.receive(envelope.message, at);
                        // Add any new envelopes to the appropriate priority queue
                        for envelope in envelopes {
                            let priority = envelope.priority.min(num_queues - 1);
                            new_events[priority].push_back(SimulatorEvent::Envelope(envelope, at));
                        }
                    }
                    SimulatorEvent::Tick(at) => {
                        for subscriber in subscribers.values_mut() {
                            let envelopes = subscriber.tick(at);
                            // Add any new envelopes to the appropriate priority queue
                            for envelope in envelopes {
                                let priority = envelope.priority.min(num_queues - 1);
                                new_events[priority]
                                    .push_back(SimulatorEvent::Envelope(envelope, at));
                            }
                        }
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
