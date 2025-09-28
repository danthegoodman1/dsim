fn main() {
    println!("Hello, world!");
}

#[cfg(test)]
mod tests {
    use dsim::message_bus::{Message, MessageBus, Simulator, Subscriber};
    use std::{
        collections::VecDeque,
        time::{self, UNIX_EPOCH},
    };

    /// PingPong will emit a ping every tick, and respond with a pong.
    struct PingPong {
        pings: VecDeque<std::time::SystemTime>,
        ping_hold_time: std::time::Duration,
        destination: String,
        name: String,
    }

    impl PingPong {
        pub fn new(ping_hold_time: std::time::Duration, destination: &str, name: &str) -> Self {
            Self {
                pings: VecDeque::new(),
                ping_hold_time,
                destination: destination.to_string(),
                name: name.to_string(),
            }
        }
    }

    impl Subscriber for PingPong {
        fn receive(
            &mut self,
            msg: Box<dyn Message>,
            at: std::time::SystemTime,
        ) -> Vec<dsim::message_bus::Envelope> {
            if let Some(_) = msg.downcast_ref::<Ping>() {
                self.pings.push_back(at);
                println!("{} received Ping at {:?}", self.name, at);
            } else if let Some(_) = msg.downcast_ref::<Pong>() {
                println!("{} received Pong at {:?}", self.name, at);
            } else {
                panic!("Message is not a Ping or Pong");
            }
            vec![]
        }

        fn tick(&mut self, at: std::time::SystemTime) -> Vec<dsim::message_bus::Envelope> {
            let mut out: Vec<dsim::message_bus::Envelope> = vec![dsim::message_bus::Envelope {
                message: Box::new(Ping {}),
                destination: self.destination.clone(),
            }];
            while let Some(&oldest) = self.pings.front() {
                if at.duration_since(oldest).unwrap() >= self.ping_hold_time {
                    self.pings.pop_front();
                    println!("{} sending pong to {}", self.name, self.destination);
                    out.push(dsim::message_bus::Envelope {
                        message: Box::new(Pong {}),
                        destination: self.destination.clone(),
                    });
                } else {
                    break;
                }
            }
            out
        }
    }

    struct Ping {}

    impl Message for Ping {}

    struct Pong {}

    impl Message for Pong {}

    #[test]
    fn test_message_bus() {
        let mut message_bus = MessageBus::new(std::time::Duration::from_millis(500));
        let ping_pong_1 = PingPong::new(
            std::time::Duration::from_millis(1000),
            "ping_pong_2",
            "ping_pong_1",
        );
        let ping_pong_2 = PingPong::new(
            std::time::Duration::from_millis(1000),
            "ping_pong_1",
            "ping_pong_2",
        );
        message_bus.subscribe("ping_pong_1".to_string(), Box::new(ping_pong_1));
        message_bus.subscribe("ping_pong_2".to_string(), Box::new(ping_pong_2));
        message_bus.start();
        std::thread::sleep(time::Duration::from_secs(3));
        message_bus.stop();
    }

    #[test]
    fn test_simulator() {
        let ping_pong_1 = PingPong::new(
            std::time::Duration::from_millis(1000),
            "ping_pong_2",
            "ping_pong_1",
        );
        let ping_pong_2 = PingPong::new(
            std::time::Duration::from_millis(1000),
            "ping_pong_1",
            "ping_pong_2",
        );
        let mut simulator = Simulator::new(
            maplit::hashmap! {
                "ping_pong_1".to_string() => Box::new(ping_pong_1) as Box<dyn Subscriber>,
                "ping_pong_2".to_string() => Box::new(ping_pong_2) as Box<dyn Subscriber>,
            },
            UNIX_EPOCH,
            vec![],
        );

        for _ in 0..50 {
            simulator.step(std::time::Duration::from_millis(100));
        }
    }
}
