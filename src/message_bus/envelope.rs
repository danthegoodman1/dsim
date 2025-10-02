use std::any::Any;

pub struct Envelope {
  pub message: Box<dyn Message>,
  pub priority: usize,
  pub destination: String,
}

pub trait Message: Any + Send + 'static {}

impl dyn Message {
    pub fn as_any(&self) -> &(dyn Any + Send) {
        self
    }

    pub fn into_any(self: Box<Self>) -> Box<dyn Any + Send> {
        self
    }

    pub fn downcast_ref<T: Message>(&self) -> Option<&T> {
        self.as_any().downcast_ref::<T>()
    }

    pub fn downcast<T: Message>(self: Box<Self>) -> Result<Box<T>, Box<dyn Message>> {
        if self.as_any().is::<T>() {
            let boxed_any = self.into_any();
            return Ok(boxed_any
                .downcast::<T>()
                .expect("type check and downcast should succeed"));
        } else {
            Err(self)
        }
    }
}
