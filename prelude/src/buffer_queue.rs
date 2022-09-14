use {
    std::sync::Arc,
    tokio::sync::{
        mpsc::{error::SendError, unbounded_channel, UnboundedReceiver, UnboundedSender},
        Notify,
    },
};

#[derive(Debug)]
pub enum Event<T> {
    Flush(Arc<Notify>),
    Update(T),
}

pub struct QueueReader<T> {
    recv: UnboundedReceiver<Event<T>>,
}

pub struct QueueWriter<T> {
    send: UnboundedSender<Event<T>>,
}

impl<T> Clone for QueueWriter<T> {
    fn clone(&self) -> Self {
        Self {
            send: self.send.clone(),
        }
    }
}

impl<T> QueueWriter<T> {
    pub fn write(&self, value: T) -> Result<(), SendError<T>> {
        let event = Event::Update(value);
        self.send.send(event).map_err(|err| {
            let value = match err {
                SendError(Event::Update(value)) => value,
                _ => unreachable!(),
            };
            SendError(value)
        })
    }

    pub async fn flush(&self) -> Result<(), SendError<()>> {
        let notify = Arc::new(Notify::const_new());
        let notify2 = notify.clone();
        if self.send.send(Event::Flush(notify2)).is_err() {
            return Err(SendError(()));
        }
        notify.notified().await;
        Ok(())
    }
}

impl<T> QueueReader<T> {
    /// This method is cancellation safe
    /// https://docs.rs/tokio/latest/tokio/macro.select.html#cancellation-safety
    pub async fn read(&mut self, buffer: &mut Vec<Event<T>>) {
        // Cancellation safe because UnboundedReceiver::recv is also
        // cancellation safe, and once we get the first item we fill the buffer
        // without awaiting. So, no messages can be lost.
        if let Some(first) = self.recv.recv().await {
            buffer.push(first);
            self.try_read(buffer)
        }
    }

    pub fn try_read(&mut self, buffer: &mut Vec<Event<T>>) {
        while let Ok(next) = self.recv.try_recv() {
            buffer.push(next)
        }
    }
}

pub fn pair<T>() -> (QueueWriter<T>, QueueReader<T>) {
    let (send, recv) = unbounded_channel();
    (QueueWriter { send }, QueueReader { recv })
}
