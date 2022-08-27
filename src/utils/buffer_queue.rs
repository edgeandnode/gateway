use tokio::sync::mpsc::{error::SendError, unbounded_channel, UnboundedReceiver, UnboundedSender};

pub struct QueueReader<T> {
    recv: UnboundedReceiver<T>,
}

pub struct QueueWriter<T> {
    send: UnboundedSender<T>,
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
        self.send.send(value)
    }
}

impl<T> QueueReader<T> {
    /// This method is cancellation safe
    /// https://docs.rs/tokio/latest/tokio/macro.select.html#cancellation-safety
    pub async fn read(&mut self, buffer: &mut Vec<T>) {
        // Cancellation safe because UnboundedReceiver::recv is also
        // cancellation safe, and once we get the first item we fill the buffer
        // without awaiting. So, no messages can be lost.
        if let Some(first) = self.recv.recv().await {
            buffer.push(first);
            self.try_read(buffer)
        }
    }

    pub fn try_read(&mut self, buffer: &mut Vec<T>) {
        while let Ok(next) = self.recv.try_recv() {
            buffer.push(next)
        }
    }
}

pub fn pair<T>() -> (QueueWriter<T>, QueueReader<T>) {
    let (send, recv) = unbounded_channel();
    (QueueWriter { send }, QueueReader { recv })
}
