use crate::prelude::*;
use futures_util::{SinkExt, StreamExt};
use std::io;
use tokio::{self, net::TcpStream, time::sleep_until};
use tokio_tungstenite::{
    tungstenite::{self, error::Error as WSError},
    MaybeTlsStream, WebSocketStream,
};
use tracing::{self, Instrument};
use url::Url;

type WSStream = WebSocketStream<MaybeTlsStream<TcpStream>>;

struct Client {
    notify: mpsc::Sender<Msg>,
    requests: mpsc::Receiver<Request>,
    server: Url,
    retry_limit: usize,
    last_ping: Instant,
    retries: usize,
}

#[derive(Debug)]
pub enum Request {
    Send(String),
}

#[derive(Debug)]
pub enum Msg {
    Connected,
    Recv(String),
}

pub struct Interface {
    pub send: mpsc::Sender<Request>,
    pub recv: mpsc::Receiver<Msg>,
}

pub fn create(buffer: usize, server: Url, retry_limit: usize) -> Interface {
    let (send, requests) = mpsc::channel(buffer);
    let (notify, recv) = mpsc::channel(buffer);
    Client {
        notify,
        requests,
        server,
        retry_limit,
        last_ping: Instant::now(),
        retries: 0,
    }
    .spawn();
    Interface { send, recv }
}

impl Client {
    fn spawn(mut self) {
        tokio::spawn(async move { while let Ok(()) = self.run().await {} }.in_current_span());
    }

    async fn run(&mut self) -> Result<(), ()> {
        let mut conn = match self.reconnect().await {
            Ok(conn) => conn,
            Err(err) => {
                tracing::error!(reconnect_err = %err);
                return Err(());
            }
        };
        self.last_ping = Instant::now();
        if let Err(_) = self.notify.send(Msg::Connected).await {
            return Err(());
        }
        while let Ok(()) = self.handle_msgs(&mut conn).await {}
        self.retries += 1;
        tracing::warn!(retries = %self.retries);
        if self.retries >= self.retry_limit {
            return Err(());
        }
        Ok(())
    }

    async fn reconnect(&mut self) -> Result<WSStream, WSError> {
        fn err<S: ToString>(msg: S) -> Result<WSStream, WSError> {
            Err(WSError::Io(io::Error::new(
                io::ErrorKind::NotConnected,
                msg.to_string(),
            )))
        }
        for _ in 0..self.retry_limit {
            let (stream, response) = tokio_tungstenite::connect_async(&self.server).await?;
            let connection_status = response.status();
            tracing::info!(%connection_status);
            if connection_status.is_client_error() {
                return err(format!("client error: {}", connection_status));
            }
            if connection_status.is_server_error() {
                return err(format!("server error: {}", connection_status));
            };
            return Ok(stream);
        }
        err("retries exhausted")
    }

    async fn handle_msgs(&mut self, conn: &mut WSStream) -> Result<(), ()> {
        tokio::select! {
            result = conn.next() => match result {
                Some(Ok(tungstenite::Message::Text(msg))) => {
                    tracing::trace!(?msg);
                    if let Err(_) = self.notify.send(Msg::Recv(msg)).await {
                        return Err(());
                    };
                }
                Some(Ok(tungstenite::Message::Ping(_))) => tracing::trace!("ping"),
                Some(Ok(tungstenite::Message::Close(close))) => {
                    tracing::warn!(?close);
                    return Err(());
                }
                Some(Ok(unexpected_msg)) => tracing::warn!(?unexpected_msg),
                Some(Err(recv_err)) => {
                    tracing::error!(%recv_err);
                    return Err(());
                }
                None => return Err(()),
            },
            result = self.requests.recv() => match result {
                Some(Request::Send(outgoing)) => {
                    tracing::trace!(?outgoing);
                    if let Err(err) = conn.send(tungstenite::Message::Text(outgoing)).await {
                        tracing::error!(send_err = %err);
                    }
                }
                None => return Err(()),
            },
            _ = sleep_until(self.last_ping + Duration::from_secs(30)) => return Err(()),
        };
        self.last_ping = Instant::now();
        Ok(())
    }
}
