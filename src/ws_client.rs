use futures_util::{SinkExt, StreamExt};
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio_tungstenite::{tungstenite, MaybeTlsStream, WebSocketStream};
use tracing::{self, Instrument};

#[derive(Debug)]
pub enum Request {
    Send(String),
}

#[derive(Debug)]
pub enum Msg {
    Connected,
    Recv(String),
}

pub fn create(
    server_url: String,
    retry_limit: usize,
) -> (mpsc::UnboundedSender<Request>, mpsc::UnboundedReceiver<Msg>) {
    let (request_tx, mut request_rx) = mpsc::unbounded_channel();
    let (mut msg_tx, msg_rx) = mpsc::unbounded_channel();
    tokio::spawn(
        async move {
            let mut retry = 0;
            while retry < retry_limit {
                retry += 1;
                let (stream, response) = match tokio_tungstenite::connect_async(&server_url).await {
                    Ok(response) => response,
                    Err(err) => {
                        tracing::error!(%err);
                        continue;
                    }
                };
                let connection_status = response.status();
                tracing::info!(%connection_status);
                if connection_status.is_client_error() {
                    return;
                }
                if connection_status.is_server_error() {
                    continue;
                }
                retry = 0;
                match msg_tx.send(Msg::Connected) {
                    Ok(()) => (),
                    Err(_) => {
                        tracing::error!("message channel dropped");
                        return;
                    }
                };
                let result = handle_stream(stream, &mut request_rx, &mut msg_tx).await;
                tracing::warn!("connection closed");
                if let Err(()) = result {
                    return;
                }
            }
        }
        .in_current_span(),
    );
    (request_tx, msg_rx)
}

async fn handle_stream(
    mut stream: WebSocketStream<MaybeTlsStream<TcpStream>>,
    requests: &mut mpsc::UnboundedReceiver<Request>,
    messages: &mut mpsc::UnboundedSender<Msg>,
) -> Result<(), ()> {
    loop {
        tokio::select! {
            result = stream.next() => match result {
                Some(Ok(tungstenite::Message::Text(msg))) => {
                    tracing::trace!(?msg);
                    if let Err(_) = messages.send(Msg::Recv(msg)) {
                        tracing::error!("message channel dropped");
                        let _ = stream.close(None).await;
                        return Err(());
                    };
                }
                Some(Ok(msg)) => {
                    tracing::trace!(?msg);
                }
                Some(Err(err)) => {
                    tracing::error!(?err);
                    return Ok(());
                }
                None => return Ok(()),
            },
            result = requests.recv() => match result {
                Some(Request::Send(msg)) => {
                    tracing::trace!("send {:?}", msg);
                    if let Err(err) = stream.send(tungstenite::Message::Text(msg)).await {
                        tracing::error!(?err);
                        return Ok(());
                    }
                }
                None => {
                    tracing::error!("request channel dropped");
                    let _ = stream.close(None).await;
                    return Err(());
                }
            }
        };
    }
}
