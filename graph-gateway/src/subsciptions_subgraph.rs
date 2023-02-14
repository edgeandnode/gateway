use crate::subgraph_client::SubgraphClient;
use crate::subscriptions::{ActiveSubscription, Subscriptions};
use eventuals::{self, EventualExt as _};
use prelude::*;
use std::{collections::HashMap, sync::Arc};
use tokio::sync::Mutex;

#[derive(Clone)]
pub struct Data {
    pub subscriptions: Subscriptions,
}

pub struct Client {
    subgraph_client: SubgraphClient,
    active_subscriptions: EventualWriter<Ptr<HashMap<Address, ActiveSubscription>>>,
}

impl Client {
    pub fn create(subgraph_client: SubgraphClient) -> Data {
        let (active_subscriptions_tx, active_subscriptions_rx) = Eventual::new();
        let client = Arc::new(Mutex::new(Client {
            subgraph_client,
            active_subscriptions: active_subscriptions_tx,
        }));

        eventuals::timer(Duration::from_secs(30))
            .pipe_async(move |_| {
                let client = client.clone();
                async move {
                    let mut client = client.lock().await;
                    if let Err(poll_active_subscriptions_err) =
                        client.poll_active_subscriptions().await
                    {
                        tracing::error!(%poll_active_subscriptions_err);
                    }
                }
            })
            .forever();

        Data {
            subscriptions: Subscriptions {
                active_subscriptions: active_subscriptions_rx,
            },
        }
    }

    async fn poll_active_subscriptions(&mut self) -> Result<(), String> {
        let active_subscriptions_response = self
            .subgraph_client
            .paginated_query::<ActiveSubscription>(
                r#"
                activeSubscriptions(first: $first, skip: $skip, block: $block) {
                    id
                    user {
                        id
                        authorizedSigners {
                            id
                            signer
                        }
                    }
                    start
                    end
                    rate
                }
                "#,
            )
            .await?;
        if active_subscriptions_response.is_empty() {
            return Err("Discarding empty update (active_subscriptions)".to_string());
        }

        let active_subscriptions_map = active_subscriptions_response
            .into_iter()
            .map(|sub| (sub.user.id, sub))
            .collect();
        self.active_subscriptions
            .write(Ptr::new(active_subscriptions_map));

        Ok(())
    }
}
