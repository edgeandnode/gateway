use std::time::Duration;
use std::{collections::HashMap, sync::Arc};

use alloy_primitives::Address;
use eventuals::{self, Eventual, EventualExt as _, EventualWriter, Ptr};
use thegraph_core::client as subgraph_client;
use tokio::sync::Mutex;

use gateway_common::utils::timestamp::unix_timestamp;

use crate::subscriptions::{ActiveSubscription, Subscription};

pub struct Client {
    subgraph_client: subgraph_client::Client,
    subscriptions: EventualWriter<Ptr<HashMap<Address, Subscription>>>,
}

impl Client {
    pub fn create(
        subgraph_client: subgraph_client::Client,
    ) -> Eventual<Ptr<HashMap<Address, Subscription>>> {
        let (subscriptions_tx, subscriptions_rx) = Eventual::new();
        let client = Arc::new(Mutex::new(Client {
            subgraph_client,
            subscriptions: subscriptions_tx,
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

        subscriptions_rx
    }

    async fn poll_active_subscriptions(&mut self) -> Result<(), String> {
        // Serve queries for subscriptions that end 10 minutes ago and later.
        let active_sub_end = (unix_timestamp() / 1000) - (60 * 10);

        let query = format!(
            r#"
            userSubscriptions(
                block: $block
                orderBy: id, orderDirection: asc
                first: $first
                where: {{
                    id_gt: $last
                    end_gte: {active_sub_end}
                    cancelled: false
                }}
            ) {{
                id
                user {{
                    id
                    authorizedSigners {{
                        signer
                    }}
                }}
                start
                end
                rate
            }}
            "#,
        );
        let active_subscriptions_response = self
            .subgraph_client
            .paginated_query::<ActiveSubscription>(query)
            .await?;
        if active_subscriptions_response.is_empty() {
            tracing::warn!("discarding empty update (active_subscriptions)");
            return Ok(());
        }

        let subscriptions_map = active_subscriptions_response
            .into_iter()
            .filter_map(|ActiveSubscription { user, rate, .. }| {
                // Skip subscriptions with a rate of 0
                // fa4a8007-1e92-46f5-a478-a1728b69deb5
                if rate == 0 {
                    return None;
                }

                let signers = user
                    .authorized_signers
                    .into_iter()
                    .map(|signer| signer.signer)
                    .chain([user.id])
                    .collect();
                Some((user.id, Subscription { signers, rate }))
            })
            .collect();
        self.subscriptions.write(Ptr::new(subscriptions_map));

        Ok(())
    }
}
