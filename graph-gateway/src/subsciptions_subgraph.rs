use crate::config::SubscriptionTiers;
use crate::kafka_client::timestamp;
use crate::price_automation::VolumeEstimations;
use crate::subgraph_client;
use crate::subscriptions::{ActiveSubscription, Subscription};
use eventuals::{self, EventualExt as _};
use prelude::*;
use std::{collections::HashMap, sync::Arc};
use tokio::sync::Mutex;

pub struct Client {
    subgraph_client: subgraph_client::Client,
    tiers: SubscriptionTiers,
    subscriptions_usage: VolumeEstimations<Address>,
    subscriptions: EventualWriter<Ptr<HashMap<Address, Subscription>>>,
}

impl Client {
    pub fn create(
        subgraph_client: subgraph_client::Client,
        tiers: SubscriptionTiers,
    ) -> Eventual<Ptr<HashMap<Address, Subscription>>> {
        let (subscriptions_tx, subscriptions_rx) = Eventual::new();
        let client = Arc::new(Mutex::new(Client {
            subgraph_client,
            tiers,
            subscriptions_usage: VolumeEstimations::new(),
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
        let active_sub_end = timestamp() - (60 * 10);

        let query = format!(
            r#"
            activeSubscriptions(
                first: $first, skip: $skip, block: $block
                where: {{ end_gte: {active_sub_end} }}
            ) {{
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
            .paginated_query::<ActiveSubscription>(&query)
            .await?;
        if active_subscriptions_response.is_empty() {
            return Err("Discarding empty update (active_subscriptions)".to_string());
        }

        let subscriptions_map = active_subscriptions_response
            .into_iter()
            .map(|active_sub| {
                let user = active_sub.user;
                let signers = user
                    .authorized_signers
                    .into_iter()
                    .map(|signer| signer.signer)
                    .chain([user.id]);
                let tier = self.tiers.tier_for_rate(active_sub.rate);
                let sub = Subscription {
                    signers: signers.collect(),
                    query_rate_limit: tier.query_rate_limit,
                    usage: self.subscriptions_usage.get(&user.id),
                };
                (user.id, sub)
            })
            .collect();
        self.subscriptions.write(Ptr::new(subscriptions_map));

        Ok(())
    }
}
