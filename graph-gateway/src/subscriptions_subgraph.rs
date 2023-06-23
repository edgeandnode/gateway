use std::{collections::HashMap, sync::Arc};

use eventuals::{self, EventualExt as _};
use graph_subscriptions::subscription_tier::SubscriptionTiers;
use tokio::sync::Mutex;

use prelude::*;

use crate::price_automation::VolumeEstimations;
use crate::subgraph_client;
use crate::subscriptions::{ActiveSubscription, Subscription};

pub struct Client {
    subgraph_client: subgraph_client::Client,
    tiers: &'static SubscriptionTiers,
    subscriptions_usage: VolumeEstimations<Address>,
    owner_subscription: Option<(Address, Subscription)>,
    subscriptions: EventualWriter<Ptr<HashMap<Address, Subscription>>>,
}

impl Client {
    pub fn create(
        subgraph_client: subgraph_client::Client,
        owner: Option<Address>,
        tiers: &'static SubscriptionTiers,
    ) -> Eventual<Ptr<HashMap<Address, Subscription>>> {
        let owner_subscription = owner.map(|owner| {
            (
                owner,
                Subscription {
                    queries_per_minute: u32::MAX,
                    // TODO: query for authorized signers for owner.
                    signers: vec![],
                    usage: Arc::default(),
                },
            )
        });

        let (mut subscriptions_tx, subscriptions_rx) = Eventual::new();
        subscriptions_tx.write(Ptr::new(owner_subscription.clone().into_iter().collect()));
        let client = Arc::new(Mutex::new(Client {
            subgraph_client,
            tiers,
            subscriptions_usage: VolumeEstimations::new(),
            owner_subscription,
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
                    queries_per_minute: tier.queries_per_minute,
                    usage: self.subscriptions_usage.get(&user.id),
                };
                (user.id, sub)
            })
            .chain(self.owner_subscription.clone())
            .collect();
        self.subscriptions.write(Ptr::new(subscriptions_map));

        Ok(())
    }
}
