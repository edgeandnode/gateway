use std::{collections::HashMap, time::Duration};

use alloy_primitives::Address;
use anyhow::anyhow;
use gateway_common::utils::timestamp::unix_timestamp;
use thegraph_core::client as subgraph_client;
use tokio::{
    sync::watch,
    time::{interval, MissedTickBehavior},
};

use crate::subscriptions::{ActiveSubscription, Subscription};

pub struct Client {
    subgraph_client: subgraph_client::Client,
    subscriptions: watch::Sender<HashMap<Address, Subscription>>,
}

impl Client {
    pub async fn create(
        subgraph_client: subgraph_client::Client,
    ) -> watch::Receiver<HashMap<Address, Subscription>> {
        let (tx, mut rx) = watch::channel(Default::default());
        let mut client = Client {
            subgraph_client,
            subscriptions: tx,
        };

        tokio::spawn(async move {
            let mut interval = interval(Duration::from_secs(30));
            interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
            loop {
                interval.tick().await;

                if let Err(poll_active_subscriptions_err) = client.poll_active_subscriptions().await
                {
                    tracing::error!(%poll_active_subscriptions_err);
                }
            }
        });

        rx.wait_for(|subscriptions| !subscriptions.is_empty())
            .await
            .unwrap();
        rx
    }

    async fn poll_active_subscriptions(&mut self) -> anyhow::Result<()> {
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
            .await
            .map_err(|err| anyhow!(err))?;
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

        self.subscriptions.send(subscriptions_map)?;

        Ok(())
    }
}
