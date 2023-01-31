use prelude::*;
use serde::Deserialize;
use std::collections::HashMap;

#[derive(Clone)]
pub struct Subscriptions {
    pub active_subscriptions: Eventual<Ptr<HashMap<Address, ActiveSubscription>>>,
}

#[derive(Clone, Deserialize)]
pub struct ActiveSubscription {
    pub id: Bytes32,
    pub user: Address,
    pub start: u64,
    pub end: u64,
    pub rate: u64,
}

impl Subscriptions {
    pub async fn active_subscription(&self, user: &Address) -> Option<ActiveSubscription> {
        self.active_subscriptions
            .value()
            .await
            .ok()?
            .get(user)
            .cloned()
    }

    pub async fn has_active_subscription(&self, user: &Address, timestamp: u64) -> bool {
        match self.active_subscription(user).await {
            Some(subscription) => subscription.start <= timestamp && subscription.end >= timestamp,
            None => false,
        }
    }
}
