use chrono::{DateTime, NaiveDateTime, Utc};
use prelude::*;
use serde::{de::Error, Deserialize, Deserializer};
use std::collections::HashMap;

#[derive(Clone)]
pub struct Subscriptions {
    pub active_subscriptions: Eventual<Ptr<HashMap<Address, ActiveSubscription>>>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct User {
    pub id: Address,
    pub authorized_signers: Option<Vec<AuthorizedSigner>>,
}

#[derive(Clone, Debug, Deserialize)]
pub struct ActiveSubscription {
    pub user: User,
    #[serde(deserialize_with = "deserialize_datetime_utc")]
    pub start: DateTime<Utc>,
    #[serde(deserialize_with = "deserialize_datetime_utc")]
    pub end: DateTime<Utc>,
    #[serde(deserialize_with = "deserialize_u128")]
    pub rate: u128,
}
fn deserialize_datetime_utc<'de, D>(deserializer: D) -> Result<DateTime<Utc>, D::Error>
where
    D: Deserializer<'de>,
{
    let input = String::deserialize(deserializer)?;
    let timestamp = input.parse::<u64>().map_err(D::Error::custom)?;
    NaiveDateTime::from_timestamp_opt(timestamp.try_into().map_err(D::Error::custom)?, 0)
        .ok_or_else(|| D::Error::custom("invalid timestamp"))
        .map(|t| DateTime::<Utc>::from_utc(t, Utc))
}
fn deserialize_u128<'de, D>(deserializer: D) -> Result<u128, D::Error>
where
    D: Deserializer<'de>,
{
    let input = String::deserialize(deserializer)?;
    u128::from_str(&input).map_err(D::Error::custom)
}

#[derive(Clone, Debug, Deserialize)]
pub struct AuthorizedSigner {
    pub signer: Address,
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

    pub async fn has_active_subscription(&self, user: &Address, timestamp: DateTime<Utc>) -> bool {
        match self.active_subscription(user).await {
            Some(subscription) => subscription.start <= timestamp && subscription.end >= timestamp,
            None => false,
        }
    }
}

#[cfg(test)]
mod tests {
    use anyhow::ensure;
    use serde_json::json;

    use super::*;

    #[test]
    fn should_parse_active_subscriptions_query() -> anyhow::Result<()> {
        let result = serde_json::from_str::<ActiveSubscription>(
            &json!({
                "user": {
                    "id": "0xf39fd6e51aad88f6f4ce6ab8827279cfffb92266",
                    "authorizedSigners": [
                        {
                            "signer": "0xf39fd6e51aad88f6f4ce6ab8827279cfffb92266"
                        }
                    ],
                },
                "start": "1676507163",
                "end": "1676507701",
                "rate": "100000000000000",
            })
            .to_string(),
        );
        ensure!(result.is_ok(), "failed to parse example: {:?}", result);
        Ok(())
    }
}
