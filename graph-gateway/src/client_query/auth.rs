use std::sync::Arc;

use thegraph::subscriptions::auth::AuthTokenClaims;

use crate::subgraph_studio::APIKey;

pub use self::context::{AuthContext, UserSettings};

mod common;
mod context;
mod studio;
mod subscriptions;

pub enum AuthToken {
    /// API key from the Subgraph Studio Database.
    StudioApiKey(Arc<APIKey>),
    /// Auth token associated with a subscription.
    SubscriptionsAuthToken(AuthTokenClaims),
}
