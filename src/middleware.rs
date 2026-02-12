//! HTTP Middleware Layers
//!
//! Axum middleware for request processing, authentication, and tracing.
//!
//! # Middleware Stack (top to bottom)
//!
//! ```text
//! Request
//!    │
//!    ▼
//! ┌─────────────────────────┐
//! │ CorsLayer               │  Allow cross-origin requests
//! └───────────┬─────────────┘
//!             ▼
//! ┌─────────────────────────┐
//! │ RequestTracingLayer     │  Add request_id span, log requests
//! └───────────┬─────────────┘
//!             ▼
//! ┌─────────────────────────┐
//! │ legacy_auth_adapter     │  Extract API key from /{api_key}/... path
//! └───────────┬─────────────┘
//!             ▼
//! ┌─────────────────────────┐
//! │ RequireAuthorizationLayer│  Validate API key, add AuthSettings extension
//! └───────────┬─────────────┘
//!             ▼
//!         Handler
//! ```
//!
//! # Modules
//!
//! - [`request_tracing`]: Adds request ID and tracing span
//! - [`require_auth`]: Enforces API key authentication
//! - [`legacy_auth`]: Adapts legacy `/{api_key}/...` URL scheme

mod legacy_auth;
mod request_tracing;
mod require_auth;

pub use legacy_auth::legacy_auth_adapter;
pub use request_tracing::{RequestId, RequestTracingLayer};
pub use require_auth::RequireAuthorizationLayer;
