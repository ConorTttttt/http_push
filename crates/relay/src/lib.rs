//! Holochain HTTP push relay.
//!
//! A small service that runs next to a Holochain conductor (HoloPort,
//! EdgeNode, or any Linux host), journals change hints observed by pluggable
//! [`hc_push_source_api::ChangeSource`]s, and serves them to lightweight
//! clients over plain HTTP:
//!
//! * `POST /v1/subscriptions` – register (operator key or open mode)
//! * `GET  /v1/events`        – Server-Sent Events while the client is in the foreground
//! * `GET  /v1/sync`          – durable cursor-based catch-up after reconnecting
//! * `GET  /v1/health`, `GET /v1/capabilities`
//!
//! See `docs/protocol.md` for the wire protocol and `docs/architecture.md`
//! for the trust model.

pub mod api;
pub mod auth;
pub mod config;
pub mod ingest;
pub mod journal;
pub mod metrics;
pub mod mock;
pub mod rate_limit;
pub mod server;
pub mod settings;
pub mod sse;

pub use journal::{Journal, Retention};
pub use server::{serve, ServeOptions};
pub use settings::RelaySettings;
