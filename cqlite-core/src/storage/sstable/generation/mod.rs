//! Generation discovery: "what are this table's live generations, and what do we know about each
//! one?" (issue #4626, OpenSpec change `generation-discovery`).
//!
//! One module owns four things and nothing else:
//!
//! 1. **Listing** ([`list_generations`]): a table directory and a [`Scope`] in, generations with
//!    component sets and [`Condition`]s out, sorted by Cassandra's id comparator, with every file
//!    in the directory accounted for.
//! 2. **Ids** ([`GenerationId`], [`next_generation_id`]): both id forms, parse limits, ordering and
//!    allocation.
//! 3. **Trusted metadata** ([`Generation::trusted_metadata`]): the one lazy, cached accessor.
//! 4. **Transaction-log liveness**: reading `*_txn_*.log` and classifying files as Cassandra's
//!    `LogAwareFileLister` does.
//!
//! Callers choose only a [`Scope`] and a [`TocPolicy`]. Anything a verb cannot handle is a named
//! refusal in that verb, raised from the typed list this module returns; the module never filters
//! on a verb's behalf and never deletes anything.
//!
//! Every Cassandra citation in this module is at the pinned `cassandra-5.0.8` tag.

mod alloc;
mod error;
mod id;
mod listing;
mod metadata;
mod model;
mod name;
mod txn_classify;
mod txn_log;

pub use alloc::{ensure_sequence_id_free, next_generation_id, next_sequence_after};
pub use error::{GenerationError, GenerationResult};
pub use id::{GenerationId, UuidGenerationId, MAX_SEQUENCE_ID};
pub use listing::list_generations;
pub use metadata::MetadataFailure;
pub use model::{
    Component, Condition, DirectoryNote, Generation, HiddenBy, HiddenFile, Listing, Scope,
    TocPolicy,
};
pub use name::{check_floor, BelowFloor};
