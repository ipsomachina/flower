//! The compatibility contract every peer connection checks.
//!
//! Members of one group must agree on how entries apply, how snapshots
//! and stored values are encoded and how the peer protocol frames them;
//! a mixed group would diverge rather than fail. Every Raft connection
//! therefore carries this build's contract in its hello, and both ends
//! compare it before any request is read (see [`crate::network`]).
//! Membership changes probe each proposed member's contract too (see
//! [`crate::membership`]).
//!
//! Bump the relevant field before publishing a change that a member of
//! the previous build would apply, encode or frame differently. The
//! build version is diagnostic: a bugfix release that keeps every field
//! joins its predecessor's group, which is what lets a cluster roll
//! compatible builds one member at a time.
//!
//! A member's files record the part of the contract that their bytes
//! depend on ([`storage_contract`]: how entries apply, how snapshots
//! and values encode) when they are created, and a member refuses at
//! start to open files another contract wrote ([`check_store`]), naming
//! both: a build past a bump would otherwise decode, or apply again,
//! entries and values as they were not written. There is no migration:
//! across a bump, a member starts over from a backup restored into a
//! fresh directory.

use std::path::Path;
use std::sync::OnceLock;

use serde::{Deserialize, Serialize};

use crate::sync::SYNC_SNAPSHOT_VERSION;

/// What a member must share with every other member of its group.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Compatibility {
    /// Peer protocol: hello, request and response frames.
    pub raft_wire: u32,
    /// Applying an entry: the `ClusterWrite` variants and what each does,
    /// deploy-time module validation included.
    pub state_machine: u32,
    /// Snapshot image encoding.
    pub snapshot_format: u32,
    /// Stored values, patches and multi-patches.
    pub value_format: u32,
    /// The Wasm runtime that validates deployed modules when their entry
    /// applies: another version may accept or refuse different modules.
    pub guest_runtime: String,
    /// The package version. Diagnostic only: never compared.
    pub build: String,
}

impl Compatibility {
    /// The compared fields as one string, as hellos carry it.
    pub fn contract(&self) -> String {
        format!(
            "wire{}-state{}-snapshot{}-value{}-{}",
            self.raft_wire,
            self.state_machine,
            self.snapshot_format,
            self.value_format,
            self.guest_runtime
        )
    }

    /// Whether a member advertising `other` may join this member's
    /// group: every field but the build matches.
    pub fn compatible_with(&self, other: &Self) -> bool {
        self.contract() == other.contract()
    }

    /// The fields a member's files depend on, as one string: how entries
    /// apply, how snapshots and values encode. The peer protocol and the
    /// guest runtime are not stored.
    pub fn stored(&self) -> String {
        format!(
            "state{}-snapshot{}-value{}",
            self.state_machine, self.snapshot_format, self.value_format
        )
    }
}

/// Failure code: a member's file was written under another storage
/// contract (see [`check_store`]).
pub const INCOMPATIBLE_STORE: &str = "incompatible_store";

/// This build's storage contract ([`Compatibility::stored`]).
pub fn storage_contract() -> &'static str {
    static STORED: OnceLock<String> = OnceLock::new();
    STORED.get_or_init(|| compatibility().stored())
}

/// What claiming the file at `path` for this build's storage contract
/// found, as an error naming both contracts when it was another's (see
/// [`flower_engine::storage::GroupStore::claim_contract`]).
pub fn check_store(path: &Path, claimed: Result<(), Option<String>>) -> Result<(), String> {
    match claimed {
        Ok(()) => Ok(()),
        Err(Some(other)) => Err(format!(
            "{} was written under storage contract {other}, and this build's is {}: files \
             do not carry over a contract change (restore a backup into a fresh directory)",
            path.display(),
            storage_contract()
        )),
        Err(None) => Err(format!(
            "{} was written by a build that recorded no storage contract, and this build's \
             is {}: files do not carry over a contract change (restore a backup into a fresh \
             directory)",
            path.display(),
            storage_contract()
        )),
    }
}

/// This build's compatibility.
pub fn compatibility() -> &'static Compatibility {
    static CURRENT: OnceLock<Compatibility> = OnceLock::new();
    CURRENT.get_or_init(|| Compatibility {
        // 2: hellos carry both node IDs and this contract; Info, Fence,
        // TransferLeader and Busy frames. 3: TransferLeader names its
        // target, and a voter it does not name releases the leader's
        // lease (Flower's OpenRaft fork); and members time Raft as v1
        // (50 ms heartbeats, an RPC deadline of its own, 0.55–1.1 s
        // elections), whose read lease assumes every voter refuses
        // candidates for the same election max, so a member of the
        // previous build (3 s) must not share a group with this one.
        // 4: hellos name the group (name and uid), so one listener
        // serves every group a process hosts. 5: vote requests carry a
        // pre-vote flag, which a member of the previous build would take
        // for a real vote; and the voters' refusal window is the 300 ms
        // leader lease, not election max. 6: `ControlRequest::Keys`, a
        // move's preflight asking its destination whether it can serve
        // the database's keys; a cross-group transaction's prepare
        // carries what is left of its caller's deadline instead of a
        // fuel override.
        // 7: resize and preferred-leader catalog control requests.
        raft_wire: 7,
        // 2: `ClusterWrite::Batch` entries, and every data entry records
        // the group head batches check (`flower_engine::batch`);
        // `ClusterWrite::Keys` catalog entries; manifests with exports,
        // imports, bindings, `Each` sources and retention hooks, which
        // activation validates on every member. 3: applying a patch
        // records in its receipts the revision and log position its
        // outcome committed at (`flower_engine::retry::Committed`), a
        // trailer older members cannot decode from a snapshot image.
        // 4: partition catalog, placement and cross-group transaction
        // batch items, and the placement and lock fences on every data
        // item. 5: manifests with maintenance tasks and aggregates,
        // which activation validates on every member, and activation
        // keeping or beginning versioned aggregates' lineages (meta rows
        // whose code identity cached aggregate values carry);
        // `ClusterWrite::Remove` entries and `BatchItem::Purge` pages (a
        // removed module's kept declarations, which deploying it again
        // diffs against, and its paged purge), and activations refused
        // while their module's data is being purged; replicated staged
        // deployments: `BatchItem::Staging` steps (stage with bindings,
        // advance, graph pages, cancel, collect) and their extended
        // staging rows, and activation moving a staging's staged graph
        // over the active outcomes. 6: a move's `Freeze` records the
        // source's history incarnation in the database's moving
        // `history_lineage` meta row, and its `Activate` puts the
        // destination's in front, so claims taken before the move stay
        // good after it (`flower_engine::placement::lineage`). 7: retry
        // identities, sessions and their receipts are checked and
        // collected against that lineage too (`placement::History`),
        // establishing an incarnation adopts a lineage that arrived
        // before it and a restore deletes lineages; retention states
        // count retained receipt and session bytes, a patch whose
        // receipts would pass `max_receipt_bytes` is refused at apply,
        // and acknowledgements carry a limit (an abandon fences any
        // distance); `Budget` and `Default` retention commands; a
        // caller's own receipt written under retention has no expiry,
        // and collection walks only receipts without one; a walk asked
        // for while one runs follows it, a budget sets no epochs (the
        // default still rotates such a database), a state from before
        // counting is counted when first used, and apply refuses a
        // scoped identity reused for other content; every activation
        // mints its module a code era (`code_era\0<module>` meta rows
        // and the `code_eras` counter) and drops the derived outcomes
        // of other modules that reached it; `QueueOp::Chained` reports
        // a claim and takes the next in one entry. 8: managed keys are
        // per database: `ClusterWrite::Keys` names one, applies to its
        // `key_catalog` meta row (which moves with its rows), registers
        // it, drops only its key readers, and is refused for a name no
        // database can have, where it does not serve or where a prepared
        // transaction holds it; modules build with their database, so
        // guests resolve keys in its catalog alone, and no member-local
        // key answers an unbound alias. A purge records the queues it
        // deletes, its pages delete their `$queue` rows, held timers and
        // `$task` failure states, a deployment declaring a queue under
        // purge is refused, and so is a patch writing one
        // (`queue_purging`); a removed module's due timers are held
        // (`$timer` rows keyed by module) instead of spending attempts.
        // Manifests carry per-definition `timeout_ms`, which activation
        // validates on every member and invocations derive their fuel
        // from, and timer IDs are any text without control characters.
        // 9: a deployment of the module's active code, which activates
        // nothing, closes the module's open staging as an activation
        // does, and a declared millisecond buys 1,000 units of logical
        // fuel and 10,000,000 of instruction fuel (`engine::FUEL_PER_MS`,
        // `wasm::INSTRUCTIONS_PER_MS`). Files record this version with the
        // snapshot and value formats (`storage_contract`), so a member
        // never opens files an earlier one wrote.
        // 10: named and request-time bounded SQL reads (host ops 35/36),
        // collection-scoped SQL endpoints with fixed ceilings, deployment-planned
        // primary/equality-index access, require_index, and exact decimal
        // logical schemas. Old members cannot execute these manifests;
        // never mix them in one consensus group. Stored values and index
        // codecs are unchanged, so value_format stays at 4.
        // 11: durable resize plans, leader preferences and participant
        // read footprints; managed X25519 shared handles scoped to each invocation.
        // 12: participant footprints certify cached derived sources;
        // receipt and snapshot-backed queue/timer conflicts are precise.
        state_machine: 12,
        snapshot_format: SYNC_SNAPSHOT_VERSION,
        // 2: `$queue` rows split into ready (scope, priority, turn,
        // due), waiting, claimed, dead and completed classes under a
        // scope hash, with fence, turn, group and line rows beside them,
        // and items carry fencing tokens, owners, names, results and
        // their claim's history incarnation; a member of the previous
        // build reads past the new rows as if the queue were empty.
        // 3: derived outcomes, stored and in patches, carry their time
        // (`flower_engine::storage::codec::OutcomeTime`) behind a flag.
        // 4: the time follows the code identity with no flag, and the
        // decoder refuses format 3's flag as corrupt.
        value_format: 4,
        // flower-engine pins wasmtime exactly: bump with it.
        guest_runtime: "wasmtime-49.0.0".to_string(),
        build: env!("CARGO_PKG_VERSION").to_string(),
    })
}

/// This build's contract string.
pub fn contract() -> &'static str {
    static CONTRACT: OnceLock<String> = OnceLock::new();
    CONTRACT.get_or_init(|| compatibility().contract())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builds_differing_only_in_version_are_compatible() {
        let current = compatibility().clone();
        let bugfix = Compatibility {
            build: "9.9.9".to_string(),
            ..current.clone()
        };
        assert!(current.compatible_with(&bugfix));
        for changed in [
            Compatibility {
                raft_wire: current.raft_wire + 1,
                ..current.clone()
            },
            Compatibility {
                state_machine: current.state_machine + 1,
                ..current.clone()
            },
            Compatibility {
                snapshot_format: current.snapshot_format + 1,
                ..current.clone()
            },
            Compatibility {
                value_format: current.value_format + 1,
                ..current.clone()
            },
            Compatibility {
                guest_runtime: "wasmtime-50.0.0".to_string(),
                ..current.clone()
            },
        ] {
            assert!(!current.compatible_with(&changed), "{changed:?}");
        }
    }

    #[test]
    fn the_value_format_names_the_stored_outcome_layout() {
        // A change to how derived outcomes encode, stored and in
        // patches, must bump `value_format`: this is the layout the
        // current format writes.
        use flower_engine::storage::codec::{
            DerivedOutcome, OUTCOME_VALUE, OutcomeBody, OutcomeTime, PINNED_FLAG, encode_outcome,
        };
        let outcome = DerivedOutcome {
            pinned: true,
            code: [7u8; flower_engine::storage::keys::HASH_LEN],
            time: OutcomeTime::At(5),
            body: OutcomeBody::Value(b"v".to_vec()),
        };
        let mut layout = vec![PINNED_FLAG, OUTCOME_VALUE];
        layout.extend_from_slice(&[7u8; flower_engine::storage::keys::HASH_LEN]);
        layout.push(0x02);
        layout.extend_from_slice(&5u64.to_be_bytes());
        layout.extend_from_slice(b"v");
        assert_eq!(
            (compatibility().value_format, encode_outcome(&outcome)),
            (4, layout)
        );
    }

    #[test]
    fn files_record_what_their_bytes_depend_on() {
        let current = compatibility().clone();
        for kept in [
            Compatibility {
                raft_wire: current.raft_wire + 1,
                ..current.clone()
            },
            Compatibility {
                guest_runtime: "wasmtime-50.0.0".to_string(),
                ..current.clone()
            },
            Compatibility {
                build: "9.9.9".to_string(),
                ..current.clone()
            },
        ] {
            assert_eq!(kept.stored(), current.stored(), "{kept:?}");
        }
        for changed in [
            Compatibility {
                state_machine: current.state_machine + 1,
                ..current.clone()
            },
            Compatibility {
                snapshot_format: current.snapshot_format + 1,
                ..current.clone()
            },
            Compatibility {
                value_format: current.value_format + 1,
                ..current.clone()
            },
        ] {
            assert_ne!(changed.stored(), current.stored(), "{changed:?}");
        }
    }

    #[test]
    fn the_contract_names_the_pinned_wasm_runtime() {
        // A wasmtime upgrade must bump `guest_runtime`.
        let manifest = include_str!("../../flower-engine/Cargo.toml");
        let pinned = manifest
            .lines()
            .find(|line| line.starts_with("wasmtime = "))
            .expect("flower-engine depends on wasmtime");
        let version = compatibility()
            .guest_runtime
            .strip_prefix("wasmtime-")
            .unwrap();
        assert!(
            pinned.contains(&format!("\"={version}\"")),
            "flower-engine pins {pinned}, the contract says {version}"
        );
    }
}
