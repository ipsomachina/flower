//! Operator calls beside deployment, membership and backup (admin
//! credentials): the modules each database runs, their removal and the
//! purge of their data, staged deployments driven step by step, the
//! managed-key catalog, Raft metrics, node metrics and backup status.
//!
//! Each call is one request through the client's retry loop and
//! transport. Staged-deployment steps go to the leader (a follower
//! forwards them); listings read the addressed member.

use std::collections::BTreeMap;

use serde::Deserialize;

use crate::client::{DeployOutcome, FlowerClient, Method, RequestOptions, decode_ok};
use crate::error::FlowerError;

/// One database's active modules.
#[derive(Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct DatabaseModules {
    /// Database name.
    pub db: String,
    /// Its modules, by name.
    pub modules: Vec<ModuleInfo>,
    /// Modules whose data is being purged, by name: they cannot deploy
    /// until they leave this list.
    #[serde(default)]
    pub purging: Vec<PurgingModule>,
    /// Modules removed without a purge, by name: their data and
    /// declarations stay, so deploying one again rebuilds nothing that
    /// did not change.
    #[serde(default)]
    pub removed: Vec<RemovedModule>,
}

/// A module whose data is being purged.
#[derive(Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct PurgingModule {
    /// Module name.
    pub name: String,
    /// Keys the purge deleted so far.
    pub deleted: u64,
}

/// A module removed without a purge.
#[derive(Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct RemovedModule {
    /// Module name.
    pub name: String,
    /// The code identity it last ran, 64 hex chars.
    pub identity: String,
}

/// A committed module removal.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RemovedOutcome {
    /// The database's revision, which a removal does not move.
    pub revision: u64,
    /// The log position the removal applied at.
    pub position: u64,
    /// Whether the module's data is being purged.
    pub purging: bool,
}

/// One active module.
#[derive(Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct ModuleInfo {
    /// Module name.
    pub name: String,
    /// Active code identity, 64 hex chars.
    pub identity: Option<String>,
    /// Definition name to kind: `query`, `mutation`, `derived` or
    /// `transaction`.
    pub definitions: BTreeMap<String, String>,
    /// Collection names.
    pub collections: Vec<String>,
    /// Name callers reach over HTTP, to the definition it calls.
    pub http: BTreeMap<String, String>,
    /// Whether the module declares an authorization hook.
    pub authorize: bool,
    /// Its maintenance tasks with their failure states (empty from
    /// servers before tasks).
    #[serde(default)]
    pub tasks: Vec<TaskInfo>,
    /// A staging row the module's deployments left, if any.
    pub staging: Option<StagingInfo>,
}

/// One maintenance task, as the leader runs it.
#[derive(Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct TaskInfo {
    /// Task name.
    pub name: String,
    /// Whether failed runs go to its error callback.
    pub on_error: bool,
    /// Consecutive failed runs, zero when its last run succeeded.
    pub failures: u64,
    /// When it may run again after its failures, epoch milliseconds.
    pub retry_at_ms: Option<u64>,
    /// Its last failure, while it is failing.
    pub error: Option<TaskErrorInfo>,
}

/// A task's last failure.
#[derive(Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct TaskErrorInfo {
    /// Failure code.
    pub code: String,
    /// Failure message.
    pub message: String,
}

/// A staging row.
#[derive(Deserialize, Clone, Copy, Debug, PartialEq, Eq)]
pub struct StagingInfo {
    /// Whether the backfill and graph rebuild finished and activation
    /// can run.
    pub ready: bool,
    /// Collections in the backfill plan.
    pub planned: usize,
    /// Plan position of the backfill.
    pub position: usize,
    /// Retired structures awaiting collection.
    pub retired: usize,
    /// The phase: `building`, `rebuilding`, `ready` or `collecting`.
    #[serde(default)]
    pub phase: Option<StagingPhaseName>,
    /// Whether the staging replicates, every member holding it alike.
    #[serde(default)]
    pub replicated: bool,
    /// Derived definitions whose roots the graph phase rebuilds.
    #[serde(default)]
    pub graph: usize,
    /// Graph position of the rebuild.
    #[serde(default)]
    pub graph_position: usize,
    /// Roots the graph phase rebuilt so far.
    #[serde(default)]
    pub rebuilt: u64,
}

/// A staging's phase.
#[derive(Deserialize, Clone, Copy, Debug, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum StagingPhaseName {
    /// Building the index backfill.
    Building,
    /// Rebuilding the derived graph under the target code.
    Rebuilding,
    /// Ready to activate.
    Ready,
    /// Only collecting what deployments retired.
    Collecting,
}

/// A staged deployment at the leader after a step.
#[derive(Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct StagedDeployment {
    /// The staging row, `None` when nothing is staged.
    pub staging: Option<StagingInfo>,
    /// The staged target's code identity, while one is open.
    pub identity: Option<String>,
    /// Records the step's backfill scanned.
    pub scanned: usize,
    /// Roots the step's graph page rebuilt.
    #[serde(default)]
    pub rebuilt: usize,
}

/// The replicated managed-key catalog's public view: no key material.
#[derive(Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct KeyCatalog {
    /// The catalog's key-identifier domain, once a key exists.
    pub domain: Option<String>,
    /// The catalog revision.
    pub revision: u64,
    /// Keys by name.
    pub keys: BTreeMap<String, ManagedKey>,
    /// Guest aliases by name.
    pub bindings: BTreeMap<String, KeyBinding>,
}

/// One managed key.
#[derive(Deserialize, Clone, Debug, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ManagedKey {
    /// The key's random identity.
    pub id: String,
    /// `Ed25519`, `P256`, `RSA`, `HS256`, `A256GCM`,
    /// `XSalsa20Poly1305` or `X25519`.
    pub algorithm: String,
    /// The version new operations use.
    pub active_version: u64,
    /// Every version, oldest first.
    pub versions: Vec<KeyVersion>,
}

/// One version of a managed key.
#[derive(Deserialize, Clone, Debug, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct KeyVersion {
    /// Version number, from 1.
    pub version: u64,
    /// Refused for every use.
    pub revoked: bool,
    /// Kept to verify and decrypt only.
    pub retired: bool,
    /// Its material is gone.
    pub destroyed: bool,
    /// The wrapping key its material is sealed under.
    pub wrapping_id: Option<String>,
    /// Its JOSE key ID.
    pub kid: String,
}

/// A guest alias granted usages over a key.
#[derive(Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct KeyBinding {
    /// The bound key's name.
    pub key: String,
    /// Granted usages.
    pub usages: Vec<String>,
}

/// A committed change to a database's catalog, or its listing.
#[derive(Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct KeyChange {
    /// The revision the change produced (the current one for a list).
    pub revision: u64,
    /// The catalog after it.
    #[serde(rename = "value")]
    pub catalog: KeyCatalog,
    /// Whether the request ID had already committed this change.
    pub duplicate: bool,
}

/// A change to a database's managed-key catalog. Each runs at the
/// leader of the database's group under a caller-chosen request ID,
/// which the database scopes: sending it again answers the committed
/// change as a duplicate, and another database's request under the
/// same ID is its own.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum KeyOperation {
    /// Generate key `name`; `bits` sizes RSA keys.
    Generate {
        /// Key name.
        name: String,
        /// Algorithm name.
        algorithm: String,
        /// RSA modulus size.
        bits: Option<u64>,
    },
    /// Import key `name` from an envelope sealed to the cluster's
    /// wrapping key (`flower_crypto::replicated::seal_import`).
    Import {
        /// Key name.
        name: String,
        /// Algorithm name.
        algorithm: String,
        /// The sealed envelope.
        sealed: serde_json::Value,
    },
    /// Grant guest alias `alias` `usages` over `key`.
    Bind {
        /// Alias guests name.
        alias: String,
        /// Key name.
        key: String,
        /// Usages granted.
        usages: Vec<String>,
    },
    /// Remove guest alias `alias`.
    Unbind {
        /// Alias.
        alias: String,
    },
    /// Make a new version of `name` active; `bits` resizes RSA keys.
    Rotate {
        /// Key name.
        name: String,
        /// RSA modulus size.
        bits: Option<u64>,
    },
    /// Keep `version` of `name` (every version without one) for
    /// verifying and decrypting only.
    Retire {
        /// Key name.
        name: String,
        /// One version.
        version: Option<u64>,
    },
    /// Refuse `version` of `name` (every version without one).
    Revoke {
        /// Key name.
        name: String,
        /// One version.
        version: Option<u64>,
    },
    /// Delete the material of `version` of `name` (every version
    /// without one) for good.
    Destroy {
        /// Key name.
        name: String,
        /// One version.
        version: Option<u64>,
    },
    /// Seal `name`'s material (every key's without one) under the
    /// leader's active wrapping key.
    Rewrap {
        /// Key name.
        name: Option<String>,
    },
}

impl KeyOperation {
    /// The request `POST /v1/admin/keys` takes, for database `db`.
    fn request(&self, db: &str, request_id: &str) -> serde_json::Value {
        use serde_json::json;
        let mut request = match self {
            KeyOperation::Generate {
                name,
                algorithm,
                bits,
            } => {
                json!({"operation": "generate", "name": name, "algorithm": algorithm, "bits": bits})
            }
            KeyOperation::Import {
                name,
                algorithm,
                sealed,
            } => {
                json!({"operation": "import", "name": name, "algorithm": algorithm, "sealed": sealed})
            }
            KeyOperation::Bind { alias, key, usages } => {
                json!({"operation": "bind", "name": alias, "key": key, "usages": usages})
            }
            KeyOperation::Unbind { alias } => json!({"operation": "unbind", "name": alias}),
            KeyOperation::Rotate { name, bits } => {
                json!({"operation": "rotate", "name": name, "bits": bits})
            }
            KeyOperation::Retire { name, version } => {
                json!({"operation": "retire", "name": name, "version": version})
            }
            KeyOperation::Revoke { name, version } => {
                json!({"operation": "revoke", "name": name, "version": version})
            }
            KeyOperation::Destroy { name, version } => {
                json!({"operation": "destroy", "name": name, "version": version})
            }
            KeyOperation::Rewrap { name } => json!({"operation": "rewrap", "name": name}),
        };
        let fields = request.as_object_mut().expect("built as an object");
        // The server reads an absent member and a null one differently.
        fields.retain(|_, value| !value.is_null());
        fields.insert("db".to_string(), db.into());
        fields.insert("requestId".to_string(), request_id.into());
        request
    }
}
#[derive(Deserialize)]
struct ModulesBody {
    databases: Vec<DatabaseModules>,
}

#[derive(Deserialize)]
struct CanceledBody {
    canceled: bool,
}

#[derive(Deserialize)]
struct CollectedRetiredBody {
    collected: usize,
    done: bool,
}

#[derive(Deserialize)]
struct RaftBody {
    raft: serde_json::Value,
}

#[derive(Deserialize)]
struct ActivatedBody {
    revision: Option<u64>,
    position: Option<u64>,
}

#[derive(Deserialize)]
struct RemovedBody {
    revision: Option<u64>,
    position: Option<u64>,
    value: RemovedValue,
}

#[derive(Deserialize)]
struct RemovedValue {
    purging: bool,
}

/// Percent-encode a query-string value.
pub(crate) fn encode_query(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for byte in value.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'.' | b'_' | b'~' => {
                out.push(byte as char);
            }
            _ => out.push_str(&format!("%{byte:02X}")),
        }
    }
    out
}

impl FlowerClient {
    /// Every database's active modules, or `db`'s alone, after the
    /// member proved a fresh read.
    pub async fn modules(
        &self,
        db: Option<&str>,
        options: &RequestOptions,
    ) -> Result<Vec<DatabaseModules>, FlowerError> {
        let path = match db {
            Some(db) => format!("/v1/admin/modules?db={}", encode_query(db)),
            None => "/v1/admin/modules".to_string(),
        };
        let body = self.send(Method::Get, &path, None, options).await?;
        decode_ok::<ModulesBody>(&body).map(|body| body.databases)
    }

    /// Stage a deployment at the leader without activating it.
    pub async fn stage_deployment(
        &self,
        db: &str,
        module: &str,
        identity: Option<[u8; 32]>,
        wasm: &[u8],
        manifest: &serde_json::Value,
        options: &RequestOptions,
    ) -> Result<StagedDeployment, FlowerError> {
        use base64::Engine;
        let identity = identity.map(|bytes| {
            bytes
                .iter()
                .map(|byte| format!("{byte:02x}"))
                .collect::<String>()
        });
        self.deployment_step(
            serde_json::json!({
                "operation": "stage",
                "db": db,
                "module": module,
                "identity": identity,
                "wasm_b64": base64::prelude::BASE64_STANDARD.encode(wasm),
                "manifest": manifest,
            }),
            options,
        )
        .await
    }

    /// Build up to `max_records` more of the staging's backfill, or once
    /// it is built, rebuild as many roots of its derived graph (the
    /// server's default page without one).
    pub async fn advance_deployment(
        &self,
        db: &str,
        module: &str,
        max_records: Option<usize>,
        options: &RequestOptions,
    ) -> Result<StagedDeployment, FlowerError> {
        self.deployment_step(
            serde_json::json!({
                "operation": "advance",
                "db": db,
                "module": module,
                "max_records": max_records,
            }),
            options,
        )
        .await
    }

    /// The leader's staging of a module.
    pub async fn deployment_status(
        &self,
        db: &str,
        module: &str,
        options: &RequestOptions,
    ) -> Result<StagedDeployment, FlowerError> {
        self.deployment_step(
            serde_json::json!({"operation": "status", "db": db, "module": module}),
            options,
        )
        .await
    }

    /// Activate the leader's ready staging through consensus.
    pub async fn activate_deployment(
        &self,
        db: &str,
        module: &str,
        options: &RequestOptions,
    ) -> Result<DeployOutcome, FlowerError> {
        let body = self
            .send(
                Method::Post,
                "/v1/admin/deployments",
                Some(&serde_json::json!({"operation": "activate", "db": db, "module": module})),
                options,
            )
            .await?;
        let body: ActivatedBody = decode_ok(&body)?;
        Ok(DeployOutcome {
            revision: body.revision.unwrap_or(0),
            position: body.position.unwrap_or(0),
        })
    }

    /// Cancel the leader's staging of a module; whether one was open.
    pub async fn cancel_deployment(
        &self,
        db: &str,
        module: &str,
        options: &RequestOptions,
    ) -> Result<bool, FlowerError> {
        let body = self
            .send(
                Method::Post,
                "/v1/admin/deployments",
                Some(&serde_json::json!({"operation": "cancel", "db": db, "module": module})),
                options,
            )
            .await?;
        decode_ok::<CanceledBody>(&body).map(|body| body.canceled)
    }

    /// Delete up to `max_keys` of the structures the module's
    /// deployments retired, on every member alike (the server's default
    /// page without one): the keys deleted, and whether nothing retired
    /// remains. The leader's maintenance collects them on its own too.
    pub async fn collect_deployment(
        &self,
        db: &str,
        module: &str,
        max_keys: Option<usize>,
        options: &RequestOptions,
    ) -> Result<(usize, bool), FlowerError> {
        let body = self
            .send(
                Method::Post,
                "/v1/admin/deployments",
                Some(&serde_json::json!({
                    "operation": "collect",
                    "db": db,
                    "module": module,
                    "max_keys": max_keys,
                })),
                options,
            )
            .await?;
        let body: CollectedRetiredBody = decode_ok(&body)?;
        Ok((body.collected, body.done))
    }

    /// Remove `module` from `db` through consensus at the leader (a
    /// follower forwards): its code, HTTP names, hooks and derived values
    /// go on every member and synced device, and its data stays with its
    /// declarations, so deploying it again rebuilds nothing that did not
    /// change. Refused (`MODULE_CONFLICT`) for a module not deployed, or
    /// one another deployed module imports from.
    pub async fn remove_module(
        &self,
        db: &str,
        module: &str,
        options: &RequestOptions,
    ) -> Result<RemovedOutcome, FlowerError> {
        self.removal(db, module, false, options).await
    }

    /// Remove `module` from `db` and purge its data, or purge what an
    /// earlier removal of it kept: the group's leader deletes it a page
    /// at a time in replicated entries, and the module cannot deploy
    /// until [`DatabaseModules::purging`] no longer names it. Asked while
    /// its purge is in flight, it changes nothing.
    pub async fn purge_module(
        &self,
        db: &str,
        module: &str,
        options: &RequestOptions,
    ) -> Result<RemovedOutcome, FlowerError> {
        self.removal(db, module, true, options).await
    }

    /// `POST /v1/admin/modules {"operation": "remove", ...}`.
    async fn removal(
        &self,
        db: &str,
        module: &str,
        purge: bool,
        options: &RequestOptions,
    ) -> Result<RemovedOutcome, FlowerError> {
        let body = self
            .send(
                Method::Post,
                "/v1/admin/modules",
                Some(&serde_json::json!({
                    "operation": "remove",
                    "db": db,
                    "module": module,
                    "purge": purge,
                })),
                options,
            )
            .await?;
        let body: RemovedBody = decode_ok(&body)?;
        Ok(RemovedOutcome {
            revision: body.revision.unwrap_or(0),
            position: body.position.unwrap_or(0),
            purging: body.value.purging,
        })
    }

    /// Database `db`'s managed-key catalog as the leader of its group
    /// applied it (a follower forwards).
    pub async fn keys(&self, db: &str, options: &RequestOptions) -> Result<KeyChange, FlowerError> {
        let path = format!("/v1/admin/keys?db={}", encode_query(db));
        let body = self.send(Method::Get, &path, None, options).await?;
        serde_json::from_slice(&body).map_err(|error| FlowerError::Decode(error.to_string()))
    }

    /// Every database's managed-key catalog in the addressed group (the
    /// root's, unless the client names another), by database name;
    /// databases without one are left out.
    pub async fn key_catalogs(
        &self,
        options: &RequestOptions,
    ) -> Result<BTreeMap<String, KeyCatalog>, FlowerError> {
        #[derive(Deserialize)]
        struct Listed {
            databases: BTreeMap<String, ListedCatalog>,
        }
        #[derive(Deserialize)]
        struct ListedCatalog {
            value: KeyCatalog,
        }
        let body = self
            .send(Method::Get, "/v1/admin/keys", None, options)
            .await?;
        let listed: Listed = serde_json::from_slice(&body)
            .map_err(|error| FlowerError::Decode(error.to_string()))?;
        Ok(listed
            .databases
            .into_iter()
            .map(|(db, listed)| (db, listed.value))
            .collect())
    }

    /// Change database `db`'s managed-key catalog at the leader of its
    /// group under `request_id`. A refused change (an unknown key, a
    /// usage its algorithm lacks, a request ID reused for other
    /// contents) is an HTTP 400 naming its `KEY` code or reason; a
    /// database moving between groups is refused as moving.
    pub async fn change_keys(
        &self,
        db: &str,
        operation: &KeyOperation,
        request_id: &str,
        options: &RequestOptions,
    ) -> Result<KeyChange, FlowerError> {
        let body = self
            .send(
                Method::Post,
                "/v1/admin/keys",
                Some(&operation.request(db, request_id)),
                options,
            )
            .await?;
        serde_json::from_slice(&body).map_err(|error| FlowerError::Decode(error.to_string()))
    }

    /// The addressed member's prepared-key cache statistics.
    pub async fn key_cache(
        &self,
        options: &RequestOptions,
    ) -> Result<serde_json::Value, FlowerError> {
        let body = self
            .send(
                Method::Post,
                "/v1/admin/keys",
                Some(&serde_json::json!({"operation": "cache"})),
                options,
            )
            .await?;
        serde_json::from_slice(&body).map_err(|error| FlowerError::Decode(error.to_string()))
    }

    /// OpenRaft's metrics for the addressed member, as JSON.
    pub async fn raft_metrics(
        &self,
        options: &RequestOptions,
    ) -> Result<serde_json::Value, FlowerError> {
        let body = self
            .send(Method::Get, "/v1/admin/raft", None, options)
            .await?;
        decode_ok::<RaftBody>(&body).map(|body| body.raft)
    }

    /// The addressed member's node metrics (`/v1/metrics`), as JSON:
    /// admission, reads, compaction, log purge, snapshots and more.
    pub async fn node_metrics(
        &self,
        options: &RequestOptions,
    ) -> Result<serde_json::Value, FlowerError> {
        let body = self.send(Method::Get, "/v1/metrics", None, options).await?;
        serde_json::from_slice(&body).map_err(|error| FlowerError::Decode(error.to_string()))
    }

    /// The addressed member's write and watch activity snapshot, as
    /// JSON (`GET /v1/admin/activity`).
    pub async fn activity(
        &self,
        options: &RequestOptions,
    ) -> Result<serde_json::Value, FlowerError> {
        let body = self
            .send(Method::Get, "/v1/admin/activity", None, options)
            .await?;
        serde_json::from_slice(&body).map_err(|error| FlowerError::Decode(error.to_string()))
    }

    /// Continuous backup on the addressed member, as JSON.
    pub async fn backup_status(
        &self,
        options: &RequestOptions,
    ) -> Result<serde_json::Value, FlowerError> {
        let body = self
            .send(Method::Get, "/v1/admin/backup", None, options)
            .await?;
        decode_ok(&body)
    }

    /// One staged-deployment step.
    async fn deployment_step(
        &self,
        request: serde_json::Value,
        options: &RequestOptions,
    ) -> Result<StagedDeployment, FlowerError> {
        let body = self
            .send(
                Method::Post,
                "/v1/admin/deployments",
                Some(&request),
                options,
            )
            .await?;
        decode_ok(&body)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn query_values_are_percent_encoded() {
        assert_eq!(encode_query("db"), "db");
        assert_eq!(encode_query("a b&c=d/é"), "a%20b%26c%3Dd%2F%C3%A9");
    }
}
