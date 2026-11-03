//! Structural contract fingerprints for persisted commands. Rust type names are not wire contracts.
use crate::FlowerError;
use flower_sdk::{BusinessError, FlowerType, schema::Schema};
use sha2::{Digest, Sha256};

pub const COMMAND_VERSION: u32 = 2;

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CommandContract {
    pub arguments: String,
    pub result: String,
    pub errors: String,
}
pub(crate) fn fingerprint(schema: Schema) -> Result<String, FlowerError> {
    let value = schema
        .to_value()
        .map_err(|e| FlowerError::Decode(e.message))?;
    let bytes = value
        .encode_binary()
        .map_err(|e| FlowerError::Decode(e.message))?;
    let mut hash = Sha256::new();
    hash.update(b"flower-contract-v1\0");
    hash.update(bytes);
    Ok(flower_protocol::hex(&hash.finalize()))
}
impl CommandContract {
    pub fn of<A: FlowerType, T: FlowerType, E: BusinessError>() -> Result<Self, FlowerError> {
        Ok(Self {
            arguments: fingerprint(A::SCHEMA)?,
            result: fingerprint(T::SCHEMA)?,
            errors: fingerprint(E::SCHEMA)?,
        })
    }
    pub(crate) fn validate_result<T: FlowerType, E: BusinessError>(
        &self,
    ) -> Result<(), FlowerError> {
        if self.result != fingerprint(T::SCHEMA)? || self.errors != fingerprint(E::SCHEMA)? {
            return Err(FlowerError::Unsupported("persisted command result or error contract changed; migrate or reconcile the original intent explicitly".into()));
        }
        Ok(())
    }
}
