//! Discover optional peer capabilities before depending on them.
use crate::{FlowerClient, FlowerError, RequestOptions};
pub use flower_protocol::Capability;

#[derive(Clone, Debug)]
pub struct ProtocolRequirements {
    pub versions: std::ops::RangeInclusive<u32>,
    pub capabilities: Vec<Capability>,
}
impl Default for ProtocolRequirements {
    fn default() -> Self {
        Self {
            versions: flower_protocol::VERSION..=flower_protocol::VERSION,
            capabilities: Vec::new(),
        }
    }
}
impl ProtocolRequirements {
    pub fn requiring(capabilities: impl IntoIterator<Item = Capability>) -> Self {
        Self {
            capabilities: capabilities.into_iter().collect(),
            ..Self::default()
        }
    }
}
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct CompatibilityReport {
    pub peer_version: u32,
    pub minimum_version: u32,
    pub maximum_version: u32,
    pub implemented_version: u32,
    pub missing: Vec<Capability>,
    pub unknown: Vec<String>,
    pub compatible: bool,
}
impl std::fmt::Display for CompatibilityReport {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "peer version {}; required {}..={}; client implements {}; missing capabilities: {:?}",
            self.peer_version,
            self.minimum_version,
            self.maximum_version,
            self.implemented_version,
            self.missing
        )
    }
}
#[derive(Debug)]
pub enum NegotiationError {
    Request(Box<FlowerError>),
    Incompatible(CompatibilityReport),
}
impl std::fmt::Display for NegotiationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Request(error) => error.fmt(f),
            Self::Incompatible(report) => report.fmt(f),
        }
    }
}
impl std::error::Error for NegotiationError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Request(error) => Some(error.as_ref()),
            _ => None,
        }
    }
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct ProtocolInfo {
    pub version: u32,
    pub capabilities: Vec<String>,
}
impl ProtocolInfo {
    pub fn supports(&self, capability: Capability) -> bool {
        self.capabilities
            .iter()
            .any(|name| name == capability.as_str())
    }
    pub fn compatibility(&self, requirements: &ProtocolRequirements) -> CompatibilityReport {
        let missing: Vec<_> = requirements
            .capabilities
            .iter()
            .copied()
            .filter(|capability| !self.supports(*capability))
            .collect::<std::collections::BTreeSet<_>>()
            .into_iter()
            .collect();
        let unknown = self
            .capabilities
            .iter()
            .filter(|name| {
                !Capability::ALL
                    .iter()
                    .any(|known| known.as_str() == name.as_str())
            })
            .cloned()
            .collect();
        CompatibilityReport {
            peer_version: self.version,
            minimum_version: *requirements.versions.start(),
            maximum_version: *requirements.versions.end(),
            compatible: self.version == flower_protocol::VERSION
                && requirements.versions.contains(&self.version)
                && missing.is_empty(),
            implemented_version: flower_protocol::VERSION,
            missing,
            unknown,
        }
    }
    pub fn require(&self, capabilities: &[&str]) -> Result<(), FlowerError> {
        if self.version != flower_protocol::VERSION {
            return Err(FlowerError::Unsupported(format!(
                "peer protocol version {}",
                self.version
            )));
        }
        for capability in capabilities {
            if !self.capabilities.iter().any(|c| c == capability) {
                return Err(FlowerError::Unsupported(format!(
                    "peer capability {capability}"
                )));
            }
        }
        Ok(())
    }
}
impl FlowerClient {
    /// Inspect an unsupported peer for rolling-upgrade diagnostics without accepting its calls.
    pub async fn inspect_protocol(
        &self,
        options: &RequestOptions,
    ) -> Result<ProtocolInfo, FlowerError> {
        let bytes = self
            .send(crate::client::Method::Get, "/v1/protocol", None, options)
            .await?;
        let info: flower_protocol::Discovery =
            serde_json::from_slice(&bytes).map_err(|e| FlowerError::Decode(e.to_string()))?;
        Ok(ProtocolInfo {
            version: info.version,
            capabilities: info.capabilities,
        })
    }
    pub async fn negotiate(
        &self,
        requirements: &ProtocolRequirements,
        options: &RequestOptions,
    ) -> Result<ProtocolInfo, NegotiationError> {
        let info = self
            .inspect_protocol(options)
            .await
            .map_err(|e| NegotiationError::Request(Box::new(e)))?;
        let report = info.compatibility(requirements);
        if !report.compatible {
            return Err(NegotiationError::Incompatible(report));
        }
        Ok(info)
    }
    pub async fn protocol(&self, options: &RequestOptions) -> Result<ProtocolInfo, FlowerError> {
        let info = self.inspect_protocol(options).await?;
        info.require(&[])?;
        Ok(info)
    }
}
