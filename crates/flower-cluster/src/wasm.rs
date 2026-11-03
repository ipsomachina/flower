//! `WasmFactory`: the production runtime factory over wasmtime.

use flower_engine::runtime::Runtime;
use flower_engine::runtime::wasm::{Limits, WasmBackend};
use flower_sdk::Value;

use crate::statemachine::RuntimeFactory;

/// A factory compiling each module's Wasm bytes into its own
/// fuel-metered backend. Registering under the deployed module
/// name is what lets later calls find their code.
#[derive(Clone, Default)]
pub struct WasmFactory {
    crypto: Option<std::sync::Arc<dyn flower_sdk::crypto::CryptoBackend>>,
    limits: Limits,
}

impl std::fmt::Debug for WasmFactory {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WasmFactory")
            .field("crypto", &self.crypto.is_some())
            .field("limits", &self.limits)
            .finish()
    }
}

impl WasmFactory {
    /// Attach the crypto dispatch for guests importing
    /// `crypto_call`.
    pub fn with_crypto(crypto: std::sync::Arc<dyn flower_sdk::crypto::CryptoBackend>) -> Self {
        WasmFactory {
            crypto: Some(crypto),
            limits: Limits::default(),
        }
    }

    /// Run every backend this factory builds under `limits`: guest
    /// calls, and `flower_init` when a module registers.
    pub fn with_limits(mut self, limits: Limits) -> Self {
        self.limits = limits;
        self
    }

    /// The limits backends run under.
    pub fn limits(&self) -> &Limits {
        &self.limits
    }
}

impl RuntimeFactory for WasmFactory {
    fn build(
        &self,
        db: &[u8],
        module: &str,
        wasm: &[u8],
        manifest: &Value,
    ) -> Result<Box<dyn Runtime>, String> {
        let mut backend = WasmBackend::with_limits(self.limits.clone())
            .map_err(|failure| format!("{}: {}", failure.code, failure.message))?;
        if let Some(crypto) = &self.crypto {
            // Scope the crypto host to the module's database and key
            // declarations: malformed declarations reject the module
            // here, at deploy.
            let scoped = crypto
                .for_module(db, module, manifest)
                .map_err(|failure| format!("{}: {}", failure.code, failure.message))?;
            backend.set_crypto(scoped.map_or_else(|| crypto.clone(), std::sync::Arc::from));
        }
        backend
            .register(module, wasm)
            .map_err(|failure| format!("{}: {}", failure.code, failure.message))?;
        Ok(Box::new(backend))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A minimal guest: imports `flower.host_call`, exports the ABI over
    /// one page of memory, and answers nothing.
    fn one_page_guest() -> Vec<u8> {
        let mut wasm = b"\0asm\x01\0\0\0".to_vec();
        // Types: 0 = (i32 i32 i32) -> i64, 1 = (i32) -> i32, 2 = () -> i64.
        wasm.extend([
            0x01, 0x11, 0x03, 0x60, 0x03, 0x7f, 0x7f, 0x7f, 0x01, 0x7e, 0x60, 0x01, 0x7f, 0x01,
            0x7f, 0x60, 0x00, 0x01, 0x7e,
        ]);
        // Import 0: flower.host_call.
        wasm.extend([0x02, 0x14, 0x01, 0x06]);
        wasm.extend(b"flower");
        wasm.push(0x09);
        wasm.extend(b"host_call");
        wasm.extend([0x00, 0x00]);
        // Functions 1 alloc, 2 invoke, 3 manifest; one page of memory.
        wasm.extend([0x03, 0x04, 0x03, 0x01, 0x00, 0x02]);
        wasm.extend([0x05, 0x03, 0x01, 0x00, 0x01]);
        let exports: [(&str, u8, u8); 4] = [
            ("memory", 2, 0),
            ("flower_alloc", 0, 1),
            ("flower_invoke", 0, 2),
            ("flower_manifest", 0, 3),
        ];
        let mut section = vec![exports.len() as u8];
        for (name, kind, index) in exports {
            section.push(name.len() as u8);
            section.extend(name.as_bytes());
            section.extend([kind, index]);
        }
        wasm.push(0x07);
        wasm.push(section.len() as u8);
        wasm.extend(section);
        // Bodies: alloc returns 0; invoke and manifest return 0.
        wasm.extend([
            0x0a, 0x10, 0x03, 0x04, 0x00, 0x41, 0x00, 0x0b, 0x04, 0x00, 0x42, 0x00, 0x0b, 0x04,
            0x00, 0x42, 0x00, 0x0b,
        ]);
        wasm
    }

    #[test]
    fn backends_run_under_the_factory_limits() {
        let wasm = one_page_guest();
        assert!(
            WasmFactory::default()
                .build(b"db", "app", &wasm, &Value::Null)
                .is_ok()
        );
        // One 64 KiB page is past a 32 KiB instance limit.
        let tight = WasmFactory::default().with_limits(Limits {
            instance_memory: 32 * 1024,
            ..Limits::default()
        });
        assert_eq!(tight.limits().instance_memory, 32 * 1024);
        let error = tight
            .build(b"db", "app", &wasm, &Value::Null)
            .err()
            .unwrap();
        assert!(error.starts_with("out_of_memory"), "{error}");
    }
}
