# Client TLS test fixtures

Test-only credentials for `tests/transport.rs`, copied from
`crates/flower-cluster/tests/fixtures/tls` (see its README for how they
were made). Never use them outside tests.

- `ca.crt`: the trusted root, `CN=Flower test CA` (its key was discarded).
- `node.crt`, `node.key`: a leaf for `DNS:localhost` and `IP:127.0.0.1`, signed by `ca.crt`; PKCS#8 P-256 key.
- `untrusted.crt`: the `node.key` leaf for the same names, signed by a root nobody trusts.

All expire in 2126.
