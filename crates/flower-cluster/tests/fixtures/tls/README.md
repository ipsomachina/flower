# Peer TLS test fixtures

Test-only credentials for `tests/membership.rs`. Never use them outside tests.

- `ca.crt`: the trusted root, `CN=Flower test CA` (its key was discarded).
- `node.crt`, `node.key`: a leaf for `DNS:localhost` and `IP:127.0.0.1`, signed by `ca.crt`; PKCS#8 P-256 key.
- `wrong.crt`, `wrong.key`: a leaf for `DNS:wrong.example` only, signed by `ca.crt`.
- `untrusted.crt`: the `node.key` leaf for the same names, signed by a root nobody trusts (`CN=Untrusted test CA`, discarded).

All expire in 2126. They were made with OpenSSL: P-256 keys (`openssl ecparam -name prime256v1 -genkey` then `openssl pkcs8 -topk8 -nocrypt`), self-signed roots with `basicConstraints=critical,CA:TRUE` and `keyUsage=critical,keyCertSign,cRLSign`, and leaves with `basicConstraints=critical,CA:FALSE`, `keyUsage=critical,digitalSignature`, `extendedKeyUsage=serverAuth` and the subject alternative names above, each `openssl x509 -req -days 36500 -sha256`.
