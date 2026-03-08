# Security Policy

## Supported Versions

| Version | Supported |
|---------|-----------|
| 0.1.x   | ✅        |

## Reporting a Vulnerability

If you discover a security vulnerability in resync-rs, please report it responsibly:

1. **Do NOT** open a public GitHub issue
2. Email **linuxlover94@gmail.com** with:
   - Description of the vulnerability
   - Steps to reproduce
   - Potential impact
3. You will receive a response within 48 hours
4. A fix will be developed privately and released as a patch

## Security Considerations

- **TLS 1.3**: Network mode uses `tokio-rustls` with modern cipher suites
- **No unsafe code**: The codebase uses only safe Rust (except in dependencies)
- **BLAKE3**: Cryptographic hash function — collision-resistant and faster than SHA-256
- **Atomic writes**: Files are written to temp paths and renamed, preventing partial writes
