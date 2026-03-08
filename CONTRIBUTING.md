# Contributing to resync-rs

Thank you for your interest in contributing to resync-rs! This document provides guidelines and instructions for contributing.

## Getting Started

1. **Fork** the repository on GitHub
2. **Clone** your fork locally:
   ```bash
   git clone https://github.com/vedLinuxian/resync-rs.git
   cd resync-rs
   ```
3. **Create a branch** for your feature or fix:
   ```bash
   git checkout -b feature/my-feature
   ```

## Development Setup

### Prerequisites

- Rust 1.75+ (`rustup update stable`)
- Linux or macOS

### Building

```bash
# Debug build
cargo build

# Release build (optimized)
cargo build --release

# Run tests
cargo test
bash tests/e2e_test.sh
bash tests/network_test.sh
```

## Code Guidelines

### Style

- Follow standard Rust formatting: `cargo fmt`
- No clippy warnings: `cargo clippy -- -D warnings`
- Write doc comments for all public items
- Use `thiserror` for error types

### Architecture Principles

- **Parallelism first** — use rayon for CPU-bound work, tokio for I/O
- **Zero-copy** — prefer `mmap` over `read()` for large files
- **Batched work** — send work to rayon in chunks of 128+, never 1-at-a-time
- **Atomic writes** — always write to tmp file, then rename

### Testing

- Every new feature must include unit tests
- Bug fixes should include a regression test
- Run the full suite before submitting:
  ```bash
  cargo test && bash tests/e2e_test.sh && bash tests/network_test.sh
  ```

## Pull Request Process

1. Ensure all tests pass
2. Update documentation if needed
3. Add a clear description of what the PR does and why
4. Reference any related issues
5. Keep PRs focused — one feature or fix per PR

## Reporting Issues

- Use GitHub Issues
- Include: OS, Rust version (`rustc --version`), steps to reproduce
- For performance issues, include benchmark output from `tests/e2e_test.sh`

## Code of Conduct

Be respectful, constructive, and welcoming. We follow the [Rust Code of Conduct](https://www.rust-lang.org/policies/code-of-conduct).

## License

By contributing, you agree that your contributions will be licensed under the MIT License.
