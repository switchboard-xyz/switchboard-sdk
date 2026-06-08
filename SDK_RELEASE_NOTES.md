# SDK Release Notes

This file is the canonical release notes ledger for Switchboard SDK releases.
The newest release batch should stay at the top.

## 2026-06-08 Release Prep

### `@switchboard-xyz/on-demand@3.10.4`

Status: prepared; dry-run verified.

- Fixed Solana network resolution so official Switchboard program IDs resolve to the correct cluster defaults.
- Fixed randomness oracle eligibility so healthy pull oracles are not rejected only because `enable_gateway = 0`; `enable_pull_oracle` remains the relevant eligibility gate.
- Includes the merged `sbv3#1015` randomness eligibility fix and smoke-test coverage.

### `switchboard-on-demand-client@0.6.0`

Status: prepared; dry-run verified.

- Updated the client crate for Solana 3.x crate compatibility.
- Fixed release guardrail paths so crate packaging checks use the `solana/rust/switchboard-on-demand-client` mirror layout.

### `switchboard-on-demand@0.12.2`

Status: prepared; dry-run verified.

- Preserved Crossbar feed simulation results when building update instructions.
- Fixed Sui result deserialization and removed noisy client logs.
- Kept the SBF-safe `libsecp256k1` feature split for Solana builds.
