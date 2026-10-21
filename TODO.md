# TODO

## Goal
Review and update project documentation (`README.md` and all files in `docs/`) to fix inaccuracies, align code snippets and environment variables with the codebase, and correct typographical and grammatical errors.

## Tasks

### 1. Fix User Documentation
- [x] Update `docs/users/getting-started.md` to correct typos, grammar, and host mount path references.
- [x] Update `docs/users/configuration.md` to change `SANDBOX_ENABLE` / `SANDBOX=true` to `SANDBOX_ENABLED`, fix typos, and ensure all configuration options match `src/config.rs`.
- [x] Update `docs/users/data-retention.md` to add missing hypertables (`killmails`, rift events), fix typos, and ensure consistency.

### 2. Fix Developer Documentation
- [x] Update `docs/developers/architecture.md` to fix minor grammatical and phrasing issues.
- [x] Update `docs/developers/world_contracts.md` to add `rifts/` to the world handlers directory structure and verify helper methods match `src/lib.rs`.
- [x] Update `docs/developers/database.md` to clarify model and migration descriptions.
- [ ] Update `docs/developers/parsing-move-objects.md` to align code snippets with the current codebase (proper Move event fields, `anyhow::Result` error handling, and safe unwrap patterns).
- [ ] Update `docs/developers/emitters-and-transports.md` to verify transport descriptions match implementations.

### 3. Fix Project Root README
- [ ] Update `README.md` to add the missing link to `parsing-move-objects.md`, fix grammatical issues and link formatting.

## Notes
- Do not make changes anywhere else in the repository outside of `README.md` and `docs/`.
- Ensure all environment variable names, hypertable names, and struct definitions strictly mirror the Rust source code and database migrations.
