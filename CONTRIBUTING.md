# Contributing

1. Fork & branch from `main` using `feat/<slug>` or `fix/<slug>`.
2. `make setup` to install dev dependencies and pre-commit hooks.
3. Keep functions small, typed, and tested. Coverage gate is 80%.
4. Conventional Commits (`feat:`, `fix:`, `docs:`, `refactor:`, `test:`, `chore:`).
5. All CI checks (lint, type-check, unit, integration, docker build) must pass.
6. Add an ADR under `docs/adr/` for any architecturally significant decision.
