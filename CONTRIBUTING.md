# Contributing

joons-allocator is a 0.1.0 preview: the API may still change, and every
change should keep the project small, standard-library only, and honest
about its guarantees (`docs/guarantees.md` is the contract, not decoration).

## Setup

```
python -m pip install -e ".[test]"
python -m pytest -q
```

Python 3.9+ (the core keeps a 3.9 syntax floor). To work on the optional
guard integration, add the extra:

```
python -m pip install -e ".[test,guard]"
```

## Before submitting a change

1. Run the full suite (`python -m pytest -q`) on your default Python, and on
   3.9 if available.
2. Run every example — each `examples/*.py` must exit 0 — and smoke-test the
   CLI: `python -m joons_allocator --help`.
3. If behaviour changed, update `docs/guarantees.md` and `CHANGELOG.md` in
   the same change.
4. Keep tests deterministic: temp directories only, `spawn` start method for
   process races, no network access in tests.

## House rules

- The core package imports the standard library only. The single permitted
  external import is the lazy `joons_agent_guard` import inside
  `joons_allocator.integrations.guard`; nothing else may reach for it.
- Every state change is one `BEGIN IMMEDIATE` transaction on one SQLite
  file; nothing expires by age; unknown outcomes (`pending`, `running`) are
  operator decisions, never automatic retries.
- Do not modify `vendor/` — it stands in for the released guard.
- The security model in `SECURITY.md` (no caller authentication, database
  file permissions are the caller's job, append-only audit) is part of the
  design; changes must not weaken it.
- Keep pull requests small and focused; one behaviour per change.
