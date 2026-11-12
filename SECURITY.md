# Security policy

joons-allocator is a 0.1.0 preview. It has not been audited; do not point it
at untrusted users or untrusted input.

## Security model (same as joons-agent-guard)

- **No caller authentication.** The allocator does not authenticate anybody.
  `principal` must come from a trusted transport (mTLS identity, verified
  signature, ...), never from a client-supplied header. Whoever can reach
  the API can submit tasks under any principal.
- **Database file permissions are the caller's job.** The whole allocator
  lives in one SQLite file (tasks, submissions, workers, audit trail).
  Anyone who can read it sees everything; anyone who can write it controls
  everything. Restrict file permissions like you would any credential
  store.
- **Audit rows are append-only through the API.** `task_audit` has no update
  or delete path. That is an API property, not tamper-proofing: a writer of
  the file can still modify rows, so the integrity of the trail is only as
  strong as the file permissions above.
- **Bounded input.** Identifiers are capped at 1024 characters, results at
  1 MiB of JSON-encodable text, reasons at 4096 characters, and payloads
  must be strict JSON (no NaN/Infinity, string keys only).

## Reporting

Report vulnerabilities through GitHub Security Advisories ("Report a
vulnerability" on this repository). Please do not use e-mail and do not open
public issues for security problems; include a minimal reproduction and the
affected code path.
