# Runtime Maintenance and Verified Continuation

Read this only when a repair requires plugin source activation. It does not grant
permission to restart the Gateway or bypass public V2 tools.

1. Determine what changed. File-backed `SKILL.md` edits refresh through the skills
   watcher; verify the effective text with `skills_read`. Do not hot-reload runtime
   code merely to deliver a skill edit. For compiled code changes, run the focused
   tests and build before requesting reload; reload does not build source.
   **Done when:** the changed delivery boundary and validation result are known.
2. Check for a pending/finished owned reload and its receipt before starting another.
   Record exact run IDs, completed effects (especially accepted metadata), approval
   scope, the next workflow stage, and owned process/receipt location in a durable
   checkpoint outside any disposable capture. Avoid unrelated transcript/secrets.
   **Done when:** a resumed turn can continue without redoing completed operations.
3. Runtime reload may wait for the current turn's retained plugin callbacks to be
   released. Do not synchronously wait inside the very turn holding that plugin.
   Use an available supported outside-turn/background operation and its documented
   completion delivery. Verify a same-session continuation/notification was actually
   accepted and name its owner/trigger; do not invent a daemon, poll indefinitely,
   use messaging RPCs through exec, or treat `sessions_yield: nothing_pending` as a
   registered continuation. If delivery cannot be established, report that specific
   mechanism gap and preserve the checkpoint without promising automatic progress.
   **Done when:** a real owner and accepted resume trigger exist, or the delivery gap
   is explicitly documented. A background PID alone is not enough.
4. On continuation, inspect the saved receipt and current authoritative state.
   Verify success, `restartRequired:false`, and selected source/capture digests when
   code changed. A capture directory ID or successful request admission is not
   version proof. If reload failed, inspect its cause before retrying; if it reports
   a required Gateway restart, follow the workspace's live-session restart preflight
   rather than restarting over active work.
   **Done when:** the changed code is proved active, or the actual failure is known.
5. Run the checkpoint's next public V2 operation in the same task, using fresh
   status/returned params and the existing scoped approval. After accepted metadata
   plus a terminal planning failure, start a fresh same-flow preview; never accept
   the metadata twice. Inspect the new plan, continue authorized placement, then
   verify protected files/DB state and remaining work.
   **Done when:** the dependent step actually started and its outcome is verified.
   Reload completion alone is never the task's completion report.
