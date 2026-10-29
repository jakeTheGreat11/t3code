# Codex

Use your ChatGPT plan or an existing Codex CLI login to code in T3 Code.

## Delegate with Multi-Agent mode

On web or desktop, choose a Codex coordinator model and reasoning setting in the
composer. Open **Multi-Agent** and choose **Auto** to let the coordinator select
each worker's model and reasoning settings from that Codex account's available
models. **Budget** favors economy, **Balanced** balances capability and expense,
and **Maximum** favors quality for demanding tasks. These guide the coordinator's
judgment; they are not measured price or performance rankings.

Choose **Custom** instead to assign a Codex model and reasoning options to
**Worker** yourself. Other roles are optional; any assigned roles must also use
Codex. Other providers are not supported for automatic delegation yet.

Send your goal normally. The coordinator decides whether independent workers will
help; you do not need to choose a worker count or divide the work. Up to two workers
can run concurrently, with four tasks total for one coordinator. Workers share the
project workspace, so review their changes together before committing.

Workers appear beneath the coordinator in the sidebar. The delegation summary
shows worker progress, selected models, and links to results. Once the outstanding
batch settles, the coordinator automatically reviews the results and performs
integration and verification. **Stop Coordinator** disables new delegation and
automatic review; already-committed workers can finish and their results remain
available.

Auto records each task's model and options before launching it. The worker
conversation and its result notification show that selection. If a model offered
to the coordinator disappears before the task is admitted, T3 can use the
coordinator's saved selection when it is still available. Invalid options are
rejected. Once a task is admitted, T3 keeps its selection across restarts and
will not submit it if that selection becomes unavailable. Such a task can remain
queued until the selection is available again; T3 does not silently switch models.

**Stop coordinator** prevents new delegation. Workers already authorized to submit
their assignments may finish, and their conversations and results remain available.
Stop an individual worker separately if needed. Later manual turns in a worker
conversation are independent of its original assignment.

## Connect with ChatGPT

Connect during onboarding or in **Settings → Providers**. For a remote machine,
select that environment first. T3 Code handles Codex installation; sign in on
OpenAI and allow sharing of your ChatGPT plan.

Manage shared usage and credits in ChatGPT through **Manage usage** in T3 Code.
If a request uses a feature that ChatGPT sharing does not support, use another
provider for that request.

When reconnecting, choose the same account in T3 Code and on OpenAI's sign-in
page. Disconnecting stops running threads but keeps their history and lets you
reconnect later.

If remote sign-in cannot return automatically, paste the full URL from the final
localhost page into the sign-in panel, even if that page could not load.

## Use an existing Codex login

T3 Code can use your installed Codex and its existing login. Run `codex login`
on the environment's machine to sign in. [Provider setup](./install.md#providers)
covers installation and custom configuration.

## Use multiple accounts

Add another ChatGPT account in **Settings → Providers**, then select the account
from the thread's model picker. Compatible accounts can continue the same thread.
Connecting accounts through T3 Code leaves your CLI login unchanged.

### Multiple CLI logins

A shared Codex home with a shadow home lets work and personal accounts continue
the same threads. The accounts share Codex sessions and configuration while keeping
their own login and available models.

Keep your first account in `~/.codex`. On the environment's machine, sign the
second account into a fresh directory:

```bash
mkdir -p ~/.codex_personal
CODEX_HOME=~/.codex_personal codex login
```

Then add a second Codex instance in **Settings > Providers**:

| Instance       | CODEX_HOME path | Shadow home path    |
| -------------- | --------------- | ------------------- |
| Codex Work     | `~/.codex`      | Leave empty         |
| Codex Personal | `~/.codex`      | `~/.codex_personal` |

Both instances must use the same **CODEX_HOME path**. T3 Code prepares the shared
state in the shadow directory; do not populate it by copying your whole Codex
home.

The shadow account needs its own `auth.json` file. If Codex uses an OS credential
store, configure file storage for this setup. See
[OpenAI's credential storage guide](https://learn.chatgpt.com/docs/auth#credential-storage).

Use a completely separate **CODEX_HOME path**, with no shadow home, when you want
separate Codex sessions and configuration. That instance cannot continue threads
from the other home.

## Switch accounts in an existing thread

Choose the other account from the thread's model picker. T3 Code offers compatible
Codex instances that share the thread's **CODEX_HOME path**. Changing accounts does
not move the conversation into a separate Codex home.

If the account is missing from the picker, compare the home paths in provider
settings. If two instances show the same unexpected account or models, check their
reported accounts, refresh provider status, and confirm the second instance has
its own shadow path and login. A shadow-home conflict usually means the directory
contains a copied Codex setup. Use a fresh shadow directory and sign in again.

## Answer questions while Codex works

Codex can ask a question and keep working. Answer it in the thread's question
panel. The answer becomes a new message: it reaches the active turn, or starts
another turn if Codex has finished. Unanswered questions survive reconnects.
If you do not want to answer, dismiss the question from its panel. Dismissing
closes it without sending anything to Codex. This requires a Codex version that
supports async questions.

## Approve app access

Codex tools can request access to another app. Respond to the named app's request
in the thread on web, desktop, or mobile. Some tools offer access for one request,
the current session, or permanently. See [Permission modes](./permission-modes.md)
for command and file approvals.

## Codex says I hit a usage limit

When Codex stops on a usage limit, the thread names the window that ran out and
when it resets, when Codex reports them. Send the message again after the reset. On a workspace plan the
message also says whether your workspace owner needs to add credits or raise the
spend limit to continue sooner.

## Send feedback to OpenAI

In an existing Codex thread, send `/feedback` with an optional description, for
example `/feedback The agent stopped before finishing the tests`. This uploads
the conversation and Codex logs to OpenAI. The returned thread ID can be shared
with OpenAI support.
