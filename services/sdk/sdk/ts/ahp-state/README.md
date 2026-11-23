# AHP State SDK for Node.js

The SDK uses the task's HTTPS State API handshake for mutual TLS and reads the task token again for each request. Pass a handshake file path or a client configuration to `CompactJournalClient`.

```ts
import {CompactJournalClient} from '@ahp-platform/state';

const client = new CompactJournalClient('/run/ahp/state-handshake.json');
const catalog = await client.listCatalog({harness: 'deepseek', limit: 20});
client.close();
```

The DeepSeek adapter loads the native runtime on demand. A compatible runtime installation supplies the optional peer packages `@deepseek-ai/dsh-session-persistence`, `@deepseek-ai/dsh-session` and `@deepseek-ai/dsh-llm`, tested with version `0.2.1-alpha.2`. The application can load the class for Cordis or construct the persistence service through the factory.

```ts
import {createDeepSeekSessionPersistence} from '@ahp-platform/state/deepseek';

const persistence = await createDeepSeekSessionPersistence(ctx, {client});
```

For VM-managed DeepSeek turns, claim the session with `claimVMTurn` and supply
the receipt to the adapter’s `preclaimedTurn` resolver with `turn_seq` set to
`input_seq`. The resolver supplies the claim when native AgentLoop creates or
resumes the session. Give the adapter a `decodeSteer` function that maps the
producer’s journal payload to a native DeepSeek user message, for example using
`createUserMessage` from `@deepseek-ai/dsh-llm`. Mount DeepSeek’s
`dsh-session-checkpoint-policy` so the runtime flushes the native message
and its ACK before dispatching the model request.

Forward `startHeartbeat` control with
`onControl: delta => persistence.acceptTurnControl(nativeId, turn.input_seq, delta)`.
The adapter also receives controls from its append receipts. The host owns the
terminal append and reads `control.next_turn` from its receipt. If completion
conflicts with steer that arrived after the native driver became idle, forward
the control from the lease heartbeat, call `resumePendingSteer(nativeId, turn.input_seq)`,
and wait for the native driver to finish and flush before retrying completion.
For a follow-up, close the preceding native writer before resolving the next
claim with its new `input_seq`.

OpenClaw exposes `CompactOpenClawStorage` for native session trees and `createOpenClawGatewayCatalog(client)` for programmatic catalog access. A dedicated stock Gateway can register catalog hooks using a preload module. Set the two environment variables to the OpenClaw installation directory and the task handshake file path. The Gateway preload uses `node:module.registerHooks` and has been tested with OpenClaw `2026.6.11`.

```js
import {registerOpenClawGatewayCatalog} from '@ahp-platform/state/openclaw';

registerOpenClawGatewayCatalog({
  installDir: process.env.AHP_OPENCLAW_INSTALL_DIR,
  handshake: process.env.AHP_OPENCLAW_CATALOG_HANDSHAKE,
});
```
