import assert from 'node:assert/strict';
import test from 'node:test';
import {JournalHTTPError} from '../src/index.mjs';

const modules = await Promise.all([
  'cordis', 'dsh-session', 'dsh-agent', 'dsh-agent-loop', 'dsh-llm',
  'dsh-tools', 'dsh-system-prompt', 'dsh-session-projection', 'dsh-session-checkpoint-policy',
].map(name => import('@deepseek-ai/' + name).catch(error => {
  if (error.code !== 'ERR_MODULE_NOT_FOUND' || !error.message.includes("'@deepseek-ai/" + name + "'")) throw error;
})));

const hasRuntime = modules.every(Boolean);
const payload = value => ({inline: Buffer.from(JSON.stringify(value)).toString('base64')});
const event = (type, seq, turnSeq, value) => ({
  type, seq, turn_seq: turnSeq, id: 'turn.input:' + String(seq).padStart(64, '0'),
  codec: 'json', role: 'user', payload: payload(value), is_active: true,
});

const scenarios = [
  {name: 'DeepSeek ACK is atomic with native user messages before dispatch'},
  {name: 'DeepSeek retries the same ACK when the append reply is lost', loseReply: true},
  {name: 'DeepSeek admits steer that races terminal disposition', late: true},
];

for (const {name, loseReply = false, late = false} of scenarios) {
  test(name, {skip: !hasRuntime, timeout: 10000}, async () => {
    const [{Context}, {default: SessionStore, SessionId}, {default: AgentRegistry},
      {default: AgentLoop}, {default: LlmRuntime, createUserMessage, LlmAdapter},
      {default: ToolRuntime}, {default: SystemPrompt}, {default: Projections}, checkpointPolicy] = modules;
    const {loadDeepSeekSessionPersistence} = await import('../src/deepseek.mjs');
    const id = SessionId('vm-deepseek');
    const rows = [event('turn.input', 1, 1, 'initial')];
    let pending;
    const lease = {session_id: 'dsh-' + id, epoch: 1, input_seq: 1, head_seq: 1, turn_seq: 1};
    const writes = [];
    const receipts = new Map();
    let acked = false;
    let lostAcknowledgement = false;
    let models = 0;
    const client = {async request(method, path, {body, query} = {}) {
      if (method === 'GET' && path.endsWith('/events')) return {
        head_seq: rows.length, lease_epoch: 1, events: rows.slice(query.after_seq, query.after_seq + query.limit),
      };
      if (method === 'GET') return {status: 'ACTIVE', input_seq: 1, lease_epoch: 1, head_seq: rows.length};
      assert(path.endsWith('/events:append'), path);
      assert.equal(body.turn_seq, 1);
      const retry = receipts.get(body.key);
      if (retry) {
        assert.deepEqual(body, retry.body);
        return retry.receipt;
      }
      if (body.expected_seq !== rows.length) throw new JournalHTTPError(409, 'CONFLICT');
      writes.push(body);
      const firstSeq = rows.length + 1;
      rows.push(...body.events.map((item, index) => ({...item, seq: firstSeq + index, turn_seq: 1})));
      const receipt = {first_seq: firstSeq, head_seq: rows.length};
      if (body.steer_ack) {
        assert.deepEqual(body.steer_ack, {turn_seq: 1, up_to_seq: pending[1].seq, count: 2});
        assert.deepEqual(body.events
          .map(item => JSON.parse(Buffer.from(item.payload.inline, 'base64')))
          .filter(item => item.type === 'user/message')
          .map(item => item.data.content[0].text), ['approved one', 'approved two']);
        acked = true;
        receipts.set(body.key, {body, receipt});
        if (loseReply && !lostAcknowledgement) {
          lostAcknowledgement = true;
          throw new JournalHTTPError(0, 'TRANSPORT', true);
        }
      } else if (pending && !acked) {
        receipt.control = {pending_steer: pending};
      }
      return receipt;
    }};
    const ctx = new Context();
    for (const plugin of [SessionStore, AgentRegistry, LlmRuntime, SystemPrompt, ToolRuntime, Projections]) {
      await ctx.plugin(plugin);
    }
    await ctx.plugin(await loadDeepSeekSessionPersistence(), {
      client, preclaimedTurn: nativeId => nativeId === id ? lease : undefined,
      decodeSteer: input => createUserMessage({
        content: [{type: 'text', text: JSON.parse(Buffer.from(input.payload.inline, 'base64'))}],
        source: {kind: 'user'},
      }),
    });
    await ctx.plugin(checkpointPolicy);
    await ctx.plugin(AgentLoop, {agents: []});
    function enqueueSteer(forward) {
      const head = rows.length;
      pending = [event('turn.steer', head + 1, 1, 'approved one'),
        event('turn.steer', head + 3, 1, 'approved two')];
      rows.push(pending[0], event('turn.followup', head + 2, undefined, 'another turn'), pending[1]);
      ctx.sessionPersistence.acceptTurnControl(id, 999, {cancel_requested: true});
      ctx.sessionPersistence.acceptTurnControl(id, 1, {turn_seq: 999, cancel_requested: true});
      if (forward) {
        const control = {turn_seq: 1, pending_steer: pending};
        ctx.sessionPersistence.acceptTurnControl(id, 1, control);
        ctx.sessionPersistence.acceptTurnControl(id, 1, control);
      }
      assert.equal(acked, false, 'peek and native queue admission do not acknowledge input');
    }

    class Model extends LlmAdapter {
      async *stream(request) {
        models++;
        if (models === 1) {
          assert.equal(acked, false);
          if (!late) enqueueSteer(loseReply);
        } else {
          assert(acked);
          const contents = JSON.stringify(request.messages);
          assert.match(contents, /approved one/);
          assert.match(contents, /approved two/);
          assert.doesNotMatch(contents, /another turn/);
        }
        yield {type: 'finish', reason: {kind: 'stop'}};
      }
    }
    ctx.llm.registerAdapter(['coordinator-test'], new Model());
    let agent;
    try {
      agent = await ctx.agents.create({sessionId: id, agentOptions: {
        provider: 'coordinator-test', model: 'coordinator-test',
      }});
      agent.agent.followup(createUserMessage({
        content: [{type: 'text', text: 'initial'}], source: {kind: 'user'},
      }));
      await agent.agent.whenIdle();
      if (late) {
        assert.equal(models, 1);
        enqueueSteer(true);
        assert.equal(await ctx.sessionPersistence.resumePendingSteer(id, 999), false);
        assert.equal(await ctx.sessionPersistence.resumePendingSteer(id, 1), true);
        await agent.agent.whenIdle();
      }
      assert.equal(lostAcknowledgement, loseReply);
      assert.equal(models, loseReply ? 1 : 2, 'a failed flush prevents model dispatch');
      await ctx.sessions.flush(agent.agent.session);
      assert.equal(acked, true);
      const reader = await ctx.sessionPersistence.open(id, 'read');
      const {events} = await reader.read();
      await reader.close();
      assert.deepEqual(events.filter(item => item.type === 'user/message').map(item => item.data.content[0].text),
        ['initial', 'approved one', 'approved two']);
      assert.equal(writes.filter(item => item.steer_ack).length, 1);
    } finally {
      await agent?.dispose();
      await ctx.fiber.dispose();
    }
  });
}
