"""Executed by Go composition fixture over actual PG/Identity/mTLS owner sockets.
Provider/MCP behavior is a protocol fixture; physical microVM is not exercised.
"""
import asyncio
import json
import ssl
import sys

import httpx
from ahp_clients import (ModelClient, ToolsClient, ComputeClient, A2AClient,
                         AsyncModelClient, AsyncToolsClient, AsyncComputeClient,
                         AsyncA2AClient, ApprovalClient, APIError, RPCError)


def main(path):
    with open(path, encoding="utf-8") as f:
        cfg = json.load(f)

    def tls(name="client"):
        ctx = ssl.create_default_context(cafile=cfg["ca"])
        ctx.load_cert_chain(cfg[name + "_cert"], cfg[name + "_key"])
        return ctx

    def refused(fn, code=None):
        try:
            fn()
        except (APIError, RPCError) as e:
            if code is not None:
                assert e.code == code or getattr(e, "reason", "") == code, f"unexpected refusal {e.code}"
            assert getattr(e, "status", 403) in (200, 403, 404, 409, 400, 503)
        else:
            raise AssertionError("unauthorized or conflicting call succeeded")

    with httpx.Client(verify=tls(), trust_env=False, timeout=20) as http:
        kwargs = {"http_client": http}
        token = lambda: cfg["token"]
        m, t, c, a = (cls(cfg["base"], token, **kwargs) for cls in (ModelClient, ToolsClient, ComputeClient, A2AClient))
        messages = [{"role": "user", "content": "Python standalone"}]
        gen = m.submit("python-model", messages)
        assert gen["status"] == "COMPLETED" and gen["output"] == "first second"
        assert m.submit("python-model", messages)["id"] == gen["id"]
        assert m.get(gen["id"])["status"] == "COMPLETED"
        refused(lambda: m.submit("python-model", [{"role": "user", "content": "changed"}]), "KEY_REUSE_MISMATCH")
        stream = m.stream("python-stream", messages)
        cursor = 0
        for event in stream:
            if event["type"] == "delta":
                cursor = event["delta"]["sequence"]
                break
        stream.close()
        assert cursor == 1
        resumed = list(m.stream("python-stream", messages, after=cursor))
        assert resumed[-1]["type"] == "terminal"
        assert resumed[-1]["generation"]["delta_sequence"] == 2
        assert [f["delta"]["sequence"] for f in resumed if f["type"] == "delta"] == [2]
        cancel_stream = m.stream("python-cancel-model", messages)
        accepted = next(cancel_stream)
        cancel_stream.close()
        canceled_generation = m.cancel(accepted["generation_id"])
        assert canceled_generation["cancel_requested"] or canceled_generation["status"] == "CANCELED"
        op = t.call("python-tool", "echo", {})
        assert op["status"] == "SUCCEEDED" and "result" not in op
        assert t.get("python-tool")["key"] == op["key"]
        assert t.call("python-tool", "echo", {})["version"] == op["version"]
        refused(lambda: t.call("guard-conflict", "unknown", {}), "CONFLICT")
        unknown = t.call("python-unknown", "unknown", {"source": "python"})
        assert unknown["status"] == "SIDE_EFFECT_UNKNOWN"
        assert t.call("python-unknown", "unknown", {"source": "python"})["status"] == "SIDE_EFFECT_UNKNOWN"
        assert t.fence("python-unknown")["status"] == "SIDE_EFFECT_UNKNOWN"
        assert t.connection_state()["connections"]
        # GetResult releases content to the owner authority only: a SUCCEEDED row
        # reads back its raw MCP payload with the dispatch provenance, an
        # uncertain row answers CONFLICT, an absent or foreign key NOT_FOUND, and
        # no read of any kind dispatches the effect again.
        result = t.tool_result("python-tool")
        assert result["status"] == "SUCCEEDED" and result["result_storage"] == "inline"
        assert result["provenance"]["schema"] == "tools-result-v1" and result["provenance"]["grant_context"]
        assert result["payload"]["content"], "raw MCP payload lost"
        assert t.get("python-tool")["version"] == op["version"], "result read disturbed the effect"
        refused(lambda: t.tool_result("python-unknown"), "CONFLICT")
        refused(lambda: t.tool_result("python-never-dispatched"), "NOT_FOUND")
        refused(lambda: ApprovalClient(cfg["base"], token, **kwargs).approvals(), "FORBIDDEN")
        comp = c.create_computer("S", "personal", "", "python", "python-computer", None)
        assert c.create_computer("S", "personal", "", "python", "python-computer", None)["id"] == comp["id"]
        c.acquire_incarnation(comp["id"], "mock", "sdk-test", "")
        command = c.exec(comp["id"], "python-command", "echo fixture", "")["command"]
        c.mark_dispatched(comp["id"], command["id"])
        assert c.fence(comp["id"], command["id"])["status"] == "side_effect_unknown"
        assert list(c.attach_command(comp["id"], command["id"]))[0]["status"] == "side_effect_unknown"
        c.ensure_environment(comp["id"], "python=3")
        refused(lambda: c.export(comp["id"], "/tmp/result", "python-export"), "FORBIDDEN")
        lifecycle = cfg["go_lifecycle"]
        c.typed_patch(lifecycle, "", "python.txt", "", "cHl0aG9u", 384)
        assert c.state_head(lifecycle, "")["tree"]["/python.txt"]["size"] == 6
        assert c.state_revisions(lifecycle, "")["revisions"]
        assert c.list_environment(lifecycle)["files"]
        c.acquire_activity_lease(lifecycle, "browser", "python", 10000)
        refused(lambda: c.sleep(lifecycle), "CONFLICT")
        assert c.activity_leases(lifecycle)["count"] == 1
        c.release_activity_lease(lifecycle, "browser", "python", 10000)
        assert c.sleep(lifecycle)["status"] == "suspended"
        c.wake(lifecycle)
        assert c.cost_report(lifecycle)["open_window"]
        request = {"message": {"messageId": "python-child", "role": "ROLE_USER", "parts": [{"text": "delegate"}]}}
        task = a.delegate("python", 2, request)["task"]
        assert a.delegate("python", 2, request)["task"]["id"] == task["id"]
        assert a.get(task["id"])["id"] == task["id"]
        replay = list(a.resubscribe(task["id"]))
        assert replay[0]["task"]["id"] == task["id"]
        refused(lambda: a.cancel(task["id"]), -32002)
        request["message"]["messageId"] = "python-cancel"
        request["configuration"] = {"returnImmediately": True}
        request["metadata"] = {"ahp_task_kind": "block"}
        pending = a.send(request)["task"]
        assert a.cancel(pending["id"])["id"] == pending["id"]
        canceled = list(a.resubscribe(pending["id"]))[-1]
        assert (canceled.get("task") or canceled.get("statusUpdate"))["status"]["state"] == "TASK_STATE_CANCELED"
        hitl = {"message": {"messageId": "python-hitl", "role": "ROLE_USER", "parts": [{"text":"need input"}]}, "metadata":{"ahp_task_kind":"input"}}
        end = list(a.send_stream(hitl))[-1]
        paused = end.get("task") or end.get("statusUpdate")
        assert paused["status"]["state"] == "TASK_STATE_INPUT_REQUIRED"
        paused_id = paused.get("id") or paused["taskId"]
        assert a.get(paused_id)["status"]["state"] == "TASK_STATE_INPUT_REQUIRED"
        assert len(list(a.resubscribe(paused_id))) == 1
        for cls, call in [(ModelClient, lambda x:x.get(gen["id"])), (ToolsClient,lambda x:x.get("python-tool")),
                          (ComputeClient,lambda x:x.get_computer(comp["id"])), (A2AClient,lambda x:x.get(task["id"]))]:
            refused(lambda cls=cls,call=call:call(cls(cfg["base"],lambda:cfg["revoked"],**kwargs)), "FORBIDDEN")
        # Workload mismatch is denied before the foreign object lookup.
        for name in ("tenant", "deployment"):
            refused(lambda name=name:ModelClient(cfg["base"],lambda:cfg[name],**kwargs).get(gen["id"]), "FORBIDDEN")

    for dimension in ("tenant", "subject", "deployment"):
        with httpx.Client(verify=tls(dimension),trust_env=False,timeout=20) as http:
            kw = {"http_client":http}
            token = lambda:cfg[dimension]
            refused(lambda:ModelClient(cfg["base"],token,**kw).get(gen["id"]), "NOT_FOUND")
            refused(lambda:ToolsClient(cfg["base"],token,**kw).get("python-tool"), "NOT_FOUND")
            refused(lambda:ToolsClient(cfg["base"],token,**kw).tool_result("python-tool"), "NOT_FOUND")
            refused(lambda:ComputeClient(cfg["base"],token,**kw).get_computer(comp["id"]), "NOT_FOUND")
            refused(lambda:A2AClient(cfg["base"],token,**kw).get(task["id"]))

    async def async_contract():
        async with httpx.AsyncClient(verify=tls(),trust_env=False,timeout=20) as http:
            kw = {"http_client":http}
            token = lambda:cfg["token"]
            m,t,c,a = (cls(cfg["base"],token,**kw) for cls in (AsyncModelClient,AsyncToolsClient,AsyncComputeClient,AsyncA2AClient))
            frames = [f async for f in m.stream("python-async-stream", messages)]
            assert frames[-1]["generation"]["status"] == "COMPLETED"
            assert (await m.get(gen["id"]))["id"] == gen["id"]
            assert (await t.get("python-tool"))["status"] == "SUCCEEDED"
            assert (await t.tool_result("python-tool"))["result_storage"] == "inline"
            try:
                await t.tool_result("python-unknown")
            except APIError as async_refusal:
                assert async_refusal.code == "CONFLICT", f"async uncertain result read: {async_refusal.code}"
            else:
                raise AssertionError("async uncertain result read succeeded")
            assert (await c.get_computer(comp["id"]))["id"] == comp["id"]
            assert (await a.get(task["id"]))["id"] == task["id"]
            assert [f async for f in a.resubscribe(task["id"])]
            assert len([f async for f in a.resubscribe(paused_id)]) == 1
            hitl["message"]["messageId"] = "python-async-hitl"
            paused_event = [f async for f in a.send_stream(hitl)]
            last = paused_event[-1].get("task") or paused_event[-1].get("statusUpdate")
            assert last["status"]["state"] == "TASK_STATE_INPUT_REQUIRED"
    asyncio.run(async_contract())
    print("Python sync+async Model/Tools/Compute/A2A actual owner HTTP/mTLS+PG PASS; foreign tenant/subject/deployment, revoke, replay, NDJSON resume, UNKNOWN preserved")


if __name__ == "__main__":
    main(sys.argv[1])
