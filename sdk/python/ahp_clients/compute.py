"""Mounted Compute lifecycle API; physical backends remain owner-controlled."""
from __future__ import annotations
import asyncio
import time
from ._transport import Transport, AsyncTransport, JSON, required


class ComputeClient:
    """Caller-owned mTLS and current task token; no automatic mutation retry."""
    def __init__(self, base_url: str, token, *, http_client):
        self._wire = Transport(base_url, token, http_client=http_client)

    def create_computer(self, class_: str, profile: str, workspace_id: str, realm: str, key: str, quota: dict|None) -> JSON:
        """Call owner CreateComputer; return its complete wire response."""
        required(key, "key")
        return self._wire.call("/v1/compute/CreateComputer", {"class": class_, "profile": profile, "workspace_id": workspace_id, "realm": realm, "key": key, "quota": quota}, mutation=True)

    def get_computer(self, computer_id: str) -> JSON:
        """Call owner GetComputer; return its complete wire response."""
        required(computer_id, "computer_id")
        return self._wire.call("/v1/compute/GetComputer", {"computer_id": computer_id}, mutation=False)

    def acquire_incarnation(self, computer_id: str, backend: str, node: str, vm_gen_id: str) -> JSON:
        """Call owner AcquireIncarnation; return its complete wire response."""
        required(computer_id, "computer_id")
        return self._wire.call("/v1/compute/AcquireIncarnation", {"computer_id": computer_id, "backend": backend, "node": node, "vm_gen_id": vm_gen_id}, mutation=True)

    def stop_incarnation(self, computer_id: str, incarnation_id: str) -> JSON:
        """Call owner StopIncarnation; return its complete wire response."""
        required(computer_id, "computer_id")
        return self._wire.call("/v1/compute/StopIncarnation", {"computer_id": computer_id, "incarnation_id": incarnation_id}, mutation=True)

    def destroy(self, computer_id: str) -> JSON:
        """Call owner Destroy; return its complete wire response."""
        required(computer_id, "computer_id")
        return self._wire.call("/v1/compute/Destroy", {"computer_id": computer_id}, mutation=True)

    def exec(self, computer_id: str, key: str, command: str, cwd: str) -> JSON:
        """Call owner Exec; return its complete wire response."""
        required(computer_id, "computer_id")
        required(key, "key")
        return self._wire.call("/v1/compute/Exec", {"computer_id": computer_id, "key": key, "command": command, "cwd": cwd}, mutation=True)

    def run(self, computer_id: str, key: str, command: str, cwd: str) -> JSON:
        """Call owner Run; return its complete wire response."""
        required(computer_id, "computer_id")
        required(key, "key")
        return self._wire.call("/v1/compute/Run", {"computer_id": computer_id, "key": key, "command": command, "cwd": cwd}, mutation=True)

    def mark_dispatched(self, computer_id: str, command_id: str) -> JSON:
        """Call owner MarkDispatched; return its complete wire response."""
        required(computer_id, "computer_id")
        required(command_id, "command_id")
        return self._wire.call("/v1/compute/MarkDispatched", {"computer_id": computer_id, "command_id": command_id}, mutation=True)

    def complete(self, computer_id: str, command_id: str, exit_code: int, stdout: str, stderr: str, output_truncated: bool) -> JSON:
        """Call owner Complete; return its complete wire response."""
        required(computer_id, "computer_id")
        required(command_id, "command_id")
        return self._wire.call("/v1/compute/Complete", {"computer_id": computer_id, "command_id": command_id, "exit_code": exit_code, "stdout": stdout, "stderr": stderr, "output_truncated": output_truncated}, mutation=True)

    def get_command(self, computer_id: str, command_id: str) -> JSON:
        """Call owner GetCommand; return its complete wire response."""
        required(computer_id, "computer_id")
        required(command_id, "command_id")
        return self._wire.call("/v1/compute/GetCommand", {"computer_id": computer_id, "command_id": command_id}, mutation=False)

    def fence(self, computer_id: str, command_id: str) -> JSON:
        """Call owner Fence; return its complete wire response."""
        required(computer_id, "computer_id")
        required(command_id, "command_id")
        return self._wire.call("/v1/compute/Fence", {"computer_id": computer_id, "command_id": command_id}, mutation=True)

    def cancel(self, computer_id: str, command_id: str) -> JSON:
        """Call owner Cancel; return its complete wire response."""
        required(computer_id, "computer_id")
        required(command_id, "command_id")
        return self._wire.call("/v1/compute/Cancel", {"computer_id": computer_id, "command_id": command_id}, mutation=True)

    def restart(self, computer_id: str, command_id: str) -> JSON:
        """Call owner Restart; return its complete wire response."""
        required(computer_id, "computer_id")
        required(command_id, "command_id")
        return self._wire.call("/v1/compute/Restart", {"computer_id": computer_id, "command_id": command_id}, mutation=True)

    def commit_workspace(self, computer_id: str, expected_head: int, digest: str) -> JSON:
        """Call owner CommitWorkspace; return its complete wire response."""
        required(computer_id, "computer_id")
        return self._wire.call("/v1/compute/CommitWorkspace", {"computer_id": computer_id, "expected_head": expected_head, "digest": digest}, mutation=True)

    def workspace_head(self, computer_id: str) -> JSON:
        """Call owner WorkspaceHead; return its complete wire response."""
        required(computer_id, "computer_id")
        return self._wire.call("/v1/compute/WorkspaceHead", {"computer_id": computer_id}, mutation=False)

    def ensure_environment(self, computer_id: str, spec: str) -> JSON:
        """Call owner EnsureEnvironment; return its complete wire response."""
        required(computer_id, "computer_id")
        return self._wire.call("/v1/compute/EnsureEnvironment", {"computer_id": computer_id, "spec": spec}, mutation=True)

    def begin_install(self, computer_id: str, spec: str, holder: str) -> JSON:
        """Call owner BeginInstall; return its complete wire response."""
        required(computer_id, "computer_id")
        return self._wire.call("/v1/compute/BeginInstall", {"computer_id": computer_id, "spec": spec, "holder": holder}, mutation=True)

    def finish_install(self, computer_id: str, revision_id: str, holder: str, ok: bool, error_msg: str) -> JSON:
        """Call owner FinishInstall; return its complete wire response."""
        required(computer_id, "computer_id")
        return self._wire.call("/v1/compute/FinishInstall", {"computer_id": computer_id, "revision_id": revision_id, "holder": holder, "ok": ok, "error_msg": error_msg}, mutation=True)

    def current_environment(self, computer_id: str) -> JSON:
        """Call owner CurrentEnvironment; return its complete wire response."""
        required(computer_id, "computer_id")
        return self._wire.call("/v1/compute/CurrentEnvironment", {"computer_id": computer_id}, mutation=False)

    def snapshot(self, computer_id: str, kind: str, path: str, digest: str, size_bytes: int) -> JSON:
        """Call owner Snapshot; return its complete wire response."""
        required(computer_id, "computer_id")
        return self._wire.call("/v1/compute/Snapshot", {"computer_id": computer_id, "kind": kind, "path": path, "digest": digest, "size_bytes": size_bytes}, mutation=True)

    def export(self, computer_id: str, path: str, key: str) -> JSON:
        """Call owner Export; return its complete wire response."""
        required(computer_id, "computer_id")
        return self._wire.call("/v1/compute/Export", {"computer_id": computer_id, "path": path,"key": required(key)}, mutation=True)

    def acquire_lease(self, computer_id: str, kind: str, holder: str, ttl_ms: int) -> JSON:
        """Call owner AcquireLease; return its complete wire response."""
        required(computer_id, "computer_id")
        return self._wire.call("/v1/compute/AcquireLease", {"computer_id": computer_id, "kind": kind, "holder": holder, "ttl_ms": ttl_ms}, mutation=True)

    def release_lease(self, computer_id: str, kind: str, holder: str) -> JSON:
        """Call owner ReleaseLease; return its complete wire response."""
        required(computer_id, "computer_id")
        return self._wire.call("/v1/compute/ReleaseLease", {"computer_id": computer_id, "kind": kind, "holder": holder}, mutation=True)

    def current_fence(self, computer_id: str) -> JSON:
        """Call owner CurrentFence; return its complete wire response."""
        required(computer_id, "computer_id")
        return self._wire.call("/v1/compute/CurrentFence", {"computer_id": computer_id}, mutation=False)

    def typed_patch(self, computer_id: str, branch: str, path: str, expected_hash: str, content_b64: str, mode: int) -> JSON:
        """Call owner TypedPatch; return its complete wire response."""
        required(computer_id, "computer_id")
        return self._wire.call("/v1/compute/TypedPatch", {"computer_id": computer_id, "branch": branch, "path": path, "expected_hash": expected_hash, "content_b64": content_b64, "mode": mode}, mutation=True)

    def state_head(self, computer_id: str, branch: str) -> JSON:
        """Call owner StateHead; return its complete wire response."""
        required(computer_id, "computer_id")
        return self._wire.call("/v1/compute/StateHead", {"computer_id": computer_id, "branch": branch}, mutation=False)

    def state_revisions(self, computer_id: str, branch: str) -> JSON:
        """Call owner StateRevisions; return its complete wire response."""
        required(computer_id, "computer_id")
        return self._wire.call("/v1/compute/StateRevisions", {"computer_id": computer_id, "branch": branch}, mutation=False)

    def derive_branch(self, computer_id: str, name: str) -> JSON:
        """Call owner DeriveBranch; return its complete wire response."""
        required(computer_id, "computer_id")
        return self._wire.call("/v1/compute/DeriveBranch", {"computer_id": computer_id, "name": name}, mutation=True)

    def merge_branch(self, computer_id: str, name: str) -> JSON:
        """Call owner MergeBranch; return its complete wire response."""
        required(computer_id, "computer_id")
        return self._wire.call("/v1/compute/MergeBranch", {"computer_id": computer_id, "name": name}, mutation=True)

    def sleep(self, computer_id: str) -> JSON:
        """Call owner Sleep; return its complete wire response."""
        required(computer_id, "computer_id")
        return self._wire.call("/v1/compute/Sleep", {"computer_id": computer_id}, mutation=True)

    def wake(self, computer_id: str) -> JSON:
        """Call owner Wake; return its complete wire response."""
        required(computer_id, "computer_id")
        return self._wire.call("/v1/compute/Wake", {"computer_id": computer_id}, mutation=True)

    def cost_report(self, computer_id: str) -> JSON:
        """Call owner CostReport; return its complete wire response."""
        required(computer_id, "computer_id")
        return self._wire.call("/v1/compute/CostReport", {"computer_id": computer_id}, mutation=False)

    def take_writer_lease(self, computer_id: str, holder: str, ttl_ms: int) -> JSON:
        """Call owner TakeWriterLease; return its complete wire response."""
        required(computer_id, "computer_id")
        return self._wire.call("/v1/compute/TakeWriterLease", {"computer_id": computer_id, "holder": holder, "ttl_ms": ttl_ms}, mutation=True)

    def release_writer_lease(self, computer_id: str, holder: str, ttl_ms: int) -> JSON:
        """Call owner ReleaseWriterLease; return its complete wire response."""
        required(computer_id, "computer_id")
        return self._wire.call("/v1/compute/ReleaseWriterLease", {"computer_id": computer_id, "holder": holder, "ttl_ms": ttl_ms}, mutation=True)

    def acquire_activity_lease(self, computer_id: str, kind: str, holder: str, ttl_ms: int) -> JSON:
        """Call owner AcquireActivityLease; return its complete wire response."""
        required(computer_id, "computer_id")
        return self._wire.call("/v1/compute/AcquireActivityLease", {"computer_id": computer_id, "kind": kind, "holder": holder, "ttl_ms": ttl_ms}, mutation=True)

    def release_activity_lease(self, computer_id: str, kind: str, holder: str, ttl_ms: int) -> JSON:
        """Call owner ReleaseActivityLease; return its complete wire response."""
        required(computer_id, "computer_id")
        return self._wire.call("/v1/compute/ReleaseActivityLease", {"computer_id": computer_id, "kind": kind, "holder": holder, "ttl_ms": ttl_ms}, mutation=True)

    def activity_leases(self, computer_id: str) -> JSON:
        """Call owner ActivityLeases; return its complete wire response."""
        required(computer_id, "computer_id")
        return self._wire.call("/v1/compute/ActivityLeases", {"computer_id": computer_id}, mutation=False)

    def run_install(self, computer_id: str, revision_id: str, command: str) -> JSON:
        """Call owner RunInstall; return its complete wire response."""
        required(computer_id, "computer_id")
        return self._wire.call("/v1/compute/RunInstall", {"computer_id": computer_id, "revision_id": revision_id, "command": command}, mutation=True)

    def list_environment(self, computer_id: str) -> JSON:
        """Call owner ListEnvironment; return its complete wire response."""
        required(computer_id, "computer_id")
        return self._wire.call("/v1/compute/ListEnvironment", {"computer_id": computer_id}, mutation=False)

    def publish_version(self, agent: str, digest: str, state_compat: bool, drain_window_ms: int) -> JSON:
        """Call owner PublishVersion; return its complete wire response."""
        required(agent, "agent")
        return self._wire.call("/v1/compute/PublishVersion", {"agent": agent, "digest": digest, "state_compat": state_compat, "drain_window_ms": drain_window_ms}, mutation=True)

    def drain_version(self, agent: str, digest: str, drain_window_ms: int) -> JSON:
        """Call owner DrainVersion; return its complete wire response."""
        required(agent, "agent")
        return self._wire.call("/v1/compute/DrainVersion", {"agent": agent, "digest": digest, "drain_window_ms": drain_window_ms}, mutation=True)

    def rollback_version(self, agent: str) -> JSON:
        """Call owner RollbackVersion; return its complete wire response."""
        required(agent, "agent")
        return self._wire.call("/v1/compute/RollbackVersion", {"agent": agent}, mutation=True)

    def get_active_version(self, agent: str) -> JSON:
        """Call owner GetActiveVersion; return its complete wire response."""
        required(agent, "agent")
        return self._wire.call("/v1/compute/GetActiveVersion", {"agent": agent}, mutation=False)

    def attach_command(self, computer_id: str, command_id: str, *, interval: float = 0.2):
        """Poll changed durable snapshots, preserving UNKNOWN; this is not a PTY stream."""
        if interval <= 0:
            raise ValueError("positive polling interval required")
        previous = None
        while True:
            command = self.get_command(computer_id, command_id)
            marker = (command.get("status"), command.get("output_seq"), command.get("updated_at"))
            if marker != previous:
                yield command
                previous = marker
            if command.get("status") in {"succeeded", "failed", "cancelled", "lost", "side_effect_unknown"}:
                return
            time.sleep(interval)


class AsyncComputeClient:
    """Caller-owned mTLS and current task token; no automatic mutation retry."""
    def __init__(self, base_url: str, token, *, http_client):
        self._wire = AsyncTransport(base_url, token, http_client=http_client)

    async def create_computer(self, class_: str, profile: str, workspace_id: str, realm: str, key: str, quota: dict|None) -> JSON:
        """Call owner CreateComputer; return its complete wire response."""
        required(key, "key")
        return await self._wire.call("/v1/compute/CreateComputer", {"class": class_, "profile": profile, "workspace_id": workspace_id, "realm": realm, "key": key, "quota": quota}, mutation=True)

    async def get_computer(self, computer_id: str) -> JSON:
        """Call owner GetComputer; return its complete wire response."""
        required(computer_id, "computer_id")
        return await self._wire.call("/v1/compute/GetComputer", {"computer_id": computer_id}, mutation=False)

    async def acquire_incarnation(self, computer_id: str, backend: str, node: str, vm_gen_id: str) -> JSON:
        """Call owner AcquireIncarnation; return its complete wire response."""
        required(computer_id, "computer_id")
        return await self._wire.call("/v1/compute/AcquireIncarnation", {"computer_id": computer_id, "backend": backend, "node": node, "vm_gen_id": vm_gen_id}, mutation=True)

    async def stop_incarnation(self, computer_id: str, incarnation_id: str) -> JSON:
        """Call owner StopIncarnation; return its complete wire response."""
        required(computer_id, "computer_id")
        return await self._wire.call("/v1/compute/StopIncarnation", {"computer_id": computer_id, "incarnation_id": incarnation_id}, mutation=True)

    async def destroy(self, computer_id: str) -> JSON:
        """Call owner Destroy; return its complete wire response."""
        required(computer_id, "computer_id")
        return await self._wire.call("/v1/compute/Destroy", {"computer_id": computer_id}, mutation=True)

    async def exec(self, computer_id: str, key: str, command: str, cwd: str) -> JSON:
        """Call owner Exec; return its complete wire response."""
        required(computer_id, "computer_id")
        required(key, "key")
        return await self._wire.call("/v1/compute/Exec", {"computer_id": computer_id, "key": key, "command": command, "cwd": cwd}, mutation=True)

    async def run(self, computer_id: str, key: str, command: str, cwd: str) -> JSON:
        """Call owner Run; return its complete wire response."""
        required(computer_id, "computer_id")
        required(key, "key")
        return await self._wire.call("/v1/compute/Run", {"computer_id": computer_id, "key": key, "command": command, "cwd": cwd}, mutation=True)

    async def mark_dispatched(self, computer_id: str, command_id: str) -> JSON:
        """Call owner MarkDispatched; return its complete wire response."""
        required(computer_id, "computer_id")
        required(command_id, "command_id")
        return await self._wire.call("/v1/compute/MarkDispatched", {"computer_id": computer_id, "command_id": command_id}, mutation=True)

    async def complete(self, computer_id: str, command_id: str, exit_code: int, stdout: str, stderr: str, output_truncated: bool) -> JSON:
        """Call owner Complete; return its complete wire response."""
        required(computer_id, "computer_id")
        required(command_id, "command_id")
        return await self._wire.call("/v1/compute/Complete", {"computer_id": computer_id, "command_id": command_id, "exit_code": exit_code, "stdout": stdout, "stderr": stderr, "output_truncated": output_truncated}, mutation=True)

    async def get_command(self, computer_id: str, command_id: str) -> JSON:
        """Call owner GetCommand; return its complete wire response."""
        required(computer_id, "computer_id")
        required(command_id, "command_id")
        return await self._wire.call("/v1/compute/GetCommand", {"computer_id": computer_id, "command_id": command_id}, mutation=False)

    async def fence(self, computer_id: str, command_id: str) -> JSON:
        """Call owner Fence; return its complete wire response."""
        required(computer_id, "computer_id")
        required(command_id, "command_id")
        return await self._wire.call("/v1/compute/Fence", {"computer_id": computer_id, "command_id": command_id}, mutation=True)

    async def cancel(self, computer_id: str, command_id: str) -> JSON:
        """Call owner Cancel; return its complete wire response."""
        required(computer_id, "computer_id")
        required(command_id, "command_id")
        return await self._wire.call("/v1/compute/Cancel", {"computer_id": computer_id, "command_id": command_id}, mutation=True)

    async def restart(self, computer_id: str, command_id: str) -> JSON:
        """Call owner Restart; return its complete wire response."""
        required(computer_id, "computer_id")
        required(command_id, "command_id")
        return await self._wire.call("/v1/compute/Restart", {"computer_id": computer_id, "command_id": command_id}, mutation=True)

    async def commit_workspace(self, computer_id: str, expected_head: int, digest: str) -> JSON:
        """Call owner CommitWorkspace; return its complete wire response."""
        required(computer_id, "computer_id")
        return await self._wire.call("/v1/compute/CommitWorkspace", {"computer_id": computer_id, "expected_head": expected_head, "digest": digest}, mutation=True)

    async def workspace_head(self, computer_id: str) -> JSON:
        """Call owner WorkspaceHead; return its complete wire response."""
        required(computer_id, "computer_id")
        return await self._wire.call("/v1/compute/WorkspaceHead", {"computer_id": computer_id}, mutation=False)

    async def ensure_environment(self, computer_id: str, spec: str) -> JSON:
        """Call owner EnsureEnvironment; return its complete wire response."""
        required(computer_id, "computer_id")
        return await self._wire.call("/v1/compute/EnsureEnvironment", {"computer_id": computer_id, "spec": spec}, mutation=True)

    async def begin_install(self, computer_id: str, spec: str, holder: str) -> JSON:
        """Call owner BeginInstall; return its complete wire response."""
        required(computer_id, "computer_id")
        return await self._wire.call("/v1/compute/BeginInstall", {"computer_id": computer_id, "spec": spec, "holder": holder}, mutation=True)

    async def finish_install(self, computer_id: str, revision_id: str, holder: str, ok: bool, error_msg: str) -> JSON:
        """Call owner FinishInstall; return its complete wire response."""
        required(computer_id, "computer_id")
        return await self._wire.call("/v1/compute/FinishInstall", {"computer_id": computer_id, "revision_id": revision_id, "holder": holder, "ok": ok, "error_msg": error_msg}, mutation=True)

    async def current_environment(self, computer_id: str) -> JSON:
        """Call owner CurrentEnvironment; return its complete wire response."""
        required(computer_id, "computer_id")
        return await self._wire.call("/v1/compute/CurrentEnvironment", {"computer_id": computer_id}, mutation=False)

    async def snapshot(self, computer_id: str, kind: str, path: str, digest: str, size_bytes: int) -> JSON:
        """Call owner Snapshot; return its complete wire response."""
        required(computer_id, "computer_id")
        return await self._wire.call("/v1/compute/Snapshot", {"computer_id": computer_id, "kind": kind, "path": path, "digest": digest, "size_bytes": size_bytes}, mutation=True)

    async def export(self, computer_id: str, path: str, key: str) -> JSON:
        """Call owner Export; return its complete wire response."""
        required(computer_id, "computer_id")
        return await self._wire.call("/v1/compute/Export", {"computer_id": computer_id, "path": path,"key": required(key)}, mutation=True)

    async def acquire_lease(self, computer_id: str, kind: str, holder: str, ttl_ms: int) -> JSON:
        """Call owner AcquireLease; return its complete wire response."""
        required(computer_id, "computer_id")
        return await self._wire.call("/v1/compute/AcquireLease", {"computer_id": computer_id, "kind": kind, "holder": holder, "ttl_ms": ttl_ms}, mutation=True)

    async def release_lease(self, computer_id: str, kind: str, holder: str) -> JSON:
        """Call owner ReleaseLease; return its complete wire response."""
        required(computer_id, "computer_id")
        return await self._wire.call("/v1/compute/ReleaseLease", {"computer_id": computer_id, "kind": kind, "holder": holder}, mutation=True)

    async def current_fence(self, computer_id: str) -> JSON:
        """Call owner CurrentFence; return its complete wire response."""
        required(computer_id, "computer_id")
        return await self._wire.call("/v1/compute/CurrentFence", {"computer_id": computer_id}, mutation=False)

    async def typed_patch(self, computer_id: str, branch: str, path: str, expected_hash: str, content_b64: str, mode: int) -> JSON:
        """Call owner TypedPatch; return its complete wire response."""
        required(computer_id, "computer_id")
        return await self._wire.call("/v1/compute/TypedPatch", {"computer_id": computer_id, "branch": branch, "path": path, "expected_hash": expected_hash, "content_b64": content_b64, "mode": mode}, mutation=True)

    async def state_head(self, computer_id: str, branch: str) -> JSON:
        """Call owner StateHead; return its complete wire response."""
        required(computer_id, "computer_id")
        return await self._wire.call("/v1/compute/StateHead", {"computer_id": computer_id, "branch": branch}, mutation=False)

    async def state_revisions(self, computer_id: str, branch: str) -> JSON:
        """Call owner StateRevisions; return its complete wire response."""
        required(computer_id, "computer_id")
        return await self._wire.call("/v1/compute/StateRevisions", {"computer_id": computer_id, "branch": branch}, mutation=False)

    async def derive_branch(self, computer_id: str, name: str) -> JSON:
        """Call owner DeriveBranch; return its complete wire response."""
        required(computer_id, "computer_id")
        return await self._wire.call("/v1/compute/DeriveBranch", {"computer_id": computer_id, "name": name}, mutation=True)

    async def merge_branch(self, computer_id: str, name: str) -> JSON:
        """Call owner MergeBranch; return its complete wire response."""
        required(computer_id, "computer_id")
        return await self._wire.call("/v1/compute/MergeBranch", {"computer_id": computer_id, "name": name}, mutation=True)

    async def sleep(self, computer_id: str) -> JSON:
        """Call owner Sleep; return its complete wire response."""
        required(computer_id, "computer_id")
        return await self._wire.call("/v1/compute/Sleep", {"computer_id": computer_id}, mutation=True)

    async def wake(self, computer_id: str) -> JSON:
        """Call owner Wake; return its complete wire response."""
        required(computer_id, "computer_id")
        return await self._wire.call("/v1/compute/Wake", {"computer_id": computer_id}, mutation=True)

    async def cost_report(self, computer_id: str) -> JSON:
        """Call owner CostReport; return its complete wire response."""
        required(computer_id, "computer_id")
        return await self._wire.call("/v1/compute/CostReport", {"computer_id": computer_id}, mutation=False)

    async def take_writer_lease(self, computer_id: str, holder: str, ttl_ms: int) -> JSON:
        """Call owner TakeWriterLease; return its complete wire response."""
        required(computer_id, "computer_id")
        return await self._wire.call("/v1/compute/TakeWriterLease", {"computer_id": computer_id, "holder": holder, "ttl_ms": ttl_ms}, mutation=True)

    async def release_writer_lease(self, computer_id: str, holder: str, ttl_ms: int) -> JSON:
        """Call owner ReleaseWriterLease; return its complete wire response."""
        required(computer_id, "computer_id")
        return await self._wire.call("/v1/compute/ReleaseWriterLease", {"computer_id": computer_id, "holder": holder, "ttl_ms": ttl_ms}, mutation=True)

    async def acquire_activity_lease(self, computer_id: str, kind: str, holder: str, ttl_ms: int) -> JSON:
        """Call owner AcquireActivityLease; return its complete wire response."""
        required(computer_id, "computer_id")
        return await self._wire.call("/v1/compute/AcquireActivityLease", {"computer_id": computer_id, "kind": kind, "holder": holder, "ttl_ms": ttl_ms}, mutation=True)

    async def release_activity_lease(self, computer_id: str, kind: str, holder: str, ttl_ms: int) -> JSON:
        """Call owner ReleaseActivityLease; return its complete wire response."""
        required(computer_id, "computer_id")
        return await self._wire.call("/v1/compute/ReleaseActivityLease", {"computer_id": computer_id, "kind": kind, "holder": holder, "ttl_ms": ttl_ms}, mutation=True)

    async def activity_leases(self, computer_id: str) -> JSON:
        """Call owner ActivityLeases; return its complete wire response."""
        required(computer_id, "computer_id")
        return await self._wire.call("/v1/compute/ActivityLeases", {"computer_id": computer_id}, mutation=False)

    async def run_install(self, computer_id: str, revision_id: str, command: str) -> JSON:
        """Call owner RunInstall; return its complete wire response."""
        required(computer_id, "computer_id")
        return await self._wire.call("/v1/compute/RunInstall", {"computer_id": computer_id, "revision_id": revision_id, "command": command}, mutation=True)

    async def list_environment(self, computer_id: str) -> JSON:
        """Call owner ListEnvironment; return its complete wire response."""
        required(computer_id, "computer_id")
        return await self._wire.call("/v1/compute/ListEnvironment", {"computer_id": computer_id}, mutation=False)

    async def publish_version(self, agent: str, digest: str, state_compat: bool, drain_window_ms: int) -> JSON:
        """Call owner PublishVersion; return its complete wire response."""
        required(agent, "agent")
        return await self._wire.call("/v1/compute/PublishVersion", {"agent": agent, "digest": digest, "state_compat": state_compat, "drain_window_ms": drain_window_ms}, mutation=True)

    async def drain_version(self, agent: str, digest: str, drain_window_ms: int) -> JSON:
        """Call owner DrainVersion; return its complete wire response."""
        required(agent, "agent")
        return await self._wire.call("/v1/compute/DrainVersion", {"agent": agent, "digest": digest, "drain_window_ms": drain_window_ms}, mutation=True)

    async def rollback_version(self, agent: str) -> JSON:
        """Call owner RollbackVersion; return its complete wire response."""
        required(agent, "agent")
        return await self._wire.call("/v1/compute/RollbackVersion", {"agent": agent}, mutation=True)

    async def get_active_version(self, agent: str) -> JSON:
        """Call owner GetActiveVersion; return its complete wire response."""
        required(agent, "agent")
        return await self._wire.call("/v1/compute/GetActiveVersion", {"agent": agent}, mutation=False)

    async def attach_command(self, computer_id: str, command_id: str, *, interval: float = 0.2):
        """Poll changed durable snapshots, preserving UNKNOWN; this is not a PTY stream."""
        if interval <= 0:
            raise ValueError("positive polling interval required")
        previous = None
        while True:
            command = await self.get_command(computer_id, command_id)
            marker = (command.get("status"), command.get("output_seq"), command.get("updated_at"))
            if marker != previous:
                yield command
                previous = marker
            if command.get("status") in {"succeeded", "failed", "cancelled", "lost", "side_effect_unknown"}:
                return
            await asyncio.sleep(interval)
