"""Framework-neutral clients for AHP owner APIs (requires only httpx)."""
from ._transport import APIError
from .tls import mtls_context
from .model import ModelClient, AsyncModelClient
from .tools import (ToolsClient, AsyncToolsClient, ApprovalClient, AsyncApprovalClient,
                    ResultShapeError, fetch_result_bytes, fetch_result_bytes_async)
from .compute import ComputeClient, AsyncComputeClient
from .artifacts import ArtifactsClient, AsyncArtifactsClient
from .a2a import A2AClient, AsyncA2AClient, RPCError
from .shared_memory import SharedMemoryClient, AsyncSharedMemoryClient

__all__ = ["ArtifactsClient", "AsyncArtifactsClient", "APIError", "ModelClient", "AsyncModelClient", "ToolsClient", "AsyncToolsClient",
           "ApprovalClient", "AsyncApprovalClient", "ComputeClient", "AsyncComputeClient",
           "A2AClient", "AsyncA2AClient", "RPCError",
           "ResultShapeError", "fetch_result_bytes", "fetch_result_bytes_async", "mtls_context",
           "SharedMemoryClient", "AsyncSharedMemoryClient"]
