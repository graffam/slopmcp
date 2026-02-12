"""MIMIC-IV Demo MCP Server for ChatGPT Apps SDK."""

import contextlib
import os

from mcp.server.fastmcp import FastMCP
from mcp.server.transport_security import TransportSecuritySettings
from starlette.applications import Starlette
from starlette.middleware.cors import CORSMiddleware
from starlette.routing import Mount

from tools import patients, labs, admissions, vitals, health

# ---------------------------------------------------------------------------
# Server
# ---------------------------------------------------------------------------


def _env_csv(name: str) -> list[str]:
    raw = os.getenv(name, "")
    return [item.strip() for item in raw.split(",") if item.strip()]


def _transport_security_settings() -> TransportSecuritySettings:
    if os.getenv("MCP_DISABLE_DNS_REBINDING_PROTECTION", "").lower() in {
        "1",
        "true",
        "yes",
        "on",
    }:
        return TransportSecuritySettings(enable_dns_rebinding_protection=False)

    allowed_hosts = _env_csv("MCP_ALLOWED_HOSTS")
    allowed_origins = _env_csv("MCP_ALLOWED_ORIGINS")

    # Defaults cover local development and Vercel deployments.
    if not allowed_hosts:
        allowed_hosts = [
            "localhost",
            "localhost:*",
            "127.0.0.1",
            "127.0.0.1:*",
        ]
        vercel_url = os.getenv("VERCEL_URL")
        if vercel_url:
            allowed_hosts.append(vercel_url)

    if not allowed_origins:
        allowed_origins = [
            "http://localhost",
            "http://localhost:*",
            "http://127.0.0.1",
            "http://127.0.0.1:*",
        ]
        vercel_url = os.getenv("VERCEL_URL")
        if vercel_url:
            allowed_origins.extend(
                [
                    f"https://{vercel_url}",
                    f"http://{vercel_url}",
                ]
            )

    return TransportSecuritySettings(
        enable_dns_rebinding_protection=True,
        allowed_hosts=allowed_hosts,
        allowed_origins=allowed_origins,
    )


mcp = FastMCP(
    "mimic-iv-demo",
    stateless_http=True,
    json_response=True,
    streamable_http_path="/",
    transport_security=_transport_security_settings(),
)


# ---------------------------------------------------------------------------
# Register tools
# ---------------------------------------------------------------------------

patients.register(mcp)
labs.register(mcp)
admissions.register(mcp)
vitals.register(mcp)
health.register(mcp)

# ---------------------------------------------------------------------------
# ASGI app with CORS + Streamable HTTP transport
# ---------------------------------------------------------------------------

_http_app = mcp.streamable_http_app()


@contextlib.asynccontextmanager
async def lifespan(app: Starlette):
    async with mcp.session_manager.run():
        yield


app = Starlette(
    routes=[
        Mount("/mcp", app=_http_app),
    ],
    lifespan=lifespan,
)


class RootMCPCompatMiddleware:
    """Allow clients configured for / to reach the /mcp mount."""

    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):
        if scope.get("type") == "http" and scope.get("path") in {"/", "/mcp"}:
            rewritten = dict(scope)
            rewritten["path"] = "/mcp/"
            rewritten["raw_path"] = b"/mcp/"
            await self.app(rewritten, receive, send)
            return
        await self.app(scope, receive, send)


app = RootMCPCompatMiddleware(app)

app = CORSMiddleware(
    app,
    allow_origins=["*"],
    allow_methods=["GET", "POST", "DELETE", "OPTIONS"],
    allow_headers=["Content-Type", "Authorization", "Mcp-Session-Id"],
    expose_headers=["Mcp-Session-Id"],
)


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "server:app",
        host="0.0.0.0",
        port=8787,
        reload=True,
    )
