"""Where Fabric is, and who we are to it — resolved from an Airflow Connection.

THE PRODUCT NEVER LEARNS WHICH TARGET ANSWERED. A DAG says `conn_id="fabric"`;
the platform provisions that connection against the emulator locally and against
real Fabric in production, and nothing here branches on which.

This is the same seam `contoso-fabric-platform/platform/target.py` occupies, and
its comment states the principle better than a restatement would:

    THE GRANT TYPE IS NOT DECIDED HERE, and that is the point. […] The target
    resolves a credential instead, so who is authenticating is the deployment's
    business and not this module's.

The audiences are the REAL Fabric ones on both targets — the emulator validates
the same strings. Asking for `https://storage.azure.com` is not an emulator
concession; it is what the token is for.

NO azure-identity. Against the emulator it is not installed, and reaching for it
is how Phase 0 ended up authenticating against real Microsoft endpoints with a
developer's own `az login` state. A client-credentials POST is nine lines of
stdlib and cannot silently retarget production.
"""
from __future__ import annotations

import json
import os
import time
import urllib.parse
import urllib.request
from dataclasses import dataclass

FABRIC_AUDIENCE = "https://api.fabric.microsoft.com"
STORAGE_AUDIENCE = "https://storage.azure.com"


@dataclass(frozen=True)
class Target:
    """One resolved target. Built from a Connection, never from module state."""

    api_root: str
    onelake_url: str
    # The emulator serves OneLake on the Fabric port and routes by Host header,
    # the way `curl --resolve` does. Real Fabric has its own hostname, so this
    # is empty there. It is the ONLY target difference in this file, and it
    # arrives as data rather than as a branch.
    onelake_host_header: str
    token_url: str
    client_id: str
    client_secret: str
    name: str = "emulator"

    @classmethod
    def from_connection(cls, conn_id: str = "fabric") -> "Target":
        """Read the platform's connection. Import is local so this module stays
        importable — and unit-testable — outside a worker."""
        from airflow.sdk import BaseHook

        extra = BaseHook.get_connection(conn_id).extra_dejson
        return cls(
            api_root=extra["api_root"].rstrip("/"),
            onelake_url=extra["onelake_url"].rstrip("/"),
            onelake_host_header=extra.get("onelake_host_header", ""),
            token_url=extra["token_url"],
            client_id=extra["client_id"],
            client_secret=extra["client_secret"],
            name=extra.get("target", "emulator"),
        )

    @classmethod
    def from_env(cls) -> "Target":
        """The same target, from environment. For witnesses that run outside a
        DAG — they must exercise the SAME code the DAG does, not a copy."""
        return cls(
            api_root=os.environ["FABRIC_API_ROOT"].rstrip("/"),
            onelake_url=os.environ["FABRIC_ONELAKE_URL"].rstrip("/"),
            onelake_host_header=os.environ.get("FABRIC_ONELAKE_HOST_HEADER", ""),
            token_url=os.environ["ENTRA_TOKEN_URL"],
            client_id=os.environ["ENTRA_CLIENT_ID"],
            client_secret=os.environ["ENTRA_CLIENT_SECRET"],
            name=os.environ.get("FABRIC_TARGET", "emulator"),
        )

    # Tokens are cached per audience until shortly before expiry. Not an
    # optimisation: a landing run makes hundreds of DFS calls, and minting a
    # token per call turns the issuer into the bottleneck and the log into
    # noise. The 60s margin is so a token cannot expire mid-upload.
    _cache: dict = None  # type: ignore[assignment]

    def token(self, audience: str) -> str:
        cache = object.__getattribute__(self, "__dict__").setdefault("_tok", {})
        hit = cache.get(audience)
        if hit and hit[1] > time.time() + 60:
            return hit[0]
        body = urllib.parse.urlencode({
            "grant_type": "client_credentials",
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "scope": f"{audience}/.default",
        }).encode()
        req = urllib.request.Request(
            self.token_url, data=body,
            headers={"Content-Type": "application/x-www-form-urlencoded"})
        with urllib.request.urlopen(req, timeout=30) as r:
            payload = json.loads(r.read())
        tok = payload["access_token"]
        cache[audience] = (tok, time.time() + int(payload.get("expires_in", 3600)))
        return tok

    def fabric_token(self) -> str:
        return self.token(FABRIC_AUDIENCE)

    def storage_token(self) -> str:
        return self.token(STORAGE_AUDIENCE)

    def delta_storage_options(self) -> dict:
        """What delta-rs needs to reach OneLake as us.

        `azure_storage_token` is the bearer, which is precisely the field dlt's
        own credential model does not have — see pyproject.toml for why that
        decided the architecture.
        """
        opts = {
            "azure_storage_account_name": "onelake",
            "azure_storage_token": self.storage_token(),
        }
        if self.is_emulator:
            opts["azure_endpoint"] = f"{self.onelake_url}/onelake"
            opts["azure_allow_http"] = "true"
        return opts

    @property
    def is_emulator(self) -> bool:
        return self.name == "emulator"
