"""The SSL context every client in this product uses. Defined ONCE.

VERIFICATION STAYS ON. The emulator serves a self-signed certificate and the
platform shares it read-only, so clients trust THAT FILE rather than disabling
checks. The distinction is not pedantry: the same code runs against real Fabric,
where a certificate error is a genuine finding, and a process-wide
`verify=False` would silence it there too.

Absent (production), the system trust store is used unchanged -- `None` is
exactly what `urlopen` wants for "behave normally".

One module rather than the same four lines in four files: a CA that is
configured in most places and missed in one is a client that silently talks to
something unverified.
"""
from __future__ import annotations

import os
import ssl

# SSL_CERT_FILE is what urllib/ssl read; REQUESTS_CA_BUNDLE is requests' name.
# The platform sets both to the same file, and either is accepted here so a
# deployment that sets only one still works.
CA_FILE = os.environ.get("SSL_CERT_FILE") or os.environ.get("REQUESTS_CA_BUNDLE")

CONTEXT: ssl.SSLContext | None = (
    ssl.create_default_context(cafile=CA_FILE) if CA_FILE else None
)
