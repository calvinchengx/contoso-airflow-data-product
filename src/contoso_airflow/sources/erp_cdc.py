"""The ERP change stream, as a dlt source.

THIS IS THE BOUNDARY. Everything upstream — Postgres, Debezium, the broker — is
the world outside the lakehouse; everything downstream is inside it. The
consumer is the only thing touching both, which is exactly where a real
ingestion job sits.

Landed VERBATIM, like the HTTP vendors and for the same reason: what arrives is
Debezium's own JSON envelope, and the answer to "what did the ERP actually say"
has to be available without going back to the vendor. So the raw message bytes
go to OneLake unchanged and dlt yields a manifest of what was written.

WHAT SURVIVES AND WHAT DOES NOT. Counts survive real CDC — the same DML produces
the same events — so the change-event total is assertable. LSNs, commit
timestamps and broker offsets do not: they differ every run, and nothing here
asserts on them. `effective_date` travels as DATA, which is what preserves the
fixture's deliberate disagreement between capture order and business order.
"""
from __future__ import annotations

import json

import dlt

from ..io import onelake
from ..target import Target


def _drain(bootstrap: str, topic: str, group: str, idle_ms: int, max_messages: int):
    """Read the topic from the beginning until it goes quiet.

    FROM THE BEGINNING, and quiet-based rather than count-based: the consumer
    must not need to be told how many events to expect. Being told would make
    the count an input, and the count is the thing under test.
    """
    from kafka import KafkaConsumer

    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=bootstrap,
        group_id=group,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        # A tombstone carries a null value; Debezium is configured not to emit
        # them, and a decoder that assumed JSON would crash if that changed.
        value_deserializer=lambda b: b,
        consumer_timeout_ms=idle_ms,
    )
    try:
        for n, msg in enumerate(consumer, 1):
            if msg.value is None:
                continue
            yield msg.value
            if n >= max_messages:
                raise RuntimeError(
                    f"{topic}: stopped at {max_messages} messages. That is a "
                    f"backstop against an unbounded drain, not an expected "
                    f"outcome -- raise it or find out why the topic is longer "
                    f"than the vendor's history.")
    finally:
        consumer.close()


@dlt.source(name="contoso_erp")
def erp_source(bootstrap: str, topic: str, target: Target,
               workspace: str, lakehouse: str, day: str,
               group: str = "contoso-airflow-landing",
               idle_ms: int = 20000, max_messages: int = 500_000):
    @dlt.resource(name="erp_landing", write_disposition="append")
    def landing():
        # Batched into parts rather than one object per event: 93k separate
        # OneLake writes would be 93k round trips, and the part is the unit of
        # I/O everywhere else in this product. JSON Lines because that is what
        # a stream of envelopes is.
        part, batch, events = 0, [], 0
        def flush(part_no: int, rows: list[bytes]) -> dict:
            blob = b"\n".join(rows) + b"\n"
            rel = f"Files/landing/contoso_erp/{day}/changes/part-{part_no:04d}.jsonl"
            written = onelake.upload(target, workspace, lakehouse, rel, blob)
            return {"vendor": "contoso_erp", "feed": "changes", "page": part_no,
                    "path": rel, "bytes": written, "events": len(rows),
                    # Declared, not inferred: these are JSON Lines by extension
                    # and Debezium envelopes by meaning.
                    "dialect": "cdc"}

        for raw in _drain(bootstrap, topic, group, idle_ms, max_messages):
            batch.append(raw)
            events += 1
            if len(batch) >= 10_000:
                part += 1
                yield flush(part, batch)
                batch = []
        if batch:
            part += 1
            yield flush(part, batch)
        if events == 0:
            raise ValueError(
                f"{topic}: the stream was empty. Either the connector was "
                f"registered after the replay -- in which case the history was "
                f"snapshotted rather than captured -- or nothing was replayed.")

    return landing


def parse_envelope(blob: bytes) -> list[dict]:
    """Debezium envelopes → bronze rows, preserving the operation.

    `op` and `ts_ms` are kept: bronze's job is to be what arrived, and an ERP
    row without its operation cannot answer whether a customer was created,
    changed or removed. Silver decides what to do about that; bronze records it.
    """
    rows = []
    for line in blob.splitlines():
        if not line.strip():
            continue
        env = json.loads(line)
        payload = env.get("payload", env)
        # A delete carries its identity in `before`; everything else in `after`.
        state = payload.get("after") or payload.get("before") or {}
        rows.append({**state,
                     "__op": payload.get("op"),
                     "__ts_ms": payload.get("ts_ms")})
    return rows
