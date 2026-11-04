"""Build the GitHub Pages walkthrough from the captured PostgreSQL 16 stream.

    python scripts/build_site.py _site

The page shows every pgoutput message of one demo round as the server sent it (bytes), what the
decoder made of it, and the records the relay delivered with the example routing config. It is
generated from tests/fixtures/demo_stream.jsonl, the same capture the test suite replays.
"""

from __future__ import annotations

import dataclasses
import html
import json
import sys
from collections import Counter
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from cdc_relay import __version__  # noqa: E402
from cdc_relay import pgoutput as pg  # noqa: E402
from cdc_relay.assembler import TransactionAssembler  # noqa: E402
from cdc_relay.cli import _jsonable, read_capture  # noqa: E402
from cdc_relay.config import load_settings  # noqa: E402
from cdc_relay.lsn import format_lsn  # noqa: E402
from cdc_relay.routing import Router  # noqa: E402

REPO = "https://github.com/AkibUllah/pg-cdc-relay"
FIXTURE = ROOT / "tests" / "fixtures" / "demo_stream.jsonl"
CONFIG = ROOT / "examples" / "relay.yaml"
TEMPLATE = Path(__file__).with_name("site_template.html")

KIND = {
    pg.Begin: "begin",
    pg.Commit: "commit",
    pg.Relation: "schema",
    pg.Type: "schema",
    pg.Origin: "schema",
    pg.Insert: "change",
    pg.Update: "change",
    pg.Delete: "change",
    pg.Truncate: "change",
    pg.LogicalMessage: "message",
}


def esc(value: object) -> str:
    return html.escape(str(value), quote=True)


def short(value: object, limit: int = 32) -> str:
    text = (
        "∅"
        if value is None
        else ("⟨unchanged TOAST⟩" if value is pg.UNCHANGED_TOAST else str(value))
    )
    return text if len(text) <= limit else text[: limit - 1] + "…"


def summary(msg: pg.Message, relations: dict[int, pg.Relation]) -> str:
    def cols(rel: pg.Relation | None, values) -> str:
        names = [c.name for c in rel.columns] if rel else [f"${i}" for i in range(len(values))]
        pairs = [f"{n}={short(v)}" for n, v in zip(names, values, strict=False) if v is not None]
        return ", ".join(pairs[:5]) + (" …" if len(pairs) > 5 else "")

    if isinstance(msg, pg.Begin):
        return f"xid {msg.xid}, commits at {format_lsn(msg.final_lsn)}"
    if isinstance(msg, pg.Commit):
        return f"end {format_lsn(msg.end_lsn)}: the relay may confirm up to here once delivered"
    if isinstance(msg, pg.Relation):
        keys = [c.name for c in msg.columns if c.is_key]
        return (
            f"{msg.qualified_name}: {len(msg.columns)} columns, key ({', '.join(keys)}), "
            f"replica identity {msg.replica_identity}"
        )
    if isinstance(msg, (pg.Insert, pg.Update, pg.Delete)):
        rel = relations.get(msg.relation_oid)
        name = rel.qualified_name if rel else str(msg.relation_oid)
        if isinstance(msg, pg.Delete):
            return f"{name}: old key ({cols(rel, msg.old)})"
        return f"{name}: {cols(rel, msg.new)}"
    if isinstance(msg, pg.Truncate):
        names = [relations[o].qualified_name for o in msg.relation_oids if o in relations]
        extra = ", restart identity" if msg.restart_identity else ""
        return ", ".join(names) + extra
    if isinstance(msg, pg.LogicalMessage):
        kind = "transactional" if msg.transactional else "non-transactional"
        return f"prefix '{msg.prefix}', {kind}: {short(msg.content.decode('utf-8', 'replace'), 70)}"
    return type(msg).__name__


def hexdump(data: bytes, limit: int = 40) -> str:
    head = f"<b>{data[:1].hex()}</b> {data[1:limit].hex(' ')}"
    return head + (f" <i>… {len(data)} bytes</i>" if len(data) > limit else "")


def build(out_dir: Path) -> dict:
    settings = load_settings(str(CONFIG), env={})
    router = Router(settings.routing)
    assembler = TransactionAssembler()
    rows, records, counts = [], [], Counter()
    for at, data in read_capture(str(FIXTURE)):
        msg = pg.decode(data)
        counts[type(msg).__name__] += 1
        for tx in assembler.feed(msg):
            records.extend(router.route(tx)[0])
        decoded = _jsonable(dataclasses.asdict(msg))
        if isinstance(msg, pg.LogicalMessage):
            decoded["content"] = msg.content.decode("utf-8", "replace")
        rows.append(
            f'<tr class="{KIND[type(msg)]}"><td class="lsn">{format_lsn(at)}</td>'
            f'<td><span class="tag">{esc(type(msg).__name__)}</span></td>'
            f"<td>{esc(summary(msg, assembler.relations))}"
            f'<details><summary>bytes and decoded fields</summary><code class="hex">'
            f"{hexdump(data)}</code><pre>{esc(json.dumps(decoded, indent=2, ensure_ascii=False))}"
            f"</pre></details></td></tr>"
        )

    record_rows = []
    for r in records:
        value = json.dumps(r.value, indent=2, ensure_ascii=False)
        record_rows.append(
            f'<tr><td class="lsn">{esc(r.id)}</td><td><code>{esc(r.topic)}</code></td>'
            f"<td>{esc(r.key or '')}</td><td><details><summary>"
            f"{esc(short(json.dumps(r.value, ensure_ascii=False), 80))}</summary>"
            f"<pre>{esc(value)}</pre></details></td></tr>"
        )

    stats = {
        "messages": sum(counts.values()),
        "transactions": counts["Commit"],
        "records": len(records),
        "outbox": sum(1 for r in records if r.topic.startswith("swift.events.")),
    }
    page = TEMPLATE.read_text(encoding="utf-8").format(
        version=esc(__version__),
        repo=REPO,
        stream_rows="\n".join(rows),
        record_rows="\n".join(record_rows),
        config=esc(CONFIG.read_text()),
        **stats,
    )
    out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / "index.html").write_text(page, encoding="utf-8")
    (out_dir / "records.json").write_text(
        json.dumps([r.to_dict() for r in records], indent=2, ensure_ascii=False), encoding="utf-8"
    )
    return stats


if __name__ == "__main__":
    out = Path(sys.argv[1] if len(sys.argv) > 1 else "_site")
    result = build(out)
    print(f"wrote {out / 'index.html'}: {result}")
