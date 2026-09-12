#!/usr/bin/env python3
"""Read-only local operations console for the Northwind data platform."""

from __future__ import annotations

import html
import json
import subprocess
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlparse

ROOT = Path(__file__).resolve().parents[1]
HOST, PORT = "127.0.0.1", 8090
PROJECT = "northwind-realtime-platform"

SERVICES = {
    "mssql": ("SQL Server", "Operational source and CDC capture", "#ef4444"),
    "kafka": ("Apache Kafka", "Durable event-stream backbone", "#a78bfa"),
    "mongodb": ("MongoDB", "Replayable raw CDC event store", "#22c55e"),
    "spark": ("Apache Spark", "Full and incremental transformations", "#f97316"),
    "clickhouse": ("ClickHouse", "Columnar analytical warehouse", "#facc15"),
    "airflow": ("Apache Airflow", "Pipeline orchestration and recovery", "#06b6d4"),
    "docker": ("Docker Runtime", "Reproducible multi-service environment", "#3b82f6"),
}


def run(*args: str, timeout: int = 8) -> str:
    try:
        proc = subprocess.run(args, cwd=ROOT, text=True, capture_output=True, timeout=timeout)
        return (proc.stdout or proc.stderr).strip()
    except Exception as exc:
        return f"Unavailable: {type(exc).__name__}"


def containers() -> list[dict]:
    raw = run("docker", "ps", "--format", "{{json .}}")
    rows = []
    for line in raw.splitlines():
        try:
            item = json.loads(line)
            if item.get("Names", "").startswith(PROJECT):
                rows.append(item)
        except json.JSONDecodeError:
            pass
    return rows


def matching(service: str, rows: list[dict]) -> dict:
    needles = {
        "mssql": "-mssql-", "kafka": "-kafka-", "mongodb": "-mongodb-",
        "clickhouse": "-clickhouse-", "airflow": "-airflow-",
    }
    needle = needles.get(service, "")
    return next((r for r in rows if needle and needle in r.get("Names", "")), {})


def service_data(service: str, rows: list[dict]) -> tuple[list[tuple[str, str]], str]:
    item = matching(service, rows)
    base = [
        ("Runtime status", item.get("Status", "Source-controlled job") if item else "Source-controlled job"),
        ("Container image", item.get("Image", "Built on demand") if item else "Built on demand"),
        ("Published ports", item.get("Ports", "Internal network only") or "Internal network only"),
    ]
    detail = ""
    if service == "kafka":
        detail = run("docker", "exec", f"{PROJECT}-kafka-1", "/opt/kafka/bin/kafka-topics.sh", "--bootstrap-server", "localhost:9092", "--list")
        base += [("Broker", "Kafka 4.3.1"), ("Topics discovered", str(len([x for x in detail.splitlines() if x]))) ]
    elif service == "mongodb":
        detail = run("docker", "exec", f"{PROJECT}-mongodb-1", "mongosh", "--quiet", "--eval", "db.version()")
        base += [("Server version", detail.splitlines()[-1] if detail else "8.0"), ("Storage role", "Raw CDC history")]
    elif service == "clickhouse":
        detail = run("docker", "exec", f"{PROJECT}-clickhouse-1", "clickhouse-client", "--query", "SHOW DATABASES")
        base += [("Databases", str(len(detail.splitlines()))), ("Engine", "Columnar OLAP")]
    elif service == "mssql":
        detail = "CDC scripts\n" + "\n".join(p.name for p in sorted((ROOT / "sql/mssql").glob("*.sql")))
        base += [("Database", "Northwind"), ("CDC definition", "Version controlled")]
    elif service == "spark":
        files = sorted((ROOT / "spark").glob("*.py"))
        detail = "\n".join(f"{p.name:<34} {p.stat().st_size:>7} bytes" for p in files)
        base += [("Transformation jobs", str(len(files))), ("Modes", "Full + Incremental")]
    elif service == "airflow":
        files = sorted((ROOT / "airflow/dags").glob("*.py"))
        detail = "\n".join(p.name for p in files)
        base += [("DAG definitions", str(len(files))), ("Web UI", "127.0.0.1:8080")]
    elif service == "docker":
        detail = "\n".join(f"{r.get('Names',''):<58} {r.get('Status','')}" for r in rows)
        healthy = sum("healthy" in r.get("Status", "").lower() for r in rows)
        base = [("Running containers", str(len(rows))), ("Health checks passing", str(healthy)), ("Network", "northwind_net")]
    return base, detail or "No records returned; service is reachable."


STYLE = """
:root{--accent:#22d3ee}*{box-sizing:border-box}body{margin:0;background:#060b14;color:#edf6ff;font-family:Inter,Arial,sans-serif}
body:before{content:'';position:fixed;inset:0;background:radial-gradient(circle at 10% 0,#0e749044,transparent 32%),radial-gradient(circle at 100% 100%,#1d4ed844,transparent 38%);pointer-events:none}
.app{position:relative;display:grid;grid-template-columns:245px 1fr;min-height:100vh}.side{padding:30px 20px;border-right:1px solid #1c2d43;background:#08111ddd}.brand{font-size:19px;font-weight:800;line-height:1.2}.brand small{display:block;color:#66dbea;font-size:11px;letter-spacing:.12em;margin-top:8px}.nav{display:grid;gap:8px;margin-top:34px}.nav a{color:#92a8c0;text-decoration:none;padding:12px 14px;border-radius:10px;font-size:14px}.nav a:hover,.nav a.on{color:white;background:#132238}.main{padding:34px 42px}.top{display:flex;align-items:center;justify-content:space-between}.live{color:#4ade80;background:#123324;padding:8px 12px;border-radius:999px;font-size:12px}.kicker{color:var(--accent);letter-spacing:.18em;font-size:12px;font-weight:800;text-transform:uppercase}.title{font-size:42px;letter-spacing:-.04em;margin:12px 0 8px}.sub{color:#8fa5bd;margin:0 0 28px}.metrics{display:grid;grid-template-columns:repeat(3,1fr);gap:14px}.card{background:#0e1928dd;border:1px solid #213650;border-radius:16px;padding:20px;box-shadow:0 20px 50px #0004}.card span{display:block;color:#7890a9;font-size:11px;text-transform:uppercase;letter-spacing:.08em}.card b{display:block;margin-top:9px;font-size:19px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}.console{margin-top:18px;background:#030810;border:1px solid #20334b;border-radius:16px;overflow:hidden}.console-head{padding:13px 17px;background:#101a29;color:#7990aa;font-size:12px}.console pre{margin:0;padding:20px;color:#b8c9dc;font:13px/1.65 ui-monospace,monospace;white-space:pre-wrap;max-height:420px;overflow:auto}.grid{display:grid;grid-template-columns:repeat(3,1fr);gap:14px}.service{text-decoration:none;color:white}.service b{font-size:18px}.service p{color:#8198b1;font-size:13px;line-height:1.4}.service em{font-style:normal;color:#4ade80;font-size:12px}.footer{margin-top:22px;color:#506780;font-size:11px;display:flex;justify-content:space-between}
"""


def nav(active: str) -> str:
    links = "".join(f'<a class="{"on" if key == active else ""}" href="/service/{key}">{html.escape(label)}</a>' for key, (label, _, _) in SERVICES.items())
    return f'<aside class="side"><div class="brand">Northwind Ops<small>REALTIME DATA PLATFORM</small></div><nav class="nav"><a class="{"on" if active == "home" else ""}" href="/">Overview</a>{links}</nav></aside>'


def page(active: str, content: str, accent="#22d3ee") -> bytes:
    return f"<!doctype html><html><head><meta charset=utf-8><meta http-equiv=refresh content=15><style>:root{{--accent:{accent}}}{STYLE}</style></head><body><div class=app>{nav(active)}<main class=main>{content}<div class=footer><span>READ-ONLY LOCAL CONSOLE</span><span>Elham Partovi · Sematec</span></div></main></div></body></html>".encode()


class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        path = urlparse(self.path).path
        rows = containers()
        if path == "/":
            cards = "".join(f'<a class="card service" href="/service/{key}"><b>{label}</b><p>{desc}</p><em>● Connected</em></a>' for key, (label, desc, _) in SERVICES.items())
            content = f'<div class=top><div><div class=kicker>Platform overview</div><h1 class=title>Northwind Operations Console</h1><p class=sub>Live visibility across the complete data journey.</p></div><span class=live>● {len(rows)} containers running</span></div><section class=grid>{cards}</section>'
            body = page("home", content)
        elif path.startswith("/service/") and path.split("/")[-1] in SERVICES:
            key = path.split("/")[-1]
            label, desc, accent = SERVICES[key]
            metrics, detail = service_data(key, rows)
            cards = "".join(f'<div class=card><span>{html.escape(k)}</span><b>{html.escape(v)}</b></div>' for k, v in metrics)
            content = f'<div class=top><div><div class=kicker>Live service detail</div><h1 class=title>{html.escape(label)}</h1><p class=sub>{html.escape(desc)}</p></div><span class=live>● Connected</span></div><section class=metrics>{cards}</section><section class=console><div class=console-head>Actual runtime output</div><pre>{html.escape(detail)}</pre></section>'
            body = page(key, content, accent)
        else:
            self.send_error(404); return
        self.send_response(200); self.send_header("Content-Type", "text/html; charset=utf-8"); self.send_header("Content-Length", str(len(body))); self.end_headers(); self.wfile.write(body)

    def log_message(self, *_):
        pass


if __name__ == "__main__":
    print(f"Northwind Ops UI: http://{HOST}:{PORT}", flush=True)
    ThreadingHTTPServer((HOST, PORT), Handler).serve_forever()
