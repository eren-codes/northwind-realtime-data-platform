#!/usr/bin/env python3
"""Create a story-led LinkedIn video for the complete Northwind data platform."""

from __future__ import annotations

import os
import subprocess
from pathlib import Path

import imageio_ffmpeg
from playwright.sync_api import sync_playwright


ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "artifacts" / "linkedin"
BASE = "http://127.0.0.1"
SIZE = {"width": 1920, "height": 1080}


def dotenv(path: Path) -> dict[str, str]:
    result: dict[str, str] = {}
    if path.exists():
        for raw in path.read_text(encoding="utf-8").splitlines():
            line = raw.strip()
            if line and not line.startswith("#") and "=" in line:
                key, value = line.split("=", 1)
                result[key.strip()] = value.strip().strip("\"").strip("'")
    return result


BASE_CSS = """
*{box-sizing:border-box} body{margin:0;width:100vw;height:100vh;overflow:hidden;color:#f8fafc;
font-family:Inter,ui-sans-serif,system-ui,sans-serif;background:radial-gradient(circle at 18% 8%,#164e63 0,transparent 30%),
radial-gradient(circle at 90% 95%,#1e3a8a 0,transparent 34%),linear-gradient(135deg,#07111f,#0f172a 58%,#020617)}
body:before{content:'';position:fixed;inset:0;opacity:.09;background-image:linear-gradient(#67e8f9 1px,transparent 1px),
linear-gradient(90deg,#67e8f9 1px,transparent 1px);background-size:54px 54px}.wrap{position:relative;height:100%;padding:70px 84px}
.tag{color:#67e8f9;font-size:20px;font-weight:800;letter-spacing:.19em;text-transform:uppercase}.title{font-size:64px;
line-height:1.06;letter-spacing:-.045em;margin:18px 0 16px;max-width:1500px}.sub{font-size:25px;line-height:1.5;color:#cbd5e1;
max-width:1360px;margin:0}.card{background:rgba(15,23,42,.82);border:1px solid rgba(103,232,249,.24);border-radius:20px;
box-shadow:0 20px 55px rgba(0,0,0,.3);backdrop-filter:blur(12px)}.in{animation:rise .7s ease-out both}@keyframes rise{from{opacity:0;
transform:translateY(20px)}to{opacity:1;transform:none}}.foot{position:absolute;left:84px;bottom:34px;color:#64748b;font-size:16px}
"""


def slide(page, body: str, ms: int) -> None:
    page.set_content(f"<!doctype html><html><head><style>{BASE_CSS}</style></head><body>{body}</body></html>")
    page.wait_for_timeout(ms)


def intro(page) -> None:
    slide(page, """<main class='wrap' style='display:grid;place-content:center;text-align:center'>
      <div class='in'><div class='tag'>REAL-TIME DATA ENGINEERING PROJECT</div>
      <h1 class='title' style='font-size:82px;margin-inline:auto'>Northwind Realtime Data Platform</h1>
      <p class='sub' style='font-size:30px;margin-inline:auto'>From SQL Server change events to governed analytics and live operational visibility.</p>
      <div style='margin-top:42px;color:#a5f3fc;font-size:22px'>CDC · Streaming · Lakehouse-style transforms · OLAP · Orchestration · Observability</div></div>
      </main>""", 5200)


def purpose(page) -> None:
    slide(page, """<main class='wrap'><div class='tag'>01 — THE GOAL</div><h1 class='title'>Turn an operational database into a reliable analytics product</h1>
      <p class='sub'>The platform captures changes continuously, preserves raw events, transforms warehouse facts incrementally, and exposes both business and pipeline health.</p>
      <section style='display:grid;grid-template-columns:repeat(3,1fr);gap:24px;margin-top:62px'>
       <div class='card in' style='padding:32px'><b style='font-size:27px;color:#67e8f9'>Low latency</b><p style='font-size:21px;line-height:1.5;color:#cbd5e1'>Changes move without repeating a full database load.</p></div>
       <div class='card in' style='padding:32px;animation-delay:.15s'><b style='font-size:27px;color:#67e8f9'>Replayable</b><p style='font-size:21px;line-height:1.5;color:#cbd5e1'>Kafka and MongoDB retain an auditable event trail.</p></div>
       <div class='card in' style='padding:32px;animation-delay:.3s'><b style='font-size:27px;color:#67e8f9'>Observable</b><p style='font-size:21px;line-height:1.5;color:#cbd5e1'>Every stage exposes latency, errors, and service state.</p></div>
      </section><div class='foot'>Northwind Realtime Data Platform</div></main>""", 6500)


def architecture(page) -> None:
    nodes = [("SQL Server","OLTP + CDC"),("Kafka","Event backbone"),("MongoDB","Raw event store"),("Spark","Incremental transforms"),("ClickHouse","Analytics warehouse"),("Grafana","Business insights")]
    html = ""
    for i, (name, sub) in enumerate(nodes):
        html += f"<div class='card in' style='padding:24px 18px;text-align:center;animation-delay:{i*.18}s'><b style='font-size:24px'>{name}</b><small style='display:block;color:#67e8f9;font-size:16px;margin-top:8px'>{sub}</small></div>"
        if i < len(nodes)-1:
            html += f"<div class='in' style='font-size:34px;color:#22d3ee;text-align:center;animation-delay:{i*.18+.1}s'>→</div>"
    slide(page, f"""<main class='wrap'><div class='tag'>02 — DATA FLOW</div><h1 class='title'>One event, six purposeful stages</h1>
      <p class='sub'>Each component has one clear responsibility, making the pipeline easier to scale, recover, and operate.</p>
      <section style='display:grid;grid-template-columns:1.4fr .35fr 1.3fr .35fr 1.4fr .35fr 1.5fr .35fr 1.5fr .35fr 1.4fr;align-items:center;gap:10px;margin-top:100px'>{html}</section>
      <div class='card in' style='margin-top:72px;padding:22px;text-align:center;font-size:21px;color:#cbd5e1;animation-delay:1.1s'>Airflow orchestrates full and incremental workflows · PostgreSQL staging tracks control state and data quality</div>
      <div class='foot'>Architecture: decoupled, replayable, and observable</div></main>""", 9000)


def cdc(page) -> None:
    slide(page, """<main class='wrap'><div class='tag'>03 — CDC & STREAMING</div><h1 class='title'>Only changed rows move through the pipeline</h1>
      <div style='display:grid;grid-template-columns:1.05fr .95fr;gap:38px;margin-top:42px'>
       <div class='card in' style='padding:38px'><div style='font-family:ui-monospace,monospace;font-size:20px;line-height:1.75;color:#cbd5e1'>
        <span style='color:#67e8f9'>UPDATE</span> Orders<br>SET ShippedDate = ...<br>WHERE OrderID = 11077;<br><br>
        <span style='color:#a78bfa'>CDC event</span> { operation, before, after, lsn, captured_at }</div></div>
       <div style='display:grid;gap:18px'><div class='card in' style='padding:27px;animation-delay:.15s'><b style='font-size:24px'>Producer</b><span style='display:block;color:#94a3b8;margin-top:7px;font-size:19px'>Reads SQL Server CDC tables and publishes ordered events.</span></div>
       <div class='card in' style='padding:27px;animation-delay:.3s'><b style='font-size:24px'>Kafka</b><span style='display:block;color:#94a3b8;margin-top:7px;font-size:19px'>Decouples capture from downstream processing.</span></div>
       <div class='card in' style='padding:27px;animation-delay:.45s'><b style='font-size:24px'>Consumer</b><span style='display:block;color:#94a3b8;margin-top:7px;font-size:19px'>Persists raw events and advances checkpoints safely.</span></div></div>
      </div><div class='foot'>Incremental by design · Idempotent checkpoints · Auditable history</div></main>""", 7500)


def transform(page) -> None:
    slide(page, """<main class='wrap'><div class='tag'>04 — TRANSFORM & SERVE</div><h1 class='title'>Spark builds analytics-ready facts; ClickHouse serves them fast</h1>
      <section style='display:grid;grid-template-columns:repeat(4,1fr);gap:22px;margin-top:70px'>
       <div class='card in' style='padding:30px'><b style='font-size:24px;color:#67e8f9'>1. Read checkpoint</b><p style='font-size:19px;color:#94a3b8;line-height:1.5'>Find the last committed warehouse position.</p></div>
       <div class='card in' style='padding:30px;animation-delay:.15s'><b style='font-size:24px;color:#67e8f9'>2. Transform</b><p style='font-size:19px;color:#94a3b8;line-height:1.5'>Join orders, products, customers, and employees.</p></div>
       <div class='card in' style='padding:30px;animation-delay:.3s'><b style='font-size:24px;color:#67e8f9'>3. Upsert facts</b><p style='font-size:19px;color:#94a3b8;line-height:1.5'>Apply only affected records to analytical tables.</p></div>
       <div class='card in' style='padding:30px;animation-delay:.45s'><b style='font-size:24px;color:#67e8f9'>4. Commit</b><p style='font-size:19px;color:#94a3b8;line-height:1.5'>Advance state only after successful validation.</p></div>
      </section><div class='card in' style='margin-top:45px;padding:28px 35px;display:flex;justify-content:space-between;animation-delay:.6s'>
       <span style='font-size:22px'>Columnar OLAP</span><span style='font-size:22px'>Fast aggregations</span><span style='font-size:22px'>Version-controlled schema</span><span style='font-size:22px'>Business-ready metrics</span></div>
      <div class='foot'>Spark for distributed transformation · ClickHouse for analytical performance</div></main>""", 7500)


def orchestration(page) -> None:
    slide(page, """<main class='wrap'><div class='tag'>05 — ORCHESTRATION</div><h1 class='title'>Airflow makes the workflow explicit and recoverable</h1>
      <div class='card in' style='margin-top:70px;padding:42px;display:grid;grid-template-columns:repeat(7,1fr);align-items:center;text-align:center;gap:12px'>
       <div><b>Inspect</b><small>source</small></div><i>→</i><div><b>Extract</b><small>changes</small></div><i>→</i><div><b>Transform</b><small>facts</small></div><i>→</i><div><b>Validate</b><small>quality</small></div>
      </div><style>.card b{font-size:25px}.card small{display:block;color:#67e8f9;font-size:17px;margin-top:9px}.card i{font-size:32px;color:#22d3ee}</style>
      <div style='display:grid;grid-template-columns:repeat(3,1fr);gap:22px;margin-top:35px'><div class='card in' style='padding:27px;animation-delay:.15s'>Full-load bootstrap</div><div class='card in' style='padding:27px;animation-delay:.3s'>Incremental CDC DAG</div><div class='card in' style='padding:27px;animation-delay:.45s'>Retries + quality gates</div></div>
      <div class='foot'>Scheduled runs, retries, dependencies, and operational history</div></main>""", 7000)


def live_stack(page) -> None:
    services = [("SQL Server","healthy"),("Kafka","healthy"),("MongoDB","healthy"),("PostgreSQL staging","healthy"),("ClickHouse","healthy"),("Airflow","running"),("CDC producer","running"),("CDC consumer","running"),("Grafana","running")]
    rows = "".join(f"<div><span>{n}</span><b>● {s}</b></div>" for n,s in services)
    slide(page, f"""<main class='wrap'><div class='tag'>06 — RUNNING PLATFORM</div><h1 class='title'>Nine services, one reproducible Docker environment</h1>
      <p class='sub'>Infrastructure, pipelines, dashboards, and SQL definitions are version-controlled together.</p>
      <div class='card in terminal' style='margin-top:38px;padding:28px 34px;font-family:ui-monospace,monospace'>{rows}</div>
      <style>.terminal div{{display:flex;justify-content:space-between;padding:10px 5px;border-bottom:1px solid #1e293b;font-size:19px}}.terminal div:last-child{{border:0}}.terminal b{{color:#4ade80;font-weight:600}}</style>
      <div class='foot'>Local-first development · Production-style separation of concerns</div></main>""", 7000)


def grafana(page, user: str, password: str) -> None:
    page.goto(f"{BASE}:3000/login", wait_until="networkidle")
    page.get_by_label("Email or username").fill(user)
    page.get_by_test_id("data-testid Password input field").fill(password)
    page.get_by_role("button", name="Log in").click()
    page.wait_for_load_state("networkidle")
    page.wait_for_timeout(1800)
    if "/login" in page.url:
        raise RuntimeError("Grafana login failed")
    page.goto(f"{BASE}:3000/d/nw-executive/northwind?orgId=1&kiosk", wait_until="networkidle", timeout=90_000)
    page.wait_for_timeout(5000)
    page.evaluate("""() => { const e=document.createElement('div');e.innerHTML='<b>07 — BUSINESS & PIPELINE OBSERVABILITY</b><span>Grafana is the final presentation layer — powered by the complete platform behind it.</span>';Object.assign(e.style,{position:'fixed',left:'36px',bottom:'30px',zIndex:999999,display:'grid',gap:'6px',padding:'18px 24px',borderRadius:'14px',background:'rgba(2,6,23,.9)',border:'1px solid #22d3ee88',color:'#f8fafc',font:'20px Inter,system-ui'});e.querySelector('span').style.cssText='font-size:16px;color:#a5f3fc';document.body.appendChild(e)}""")
    page.wait_for_timeout(6500)
    page.mouse.wheel(0, 650)
    page.wait_for_timeout(4000)
    page.goto(f"{BASE}:3000/d/nw-pipeline/northwind?orgId=1&kiosk", wait_until="networkidle", timeout=90_000)
    page.wait_for_timeout(5000)
    page.mouse.wheel(0, 500)
    page.wait_for_timeout(4000)


def outro(page) -> None:
    slide(page, """<main class='wrap' style='display:grid;place-content:center;text-align:center'><div class='in'>
      <div class='tag'>THE COMPLETE PICTURE</div><h1 class='title' style='font-size:72px;margin-inline:auto'>More than dashboards</h1>
      <p class='sub' style='font-size:29px;margin-inline:auto'>A complete path from operational change to trustworthy insight—with replay, orchestration, quality controls, and observability built in.</p>
      <div style='margin-top:45px;font-size:22px;color:#67e8f9'>SQL Server → Kafka → MongoDB → Spark → ClickHouse → Grafana</div>
      </div></main>""", 6000)


def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    env = dotenv(ROOT / ".env")
    user = os.getenv("GRAFANA_ADMIN_USER", env.get("GRAFANA_ADMIN_USER", "admin"))
    password = os.getenv("GRAFANA_ADMIN_PASSWORD", env.get("GRAFANA_ADMIN_PASSWORD", ""))
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True, args=["--disable-dev-shm-usage"])
        context = browser.new_context(viewport=SIZE, record_video_dir=str(OUT), record_video_size=SIZE, color_scheme="dark")
        page = context.new_page()
        intro(page); purpose(page); architecture(page); cdc(page); transform(page); orchestration(page); live_stack(page)
        grafana(page, user, password); outro(page)
        video = page.video; context.close(); browser.close(); webm = Path(video.path())
    target = OUT / "northwind-complete-project-linkedin.mp4"
    subprocess.run([imageio_ffmpeg.get_ffmpeg_exe(),"-y","-i",str(webm),"-c:v","libx264","-preset","medium","-crf","20","-pix_fmt","yuv420p","-movflags","+faststart","-an",str(target)],check=True)
    webm.unlink(); print(target)


if __name__ == "__main__": main()
