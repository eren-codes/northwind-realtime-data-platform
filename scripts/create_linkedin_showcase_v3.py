#!/usr/bin/env python3
"""Render a polished 4:5 LinkedIn showcase for the whole Northwind platform."""

from pathlib import Path
import os
import subprocess

import imageio_ffmpeg
from playwright.sync_api import sync_playwright

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "artifacts" / "linkedin"
W, H = 1080, 1350

CSS = """
*{box-sizing:border-box}body{margin:0;width:100vw;height:100vh;overflow:hidden;background:#050a13;color:#eef6ff;
font-family:Inter,Arial,sans-serif}body:before{content:'';position:fixed;inset:0;background:radial-gradient(circle at 15% 5%,#0e749055,transparent 34%),
radial-gradient(circle at 95% 95%,#1d4ed855,transparent 36%);pointer-events:none}.page{position:relative;height:100%;padding:64px 58px 54px}
.kicker{font-size:15px;font-weight:800;letter-spacing:.21em;color:#5ee5f7;text-transform:uppercase}.title{font-size:49px;line-height:1.06;
letter-spacing:-.045em;margin:16px 0 13px}.lead{font-size:20px;line-height:1.48;color:#aebdd0;margin:0;max-width:900px}.panel{background:linear-gradient(145deg,#111d30ee,#091321ee);
border:1px solid #29415d;border-radius:22px;box-shadow:0 24px 60px #0008;overflow:hidden}.chrome{height:48px;background:#101827;border-bottom:1px solid #24344a;
display:flex;align-items:center;padding:0 18px;gap:8px}.dot{width:10px;height:10px;border-radius:50%}.bar{margin-left:14px;height:25px;flex:1;background:#07101d;border-radius:7px;
color:#66809e;font:12px ui-monospace,monospace;padding:6px 12px}.badge{padding:7px 11px;border-radius:999px;background:#0b3941;color:#67e8f9;font-size:12px;font-weight:800}
.body{padding:25px}.mono{font-family:ui-monospace,SFMono-Regular,monospace}.muted{color:#7890aa}.green{color:#4ade80}.cyan{color:#67e8f9}.purple{color:#c4b5fd}
.foot{position:absolute;left:58px;right:58px;bottom:25px;display:flex;justify-content:space-between;color:#58708d;font-size:12px}.enter{animation:up .65s ease-out both}
.delay1{animation-delay:.12s}.delay2{animation-delay:.24s}.delay3{animation-delay:.36s}@keyframes up{from{opacity:0;transform:translateY(18px)}to{opacity:1;transform:none}}
.metric{padding:18px;border-radius:16px;background:#081322;border:1px solid #1d334c}.metric b{display:block;font-size:28px;margin-top:8px}.metric span{font-size:12px;color:#7f97b1;text-transform:uppercase;letter-spacing:.08em}
.row{display:flex;align-items:center;justify-content:space-between;padding:13px 4px;border-bottom:1px solid #1d2e43;font-size:15px}.row:last-child{border:0}
"""


def show(page, html: str, duration=6500):
    page.set_content(f"<!doctype html><html><head><style>{CSS}</style></head><body>{html}</body></html>")
    page.wait_for_timeout(duration)


def shell(title, product, inner, accent="#22d3ee"):
    return f"""<div class='panel enter' style='margin-top:34px;border-color:{accent}66'><div class='chrome'>
      <i class='dot' style='background:#fb7185'></i><i class='dot' style='background:#fbbf24'></i><i class='dot' style='background:#4ade80'></i>
      <div class='bar'>{product} · northwind-realtime-platform</div><span class='badge'>LIVE</span></div><div class='body'>{inner}</div></div>"""


def footer(step):
    return f"<div class='foot'><span>ELHAM PARTOVI · SEMATEC</span><span>{step} / 10</span></div>"


def cover(page):
    show(page, """<main class='page' style='display:flex;flex-direction:column;justify-content:center'>
      <div class='enter'><div class='kicker'>Data Engineering · Final Project</div><h1 class='title' style='font-size:70px'>Northwind<br><span class='cyan'>Realtime Data Platform</span></h1>
      <p class='lead' style='font-size:24px;max-width:820px'>An end-to-end CDC, streaming, transformation, analytics, and observability platform.</p>
      <div style='height:1px;background:linear-gradient(90deg,#22d3ee,transparent);margin:46px 0 36px'></div>
      <div style='display:grid;grid-template-columns:1fr 1fr;gap:17px'>
       <div class='metric'><span>Presented by</span><b style='font-size:25px'>Elham Partovi</b></div><div class='metric'><span>Instructor</span><b style='font-size:25px'>Vahid Ghorbani</b></div>
       <div class='metric' style='grid-column:1/-1'><span>Institute</span><b style='font-size:25px'>Sematec</b></div></div></div>
      <div class='foot'><span>BUILT FROM SOURCE TO INSIGHT</span><span>2026</span></div></main>""", 7000)


def architecture(page):
    items=[("SQL Server","CDC source"),("Kafka","event stream"),("MongoDB","raw history"),("Spark","transform"),("ClickHouse","warehouse"),("Grafana","insight")]
    cards="".join(f"<div class='metric enter delay{min(i,3)}' style='text-align:center'><span>{sub}</span><b style='font-size:19px'>{name}</b></div>" for i,(name,sub) in enumerate(items))
    show(page, f"""<main class='page'><div class='kicker'>01 · Architecture</div><h1 class='title'>One change.<br>One observable journey.</h1>
      <p class='lead'>A decoupled platform designed for replay, incremental processing, and fast analytical queries.</p>
      <div style='display:grid;grid-template-columns:1fr 1fr;gap:17px;margin-top:42px'>{cards}</div>
      <div class='panel enter delay3' style='padding:20px;margin-top:18px;text-align:center;color:#a5f3fc;font-size:15px'>Airflow orchestration · PostgreSQL control plane · Docker Compose</div>{footer('02')}</main>""", 7000)


def sqlserver(page):
    inner="""<div style='display:grid;grid-template-columns:1fr 1fr;gap:14px'><div class='metric'><span>CDC status</span><b class='green'>Enabled</b></div><div class='metric'><span>Capture tables</span><b>9</b></div></div>
    <div class='mono' style='background:#050b14;border-radius:14px;padding:22px;margin-top:17px;font-size:15px;line-height:1.7'><span class='purple'>UPDATE</span> dbo.Orders<br><span class='muted'>SET</span> ShippedDate = GETDATE()<br><span class='muted'>WHERE</span> OrderID = 11077;<br><br><span class='green'>✓ change captured with LSN checkpoint</span></div>"""
    show(page,f"<main class='page'><div class='kicker'>02 · Source</div><h1 class='title'>SQL Server CDC</h1><p class='lead'>Capture inserts, updates, and deletes without reloading the full operational database.</p>{shell('','SQL Server Management',inner,'#ef4444')}{footer('03')}</main>")


def kafka(page):
    bars="".join(f"<div style='height:{h}px;flex:1;background:linear-gradient(#a78bfa,#6d28d9);border-radius:5px 5px 0 0'></div>" for h in [36,60,44,82,55,96,71,104,78,115,88,122])
    inner=f"""<div style='display:grid;grid-template-columns:1fr 1fr;gap:14px'><div class='metric'><span>Topic</span><b style='font-size:18px'>northwind.cdc</b></div><div class='metric'><span>Consumer lag</span><b class='green'>0</b></div></div>
    <div class='metric' style='margin-top:16px'><span>Event throughput</span><div style='height:145px;display:flex;align-items:end;gap:9px;margin-top:18px'>{bars}</div></div>
    <div class='row'><span class='mono cyan'>key: Orders:11077</span><span class='badge'>UPDATE</span></div>"""
    show(page,f"<main class='page'><div class='kicker'>03 · Event Backbone</div><h1 class='title'>Apache Kafka</h1><p class='lead'>Ordered, durable events decouple database capture from every downstream consumer.</p>{shell('','Kafka Control Center',inner,'#a78bfa')}{footer('04')}</main>")


def mongodb(page):
    doc="""<div class='mono' style='background:#050b14;border-radius:14px;padding:20px;font-size:14px;line-height:1.65'><span class='muted'>{</span><br>&nbsp; <span class='cyan'>"event_id"</span>: "11077:0x00004A",<br>&nbsp; <span class='cyan'>"table"</span>: "Orders",<br>&nbsp; <span class='cyan'>"operation"</span>: <span class='green'>"UPDATE"</span>,<br>&nbsp; <span class='cyan'>"captured_at"</span>: "2026-09-12T...",<br>&nbsp; <span class='cyan'>"payload"</span>: { "ShipCountry": "USA" }<br><span class='muted'>}</span></div>
    <div style='display:grid;grid-template-columns:repeat(3,1fr);gap:10px;margin-top:16px'><div class='metric'><span>Store</span><b style='font-size:18px'>Raw</b></div><div class='metric'><span>Index</span><b class='green' style='font-size:18px'>Ready</b></div><div class='metric'><span>Replay</span><b style='font-size:18px'>Safe</b></div></div>"""
    show(page,f"<main class='page'><div class='kicker'>04 · Event Store</div><h1 class='title'>MongoDB</h1><p class='lead'>An immutable raw history keeps every CDC event auditable and replayable.</p>{shell('','MongoDB Compass',doc,'#22c55e')}{footer('05')}</main>")


def spark(page):
    stages="""<div class='row'><span>Stage 1 · Read checkpoint</span><b class='green'>SUCCESS</b></div><div class='row'><span>Stage 2 · Join dimensions</span><b class='green'>SUCCESS</b></div><div class='row'><span>Stage 3 · Build fact rows</span><b class='green'>SUCCESS</b></div><div class='row'><span>Stage 4 · Incremental write</span><b class='green'>SUCCESS</b></div>
    <div style='display:grid;grid-template-columns:1fr 1fr;gap:14px;margin-top:18px'><div class='metric'><span>Processing mode</span><b style='font-size:20px'>Incremental</b></div><div class='metric'><span>Failed stages</span><b class='green'>0</b></div></div>"""
    show(page,f"<main class='page'><div class='kicker'>05 · Processing</div><h1 class='title'>Apache Spark</h1><p class='lead'>Incremental jobs enrich events and rebuild only the warehouse facts that changed.</p>{shell('','Spark Jobs',stages,'#f97316')}{footer('06')}</main>")


def clickhouse(page):
    sql="""<div class='mono' style='background:#050b14;border-radius:14px;padding:20px;font-size:14px;line-height:1.65'><span class='purple'>SELECT</span> category_name,<br>&nbsp;&nbsp;round(sum(net_revenue), 2) revenue<br><span class='purple'>FROM</span> northwind_dw.fact_sales<br><span class='purple'>GROUP BY</span> category_name<br><span class='purple'>ORDER BY</span> revenue DESC;</div>
    <div class='row'><span>Beverages</span><b>$286,527</b></div><div class='row'><span>Dairy Products</span><b>$251,331</b></div><div class='row'><span>Confections</span><b>$177,099</b></div><div class='metric' style='margin-top:15px'><span>Query engine</span><b class='green' style='font-size:19px'>Columnar · OLAP · Fast</b></div>"""
    show(page,f"<main class='page'><div class='kicker'>06 · Analytics Warehouse</div><h1 class='title'>ClickHouse</h1><p class='lead'>A columnar serving layer turns transformed facts into interactive analytical queries.</p>{shell('','ClickHouse Query Console',sql,'#facc15')}{footer('07')}</main>")


def airflow(page):
    dag="""<div style='display:flex;align-items:center;justify-content:space-between;gap:8px;margin:15px 0 28px'><div class='metric' style='text-align:center'><span>Extract CDC</span><b class='green' style='font-size:16px'>✓</b></div><b class='cyan'>→</b><div class='metric' style='text-align:center'><span>Transform</span><b class='green' style='font-size:16px'>✓</b></div><b class='cyan'>→</b><div class='metric' style='text-align:center'><span>Validate</span><b class='green' style='font-size:16px'>✓</b></div></div>
    <div class='row'><span>northwind_incremental_cdc</span><b class='green'>Running</b></div><div class='row'><span>northwind_full_pipeline</span><b class='green'>Success</b></div><div class='row'><span>Retries / failures</span><b>0 / 0</b></div>"""
    show(page,f"<main class='page'><div class='kicker'>07 · Orchestration</div><h1 class='title'>Apache Airflow</h1><p class='lead'>DAGs coordinate dependencies, retries, checkpoints, and data-quality gates.</p>{shell('','Airflow DAG Overview',dag,'#06b6d4')}{footer('08')}</main>")


def docker(page):
    names=["mssql","kafka","mongodb","cdc-producer","cdc-consumer","spark","clickhouse","airflow","grafana"]
    rows="".join(f"<div class='row'><span class='mono'>{n}</span><b class='green'>● running</b></div>" for n in names)
    show(page,f"<main class='page'><div class='kicker'>08 · Platform Runtime</div><h1 class='title'>Docker Compose</h1><p class='lead'>The complete environment is reproducible, isolated, and observable as one stack.</p>{shell('','Container Desktop',rows,'#3b82f6')}{footer('09')}</main>",7000)


def grafana(page, user, password):
    page.goto("http://127.0.0.1:3000/login",wait_until="networkidle")
    page.get_by_label("Email or username").fill(user)
    page.get_by_test_id("data-testid Password input field").fill(password)
    page.get_by_role("button",name="Log in").click();page.wait_for_load_state("networkidle");page.wait_for_timeout(1200)
    if "/login" in page.url: raise RuntimeError("Grafana login failed")
    page.goto("http://127.0.0.1:3000/d/nw-executive/northwind?orgId=1&kiosk",wait_until="networkidle",timeout=90000);page.wait_for_timeout(5200)
    page.evaluate("""() => {let e=document.createElement('div');e.innerHTML='<b>09 · INSIGHT</b><span>Grafana is the destination—not the whole story.</span>';Object.assign(e.style,{position:'fixed',left:'25px',right:'25px',bottom:'24px',zIndex:99999,padding:'16px 20px',borderRadius:'14px',background:'#050a13ee',border:'1px solid #22d3ee88',color:'white',font:'16px Inter,Arial'});e.querySelector('span').style='display:block;color:#67e8f9;margin-top:4px;font-size:13px';document.body.appendChild(e)}""");page.wait_for_timeout(4500)


def outro(page):
    show(page,"""<main class='page' style='display:flex;flex-direction:column;justify-content:center;text-align:center'><div class='enter'>
      <div class='kicker'>FROM CHANGE TO DECISION</div><h1 class='title' style='font-size:63px'>Built end to end.<br><span class='cyan'>Engineered to be trusted.</span></h1>
      <p class='lead' style='margin:20px auto 0;font-size:22px'>CDC · Kafka · MongoDB · Spark · ClickHouse · Airflow · Grafana</p>
      <div class='panel' style='padding:25px;margin-top:45px'><b style='font-size:22px'>Elham Partovi</b><span style='display:block;color:#8fa5bd;margin-top:9px'>Instructor: Vahid Ghorbani · Sematec</span></div></div>
      <div class='foot'><span>NORTHWIND REALTIME DATA PLATFORM</span><span>THANK YOU</span></div></main>""",6500)


def main():
    OUT.mkdir(parents=True,exist_ok=True)
    user=os.getenv("GRAFANA_ADMIN_USER","admin");password=os.getenv("GRAFANA_ADMIN_PASSWORD","")
    with sync_playwright() as p:
        browser=p.chromium.launch(headless=True,args=["--disable-dev-shm-usage"])
        ctx=browser.new_context(viewport={"width":W,"height":H},record_video_dir=str(OUT),record_video_size={"width":W,"height":H},color_scheme="dark")
        page=ctx.new_page();cover(page);architecture(page);sqlserver(page);kafka(page);mongodb(page);spark(page);clickhouse(page);airflow(page);docker(page);grafana(page,user,password);outro(page)
        video=page.video;ctx.close();browser.close();raw=Path(video.path())
    target=OUT/"Elham-Partovi-Northwind-LinkedIn-Showcase.mp4"
    subprocess.run([imageio_ffmpeg.get_ffmpeg_exe(),"-y","-i",str(raw),"-c:v","libx264","-preset","medium","-crf","19","-pix_fmt","yuv420p","-movflags","+faststart","-an",str(target)],check=True)
    raw.unlink();print(target)

if __name__=="__main__":main()
