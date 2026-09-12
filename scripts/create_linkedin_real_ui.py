#!/usr/bin/env python3
"""Record the LinkedIn showcase from real, live-backed project interfaces."""

from pathlib import Path
import os
import subprocess

import imageio_ffmpeg
from playwright.sync_api import sync_playwright

from scripts import create_linkedin_showcase_v3 as visual
from scripts import create_linkedin_architecture_demo as story

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "artifacts" / "linkedin"
SIZE = {"width": 1080, "height": 1350}


def ops_page(page, route: str, label: str, duration: int = 4800) -> None:
    page.goto(f"http://127.0.0.1:8090{route}", wait_until="networkidle", timeout=30_000)
    page.wait_for_timeout(900)
    if page.locator("h1").count() == 0:
        raise RuntimeError(f"Operations UI did not render: {route}")
    page.evaluate(
        """label => {
          const el = document.createElement('div');
          el.textContent = label;
          Object.assign(el.style, {position:'fixed',right:'28px',bottom:'25px',zIndex:9999,
            padding:'9px 13px',borderRadius:'999px',background:'#07111fee',color:'#67e8f9',
            border:'1px solid #22d3ee77',font:'700 11px Inter,Arial',letterSpacing:'.1em'});
          document.body.appendChild(el);
        }""",
        label,
    )
    page.wait_for_timeout(duration)


def grafana(page, user: str, password: str) -> None:
    page.goto("http://127.0.0.1:3000/login", wait_until="networkidle")
    page.get_by_label("Email or username").fill(user)
    page.get_by_test_id("data-testid Password input field").fill(password)
    page.get_by_role("button", name="Log in").click()
    page.wait_for_load_state("networkidle")
    page.wait_for_timeout(1200)
    if "/login" in page.url:
        raise RuntimeError("Grafana login failed")
    page.goto("http://127.0.0.1:3000/d/nw-pipeline/northwind?orgId=1&kiosk", wait_until="networkidle", timeout=90_000)
    page.wait_for_timeout(6500)


def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True, args=["--disable-dev-shm-usage"])
        ctx = browser.new_context(viewport=SIZE, record_video_dir=str(OUT), record_video_size=SIZE, color_scheme="dark")
        page = ctx.new_page()

        visual.cover(page)
        story.purpose(page)
        visual.architecture(page)
        ops_page(page, "/", "LIVE OPERATIONS OVERVIEW", 5200)
        ops_page(page, "/service/mssql", "ACTUAL SQL SERVER STATUS")
        ops_page(page, "/service/kafka", "ACTUAL KAFKA OUTPUT")
        ops_page(page, "/service/mongodb", "ACTUAL MONGODB STATUS")
        ops_page(page, "/service/spark", "ACTUAL SPARK JOBS")
        ops_page(page, "/service/clickhouse", "ACTUAL CLICKHOUSE DATABASES")
        ops_page(page, "/service/airflow", "ACTUAL AIRFLOW DAGS")
        ops_page(page, "/service/docker", "ACTUAL RUNNING CONTAINERS", 5600)
        grafana(page, os.getenv("GRAFANA_ADMIN_USER", "admin"), os.getenv("GRAFANA_ADMIN_PASSWORD", ""))
        visual.outro(page)

        video = page.video
        ctx.close()
        browser.close()
        raw = Path(video.path())

    target = OUT / "Elham-Partovi-Northwind-Real-UI-Final.mp4"
    subprocess.run(
        [imageio_ffmpeg.get_ffmpeg_exe(), "-y", "-i", str(raw), "-c:v", "libx264",
         "-preset", "medium", "-crf", "19", "-pix_fmt", "yuv420p", "-movflags", "+faststart",
         "-an", str(target)], check=True,
    )
    raw.unlink()
    print(target)


if __name__ == "__main__":
    main()
