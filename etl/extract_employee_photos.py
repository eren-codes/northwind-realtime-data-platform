#!/usr/bin/env python3
"""Extract Northwind employee photos from PostgreSQL staging into the Data Lake.

The classic Northwind ``Employees.Photo`` values usually contain a small OLE
wrapper before the real bitmap.  This job preserves the original bytes in the
raw zone, locates and extracts the embedded image into the curated zone, and
writes a SHA-256 manifest for traceability.
"""

from __future__ import annotations

import hashlib
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import NamedTuple

import psycopg


class DetectedImage(NamedTuple):
    image_format: str
    extension: str
    signature_offset: int
    payload: bytes


def required_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"Required environment variable is missing: {name}")
    return value


def atomic_write_bytes(path: Path, payload: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary_path = path.with_name(f".{path.name}.tmp")
    temporary_path.write_bytes(payload)
    temporary_path.replace(path)


def atomic_write_json(path: Path, document: dict) -> None:
    encoded = (json.dumps(document, indent=2, ensure_ascii=False) + "\n").encode(
        "utf-8"
    )
    atomic_write_bytes(path, encoded)


def valid_bmp_at(payload: bytes, offset: int) -> bool:
    """Reject accidental ``BM`` text and accept a plausible BMP header."""
    if offset + 18 > len(payload):
        return False

    available_size = len(payload) - offset
    declared_size = int.from_bytes(payload[offset + 2 : offset + 6], "little")
    pixel_offset = int.from_bytes(payload[offset + 10 : offset + 14], "little")
    dib_header_size = int.from_bytes(payload[offset + 14 : offset + 18], "little")

    return (
        14 <= pixel_offset < available_size
        and 12 <= dib_header_size <= 124
        and 26 <= declared_size <= available_size
    )


def extract_embedded_image(source: bytes) -> DetectedImage:
    """Find an embedded BMP/PNG/JPEG/GIF and remove any leading OLE wrapper."""
    search_window = source[: min(len(source), 4096)]
    candidates: list[tuple[int, str, str]] = []

    signatures = (
        (b"\x89PNG\r\n\x1a\n", "png", "png"),
        (b"\xff\xd8\xff", "jpeg", "jpg"),
        (b"GIF87a", "gif", "gif"),
        (b"GIF89a", "gif", "gif"),
    )

    for signature, image_format, extension in signatures:
        offset = search_window.find(signature)
        if offset >= 0:
            candidates.append((offset, image_format, extension))

    bmp_search_start = 0
    while True:
        bmp_offset = search_window.find(b"BM", bmp_search_start)
        if bmp_offset < 0:
            break
        if valid_bmp_at(source, bmp_offset):
            candidates.append((bmp_offset, "bmp", "bmp"))
            break
        bmp_search_start = bmp_offset + 2

    if not candidates:
        return DetectedImage("unknown", "bin", 0, source)

    offset, image_format, extension = min(candidates, key=lambda item: item[0])
    extracted = source[offset:]

    # BMP stores its exact file length in bytes 2..5.  Trimming by that value
    # prevents trailing OLE metadata from leaking into the curated image.
    if image_format == "bmp" and len(extracted) >= 6:
        declared_size = int.from_bytes(extracted[2:6], "little")
        if 26 <= declared_size <= len(extracted):
            extracted = extracted[:declared_size]

    # A JPEG may also be followed by wrapper bytes; keep through the first EOI.
    if image_format == "jpeg":
        end_marker = extracted.find(b"\xff\xd9", 3)
        if end_marker >= 0:
            extracted = extracted[: end_marker + 2]

    return DetectedImage(image_format, extension, offset, extracted)


def absolute_data_lake_path(root: Path, path: Path) -> str:
    """Return the stable container path used by downstream warehouse records."""
    return f"/data-lake/{path.relative_to(root).as_posix()}"


def main() -> None:
    data_lake_root = Path(os.getenv("DATA_LAKE_ROOT", "/data-lake"))
    raw_directory = data_lake_root / "raw" / "employee-photos"
    curated_directory = data_lake_root / "curated" / "employee-photos"
    manifest_path = data_lake_root / "manifests" / "employee_photos.json"

    expected_rows = int(os.getenv("EXPECTED_EMPLOYEE_ROWS", "9"))
    extracted_at = datetime.now(timezone.utc).isoformat()

    connection_options = {
        "host": os.getenv("POSTGRES_HOST", "postgres-staging"),
        "port": int(os.getenv("POSTGRES_PORT", "5432")),
        "dbname": os.getenv("POSTGRES_DB", "northwind_staging"),
        "user": os.getenv("POSTGRES_USER", "northwind"),
        "password": required_env("POSTGRES_PASSWORD"),
    }

    with psycopg.connect(**connection_options) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT employee_id, first_name, last_name, photo
                FROM staging.employees
                ORDER BY employee_id
                """
            )
            employees = cursor.fetchall()

    manifest_entries: list[dict] = []
    raw_photo_count = 0
    curated_image_count = 0
    missing_photo_count = 0
    unknown_format_count = 0

    for employee_id, first_name, last_name, database_photo in employees:
        if database_photo is None or len(database_photo) == 0:
            missing_photo_count += 1
            continue

        source = bytes(database_photo)
        raw_path = raw_directory / f"employee_{employee_id}.bin"
        atomic_write_bytes(raw_path, source)
        raw_photo_count += 1

        detected = extract_embedded_image(source)
        curated_path = curated_directory / (
            f"employee_{employee_id}.{detected.extension}"
        )
        atomic_write_bytes(curated_path, detected.payload)

        if detected.image_format == "unknown":
            unknown_format_count += 1
        else:
            curated_image_count += 1

        manifest_entries.append(
            {
                "employee_id": int(employee_id),
                "first_name": first_name,
                "last_name": last_name,
                "raw_path": absolute_data_lake_path(data_lake_root, raw_path),
                "curated_path": absolute_data_lake_path(
                    data_lake_root, curated_path
                ),
                "format": detected.image_format,
                "signature_offset": detected.signature_offset,
                "source_bytes": len(source),
                "curated_bytes": len(detected.payload),
                "source_sha256": hashlib.sha256(source).hexdigest(),
                "sha256": hashlib.sha256(detected.payload).hexdigest(),
                "extracted_at": extracted_at,
            }
        )

    manifest = {
        "manifest_version": 1,
        "generated_at": extracted_at,
        "source": "postgresql://postgres-staging/northwind_staging/staging.employees",
        "data_lake_root": "/data-lake",
        "counts": {
            "employee_rows": len(employees),
            "raw_photos": raw_photo_count,
            "curated_images": curated_image_count,
            "missing_photos": missing_photo_count,
            "unknown_formats": unknown_format_count,
        },
        "employees": manifest_entries,
    }
    atomic_write_json(manifest_path, manifest)

    print(f"EMPLOYEE_ROWS={len(employees)}")
    print(f"RAW_PHOTOS={raw_photo_count}")
    print(f"CURATED_IMAGES={curated_image_count}")
    print(f"MISSING_PHOTOS={missing_photo_count}")
    print(f"UNKNOWN_FORMATS={unknown_format_count}")
    print(f"MANIFEST_PATH={manifest_path}")

    checks_passed = (
        len(employees) == expected_rows
        and raw_photo_count == expected_rows
        and curated_image_count == expected_rows
        and missing_photo_count == 0
        and unknown_format_count == 0
    )
    if not checks_passed:
        print("DATA_LAKE_EXTRACTION=FAIL")
        raise RuntimeError("Employee photo extraction data-quality checks failed")

    print("DATA_LAKE_EXTRACTION=PASS")


if __name__ == "__main__":
    main()
