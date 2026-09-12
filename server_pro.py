#!/usr/bin/env python3
"""Convenient root-level launcher for the Northwind Operations Console Pro."""

from ops_ui.server_pro import HOST, PORT, base


if __name__ == "__main__":
    print(f"Northwind Ops Pro: http://{HOST}:{PORT}", flush=True)
    base.ThreadingHTTPServer((HOST, PORT), base.Handler).serve_forever()
