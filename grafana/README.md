# Northwind Grafana Dashboards

This package provisions Grafana, the official ClickHouse data-source
plugin, a ClickHouse business data source, a PostgreSQL operations data
source, and six version-controlled dashboards.

## Dashboards

1. Executive Overview
2. Sales Trends
3. Product Performance
4. Customers & Geography
5. Employees & Shipping
6. Real-Time Pipeline Health

## Security

Grafana is bound only to `127.0.0.1:3000`. Access it through an SSH
tunnel or VS Code port forwarding. Credentials are read from `.env` and
are not stored in the dashboard files.

## Compose command

Use both files for every Grafana lifecycle command:

```bash
docker compose \
  -f docker-compose.yml \
  -f docker-compose.grafana.yml \
  up -d grafana
```

Do not use `--remove-orphans`; the Airflow and real-time services are
intentionally managed through separate Compose overlays.
