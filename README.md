# Northwind Realtime Data Platform

A real-time data platform built around the **Northwind database**, designed to demonstrate real-time data ingestion, processing, storage, monitoring, and visualization using modern data engineering technologies.

## Overview

This project implements an end-to-end real-time data pipeline that captures changes from the Northwind database, processes streaming data, and makes the results available for analytics and monitoring.

The platform is designed with a modular architecture and can be deployed using Docker containers.

## Architecture

The main components of the platform include:

* **SQL Server** – Source database containing the Northwind dataset
* **CDC / Change Data Capture** – Captures changes from source tables
* **Apache Kafka** – Real-time message streaming and event transportation
* **Apache Spark** – Real-time data processing and transformation
* **Data Storage** – Stores processed and historical data
* **Grafana** – Monitoring and real-time visualization
* **Docker / Docker Compose** – Containerized deployment and service orchestration

### Data Flow

```text
Northwind SQL Server
        │
        ▼
   Change Capture
        │
        ▼
      Kafka
        │
        ▼
   Spark Streaming
        │
        ▼
  Data Storage / DB
        │
        ▼
     Grafana
```

## Project Structure

```text
northwind-realtime-data-platform/
│
├── docker-compose.yml
├── docker-compose.grafana.yml
├── README.md
│
├── config/
│   └── ...
│
├── scripts/
│   └── ...
│
├── sql/
│   └── ...
│
└── grafana/
    └── ...
```

## Requirements

Before running the project, make sure the following are installed:

* Docker
* Docker Compose
* Git

## Installation

Clone the repository:

```bash
git clone <repository-url>
cd northwind-realtime-data-platform
```

Create the required environment configuration if applicable:

```bash
cp .env.example .env
```

Update the environment variables according to your environment.

## Running the Platform

Start the main services:

```bash
docker compose up -d
```

Check running containers:

```bash
docker compose ps
```

To start Grafana separately:

```bash
docker compose -f docker-compose.grafana.yml up -d
```

### Grafana

Grafana runs on port `3000`.

When deployed on a server, it can be accessed through:

```text
http://<SERVER-IP>:3000
```

The Grafana data directory is persisted using Docker volumes to prevent dashboards, data sources, and configuration from being lost when containers are recreated.

## Monitoring

Grafana is used to monitor the real-time data platform and visualize operational and streaming metrics.

Example monitoring metrics include:

* Data ingestion rate
* Kafka messages
* Processing throughput
* Processing latency
* Error rate
* Pipeline status
* Resource utilization
* Database activity

## Data Pipeline

The pipeline follows these main stages:

1. **Source Data**

   * Northwind SQL Server database

2. **Change Detection**

   * Detect and capture changes from source tables

3. **Streaming**

   * Publish change events to Kafka topics

4. **Processing**

   * Process and transform streaming events using Spark

5. **Storage**

   * Store processed data for analytical and operational use

6. **Visualization**

   * Build monitoring and analytical dashboards in Grafana

## Persistence

Persistent Docker volumes are used for stateful services.

> **Important:** Do not use `docker compose down -v` unless you intentionally want to remove the associated Docker volumes and their stored data.

To stop the services without removing volumes:

```bash
docker compose down
```

To restart them:

```bash
docker compose up -d
```

## Useful Commands

Check running containers:

```bash
docker ps
```

Check service logs:

```bash
docker compose logs -f <service-name>
```

Restart a service:

```bash
docker compose restart <service-name>
```

Check Grafana port:

```bash
sudo ss -lntp | grep 3000
```

## Troubleshooting

### Grafana is only accessible through localhost
<img width="1705" height="776" alt="image" src="https://github.com/user-attachments/assets/da35d9ca-59dc-45f8-b973-6e7778e49291" />

If Grafana is listening on:

```text
127.0.0.1:3000
```

it is only accessible from the server itself.

The Docker port mapping should expose port `3000` on all interfaces:

```yaml
ports:
  - "3000:3000"
```

After updating the configuration:

```bash
docker compose -f docker-compose.grafana.yml up -d
```

Then verify:

```bash
sudo ss -lntp | grep 3000
```

The expected result should contain:

```text
0.0.0.0:3000
```

