# zVirt Exporter

Prometheus exporter for **zVirt / oVirt** that collects statistics from the engine API and exposes them via `/metrics`.

Exporter is written in Python using **asyncio + aiohttp** and serves metrics via **FastAPI + uvicorn** with a background collection loop.

---

## Features

- Asynchronous data collection from zVirt/oVirt API
- Background metrics cache (no blocking during scrape)
- Fast `/metrics` endpoint
- Designed for Prometheus
- systemd service included
- Handles large metric sets (~10–20 MB per scrape)

---

## How it works

The exporter uses a background task:

1. Every **15 seconds** it collects metrics from the zVirt API.
2. Results are stored in an in-memory cache.
3. The `/metrics` endpoint simply returns the cached data.

This prevents:

- worker timeouts
- slow scrapes
- gaps in Prometheus graphs

---

## Requirements

- Python 3.10+
- Access to zVirt/oVirt API
- Prometheus

Python packages:



fastapi
uvicorn
aiohttp


---

## Installation

### 1. Clone repository

```bash
git clone https://github.com/Freezepop/zvirt_exporter.git
cd zvirt_exporter
### 2. Install dependencies
pip install -r requirements.txt
```
Or manually:

```bash
pip install fastapi uvicorn aiohttp
```

### 3. Configure environment

Edit your environment file (used by systemd), for example:

/home/zvirt-exporter/.bash_profile

Example variables:

```bash
VIRT_SCHEME="https"
VIRT_URL="some-hostname.example.com"
USERNAME="some_user"
PASSWORD="some_password"
DOMAIN="example.com"
```

From the project directory:
```bash
uvicorn zvirt_exporter:app --host 0.0.0.0 --port 9190
```
Test:
```bash
curl https://localhost:9190/metrics
```

systemd service

Example service file:
/etc/systemd/system/zvirt-exporter.service
```bash
[Unit]
Description=zVirt Exporter Service
After=network.target

[Service]
EnvironmentFile=/home/zvirt-exporter/.bash_profile
Type=simple
User=zvirt-exporter
Group=zvirt-exporter
WorkingDirectory=/opt/zvirt-exporter
ExecStart=/usr/local/bin/uvicorn zvirt_exporter:app --host 0.0.0.0 --port 9190 --workers 1
Restart=always

[Install]
WantedBy=multi-user.target
```
Enable and start:
```bash
systemctl daemon-reload; systemctl enable --now zvirt-exporter
```

Check status:
```bash
systemctl status zvirt-exporter
```
---

Use 1 worker in uvicorn to avoid duplicate API calls.
---

Troubleshooting
Check logs
```bash
journalctl -u zvirt-exporter -f
```
Test endpoint
```bash
curl -v http://localhost:9190/metrics
```
---
Architecture overview
Prometheus --> /metrics --> FastAPI (uvicorn) --> METRICS CACHE <-- background task (15s) --> zVirt / oVirt API