
# OpenFIGI Tickers

**OpenFIGI Tickers is a production‑ready Python micro‑service that enriches iShares index constituents with Bloomberg OpenFIGI, Yahoo Finance and EOD Historical Data tickers, then writes the consolidated reference data back to Azure SQL — all running password‑less on Azure Kubernetes Service (AKS) behind an Azure DevOps CI/CD pipeline.**

[![Build Status](https://img.shields.io/badge/Azure%20DevOps-CI%20%E2%9C%94-blue)](#)
[![Python](https://img.shields.io/badge/Python-3.13-blue)](#)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue)](#license)

---

## Table of Contents

- [Features](#-features)
- [High‑level Architecture](#-high-level-architecture)
- [Workflow](#-workflow)
- [Tech Stack](#-tech-stack)
- [Getting Started](#-getting-started)
- [Build & Run with Docker](#-build--run-with-docker)
- [Continuous Delivery](#-continuous-delivery-azure-devops)
- [AKS Deployment Snippet](#-aks-deployment-snippet)
- [Database Output Schema](#-database-output-schema-default)
- [Logging & Monitoring](#-logging--monitoring)
- [Contributing](#-contributing)
- [License](#-license)

---

## ✨ Features

| Capability       | Details                                                                                                                                                                   |
|------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| **Ingest**       | Pulls the latest iShares ETF constituents and several supporting tables directly from Azure SQL.                                                                          |
| **Enrich**       | Sends batched requests to **OpenFIGI** for Bloomberg IDs, then augments the data set with **Yahoo Finance** and **EODHD** tickers.                                        |
| **Transform**    | Normalises and de‑duplicates data, assigns preferred exchanges / composite tickers, and adds currency metadata.                                                           |
| **Persist**      | Writes the final dataframe into a configurable Azure SQL table using bulk‑insert via *fast‑to‑sql*.                                                                       |
| **Cloud‑native** | Ships as a tiny Docker image (≈200 MB) and runs as a stateless pod on AKS. The app authenticates to Azure SQL with **Managed Identity** (no passwords).                   |
| **Automated CI/CD** | Azure DevOps builds, tests, publishes and deploys to AKS on every push to `main`.                                                                                      |

---

## 🏗️ High‑level Architecture

```text
┌─────────────────┐      Azure DevOps       ┌──────────────┐
│  GitHub Repo    │────────────────────────▶│   ACR Image  │
└─────────────────┘  build / push image     └──────────────┘
        │                                       │
        │ kubectl apply                         ▼
        │                               ┌──────────────────┐
        └──────────────────────────────▶│ AKS Deployment   │
                                        │  (Managed ID)   │
                                        └────────┬────────┘
                                                 │
                                          pyodbc │ access token
                                                 ▼
                                        ┌──────────────────┐
                                        │ Azure SQL DB     │
                                        └──────────────────┘
```

---

## 🛠️ Workflow

1. **Fetch constituents** – Execute `ISHARES_QUERY` against Azure SQL to pull the current index universe.  
2. **Batch & enrich with OpenFIGI** – Split into batches of up to 100 ISINs and send parallel POST requests (respecting rate limits) to the OpenFIGI API using the rotating tokens configured in `OPENFIGI_TOKENS`.  
3. **Augment with market tickers** – For each FIGI mapping, look up Yahoo Finance and EODHD symbols (optionally via BrightData proxy) to collect exchange‑specific codes.  
4. **Transform & prioritise** – Normalise exchange codes, deduplicate, and select preferred tickers using `EXCHANGES_PRIORITY_QUERY` / `EXCHANGES_COMP_PRIORITY_QUERY`; add ISO‑currency and clean security names.  
5. **Persist** – Bulk‑insert / MERGE the enriched dataframe into `OUTPUT_TABLE` via *fast‑to‑sql* using an Azure AD access token obtained through Managed Identity.  
6. **Log** – Emit structured JSON logs at each stage; container stdout is scraped by Azure Monitor in AKS for dashboards and alerting.  

---

## ⚙️ Tech Stack

* **Python 3.13** · pandas · requests · fast‑to‑sql · pyodbc · azure‑identity  
* **Azure SQL** (managed identity authentication)  
* **Docker** · **Kubernetes** (AKS)  
* **Azure DevOps** pipeline & **Azure Container Registry**

> All Python dependencies are pinned in [`requirements.txt`] — install locally with `pip install -r requirements.txt`.

---

## 🚀 Getting Started

### 1. Clone & set up

```bash
# Clone
git clone https://github.com/your-org/aks-stock-enrichment.git
cd aks-stock-enrichment

# Create virtual environment (optional but recommended)
python -m venv .venv && source .venv/bin/activate

# Install deps
pip install -r requirements.txt
```

### 2. Configure your **.env**

The app reads all configuration from environment variables (see `config/settings.py`). Create a `.env` file in the project root:

```ini
# Logging
LOG_LEVEL=INFO

# Output
OUTPUT_TABLE=dbo.SecurityReference

# iShares & supporting queries (examples)
ISHARES_QUERY=EXEC dbo.usp_Get_iShares_Universe
EOD_TICKERS_QUERY=EXEC dbo.usp_Get_EOD_Tickers
CURRENCIES_QUERY=SELECT * FROM ref.Currencies
ALL_EXCHANGES_QUERY=SELECT * FROM ref.Exchanges
EXCHANGES_PRIORITY_QUERY=SELECT * FROM ref.ExchangePriority
EXCHANGES_COMP_PRIORITY_QUERY=SELECT * FROM ref.CompositeExchangePriority

# OpenFIGI
OPENFIGI_TOKENS=123abc,456def
OPENFIGI_THREAD_COUNT=5
OPENFIGI_MAX_RETRIES=3
OPENFIGI_BACKOFF_FACTOR=2

# Optional BrightData proxy (for EODHD / Yahoo scraping)
BRIGHTDATA_PROXY=eu.proxy.com
BRIGHTDATA_PORT=22225
BRIGHTDATA_USER=user
BRIGHTDATA_PASSWD=passwd

# Azure SQL connection
MSSQL_SERVER=yourserver.database.windows.net
MSSQL_DATABASE=MarketData
MSSQL_AD_LOGIN=true            # Use Managed Identity on AKS
# If running locally without MI:
# MSSQL_USERNAME=db_user
# MSSQL_PASSWORD=Str0ngP@ss!
```

### 3. Run locally

```bash
python main.py
```

The script will log progress and insert/merge the enriched dataset into `OUTPUT_TABLE`.

---

## 🐳 Build & Run with Docker

```bash
# Build the image
docker build -t stock-enrichment:latest .

# Run (local credentials)
docker run --env-file .env stock-enrichment:latest
```

To build in Azure DevOps and push to ACR, see [`azure-pipelines.yml`](#continuous-delivery-azure-devops).

---

## 🔄 Continuous Delivery (Azure DevOps)

```yaml
# azure-pipelines.yml (simplified)
trigger:
  branches: { include: [ main ] }

variables:
  ACR_NAME: myregistry.azurecr.io
  IMAGE: $(ACR_NAME)/stock-enrichment:$(Build.BuildId)

stages:
- stage: Build
  jobs:
  - job: Build
    pool: { vmImage: 'ubuntu-latest' }
    steps:
    - task: Docker@2
      displayName: Build & push
      inputs:
        command: buildAndPush
        repository: $(IMAGE)
        dockerfile: Dockerfile
        tags: latest
        containerRegistry: $(ACR_NAME)
```

The CD stage typically applies a helm chart or raw k8s manifests that mount the `.env` as a Kubernetes **Secret** and assign the pod’s **Azure Workload Identity** or **AAD Pod Identity** to obtain the database access token.

---

## ☸️ AKS Deployment Snippet

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: stock-enrichment
spec:
  replicas: 1
  selector:
    matchLabels:
      app: stock-enrichment
  template:
    metadata:
      labels:
        app: stock-enrichment
      annotations:
        azure.workload.identity/client-id: <MANAGED_IDENTITY_CLIENT_ID>
    spec:
      serviceAccountName: stock-enrichment-sa
      containers:
        - name: app
          image: myregistry.azurecr.io/stock-enrichment:latest
          envFrom:
            - secretRef:
                name: stock-enrichment-env
          resources:
            requests:
              cpu: "100m"
              memory: "256Mi"
            limits:
              cpu: "500m"
              memory: "512Mi"
          imagePullPolicy: Always
```

> The pod receives an access token for Azure SQL automatically via `DefaultAzureCredential()`, which the `MSSQLDatabase` helper converts into a pyodbc access token for password‑less SQL access.

---

## 🗄️ Database Output Schema (default)

| Column          | Type            | Source                       |
|-----------------|-----------------|------------------------------|
| `isin`          | `NVARCHAR(12)`  | iShares constituent list     |
| `bbg_ticker`    | `NVARCHAR(32)`  | OpenFIGI mapping             |
| `ext2_ticker`   | `NVARCHAR(32)`  | EODHD                        |
| `ext3_ticker`   | `NVARCHAR(32)`  | Yahoo Finance                |
| `bbg_figi`      | `NVARCHAR(12)`  | OpenFIGI                     |
| `bbg_name`      | `NVARCHAR(256)` | OpenFIGI                     |
| `exchange`      | `NVARCHAR(8)`   | Normalised exchange code     |
| `currency`      | `CHAR(3)`       | ISO 4217                     |
| …               | …               | …                            |

> The full column map lives in `agent.py`.

---

## 📈 Logging & Monitoring

* **Structured logging** to stdout (see `logger.py`).  
* Capture logs via Azure Monitor or your preferred log aggregator in AKS.  

---

## 🤝 Contributing

1. Fork the repo & create your feature branch (`git checkout -b feature/amazing-feature`).  
2. Commit your changes (`git commit -am 'Add some feature'`).  
3. Push to the branch (`git push origin feature/amazing-feature`).  
4. Open a Pull Request.  

---

## 📝 License

Licensed under the **Apache License 2.0**.

---

## 🙏 Acknowledgements

* [iShares](https://www.ishares.com/)
* [Bloomberg OpenFIGI](https://www.openfigi.com/)
* [Yahoo Finance](https://finance.yahoo.com/)
* [EOD Historical Data](https://eodhistoricaldata.com/)

