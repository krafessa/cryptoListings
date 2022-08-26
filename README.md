# Crypto Data Platform

A multi-service system for scraping, storing, and serving cryptocurrency market and GitHub repository statistics, with a Python ML client for downstream analysis.

## Architecture

```
 ┌─────────────┐   gRPC    ┌──────────────┐   gRPC    ┌────────────────┐
 │  MLclient    │ ────────▶│ data_service │ ────────▶│ backend/ApiServer│
 │ (Seeker.py)  │           │   (Go)       │           │   (Python)       │
 └─────────────┘           └──────────────┘           └────────┬─────────┘
                                                                 │
                                                                 ▼
                                                          ┌─────────────┐
                                                          │  MongoDB    │
                                                          └─────────────┘
                                                                 ▲
                                                     ┌───────────┴───────────┐
                                                     │  CGscraper / CMCscraper │
                                                     │ (CoinGecko / CoinMarketCap) │
                                                     └─────────────────────────┘
```

- **MLclient** sends requests through the Go `data_service`, which dispatches them to the Python `ApiServer`.
- **ApiServer** reads from MongoDB, which is populated by two scrapers pulling market data (CoinMarketCap) and repository stats (CoinGecko).
- Communication between all services uses **gRPC**, with contracts defined once in `proto/` and compiled to both Go and Python bindings.

## Components

| Component | Language | Description |
|---|---|---|
| `proto/` | Protobuf | Shared gRPC service definitions (`api.proto`, `dataService.proto`) |
| `backend/` | Python | `ApiServer.py` — gRPC server that queries MongoDB and returns crypto stats/prices |
| `backend/database/` | Python | Scrapers (`CGscraper.py`, `CMCscraper.py`) and the MongoDB connection layer (`dbconnect.py`) |
| `backend/pb/`, `pb/proto/` | Go | Generated Go protobuf bindings for the API service |
| `data_service/` | Go | Dispatcher service relaying requests from the ML client to the API server, with local caching |
| `MLclient/` | Python | `Seeker.py` client for querying names/prices/stats, plus stats computation utilities |

## Tech Stack

- **Go** (data dispatcher, gRPC)
- **Python** (API server, scrapers, ML client)
- **gRPC / Protocol Buffers** (inter-service communication)
- **MongoDB** (persistence)
- **Selenium / BeautifulSoup** (scraping)
- **go-cache** (in-memory caching on the Go side)

## Getting Started

### Prerequisites
- Go 1.18+
- Python 3.10+
- A running MongoDB instance

### Backend (API server)
```bash
cd backend
pip install -r requirements.txt   # grpcio, pymongo, beautifulsoup4, selenium, pandas
python ApiServer.py
```

### Data service (Go dispatcher)
```bash
cd data_service
go run data_service.go
```

### ML client
```bash
cd MLclient
python -c "from Seeker import Seeker; s = Seeker(); print(s.lookUpNames(None))"
```

### Regenerating protobuf bindings
```bash
# Python
python -m grpc_tools.protoc -I proto --python_out=. --grpc_python_out=. proto/api.proto

# Go
protoc --go_out=. --go-grpc_out=. proto/dataService.proto
```

## Project Timeline

Built during a software engineering internship, May–August 2022.
