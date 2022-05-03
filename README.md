# Crypto Data Platform

A multi-service system for scraping, storing, and serving cryptocurrency market and repository statistics, with an ML client for downstream analysis.

## Components
- **backend/** – Python API server (gRPC) with MongoDB-backed scrapers (CoinGecko, CoinMarketCap)
- **data_service/** – Go dispatcher service between the ML client and the API server
- **MLclient/** – Python client for querying data and computing stats
- **proto/** – gRPC service definitions shared across components
