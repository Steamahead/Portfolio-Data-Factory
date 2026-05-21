# Cross-Model Variance Experiment

**Date:** 2026-04-30 11:08 UTC  
**Total calls:** 45  
**Setup:** 3 models x 3 tickers x 5 runs, identical headlines+price payload (cached once per ticker).

## Aggregated (mean +/- std across 5 runs)

| Ticker | Model | Sentiment mean | Sentiment std | Hype mean | Hype std | Avg latency | Errors |
|---|---|---|---|---|---|---|---|
| NVDA | gemini-2.5-flash | 55.1 | 14.28 | 68.6 | 0.42 | 24.3s | 3/5 |
| NVDA | gemini-3-flash-preview | 27.8 | 9.47 | 70.04 | 6.07 | 9.9s | 0/5 |
| NVDA | gemini-3.1-flash-lite-preview | 1.67 | 2.06 | 49.13 | 3.01 | 19.1s | 2/5 |
| WMT | gemini-2.5-flash | 47.6 | - | 20.5 | - | 23.7s | 4/5 |
| WMT | gemini-3-flash-preview | 37.2 | 2.19 | 26.44 | 4.1 | 12.2s | 0/5 |
| WMT | gemini-3.1-flash-lite-preview | 13.85 | 1.04 | 18.25 | 2.44 | 45.9s | 1/5 |
| TSLA | gemini-2.5-flash | -60.0 | 0.0 | 60.0 | - | 31.0s | 3/5 |
| TSLA | gemini-3-flash-preview | -67.0 | 4.47 | 77.0 | 4.47 | 10.9s | 0/5 |
| TSLA | gemini-3.1-flash-lite-preview | -24.93 | 4.39 | 45.1 | 8.61 | 26.8s | 2/5 |

## Raw results (45 rows)

| Run | Ticker | Model | Sentiment | Hype | S-Conf | H-Conf | Latency | Error |
|---|---|---|---|---|---|---|---|---|
| 1 | NVDA | gemini-2.5-flash | - | - | - | - | 1.18s | ServerError: 503 UNAVAILABLE. {'error': {'code': 503, 'message': 'This model is  |
| 1 | NVDA | gemini-3-flash-preview | 37.9 | 70.1 | MEDIUM | MEDIUM | 9.83s |  |
| 1 | NVDA | gemini-3.1-flash-lite-preview | - | - | - | - | 1.2s | ServerError: 503 UNAVAILABLE. {'error': {'code': 503, 'message': 'This model is  |
| 1 | WMT | gemini-2.5-flash | - | - | - | - | 1.02s | ServerError: 503 UNAVAILABLE. {'error': {'code': 503, 'message': 'This model is  |
| 1 | WMT | gemini-3-flash-preview | 36.1 | 28.1 | MEDIUM | MEDIUM | 22.68s |  |
| 1 | WMT | gemini-3.1-flash-lite-preview | 14.3 | 21.9 | MEDIUM | MEDIUM | 17.22s |  |
| 1 | TSLA | gemini-2.5-flash | - | - | - | - | 11.78s | ServerError: 503 UNAVAILABLE. {'error': {'code': 503, 'message': 'This model is  |
| 1 | TSLA | gemini-3-flash-preview | -65.0 | 75.0 | LOW | LOW | 10.96s |  |
| 1 | TSLA | gemini-3.1-flash-lite-preview | -30.0 | 42.9 | LOW | MEDIUM | 27.1s |  |
| 2 | NVDA | gemini-2.5-flash | 65.2 | 68.3 | MEDIUM | MEDIUM | 22.67s |  |
| 2 | NVDA | gemini-3-flash-preview | 21.5 | 74.1 | MEDIUM | MEDIUM | 10.28s |  |
| 2 | NVDA | gemini-3.1-flash-lite-preview | - | - | - | - | 1.57s | ServerError: 503 UNAVAILABLE. {'error': {'code': 503, 'message': 'This model is  |
| 2 | WMT | gemini-2.5-flash | - | - | - | - | 1.38s | ServerError: 503 UNAVAILABLE. {'error': {'code': 503, 'message': 'This model is  |
| 2 | WMT | gemini-3-flash-preview | 38.3 | 22.6 | MEDIUM | MEDIUM | 9.64s |  |
| 2 | WMT | gemini-3.1-flash-lite-preview | 14.3 | 17.1 | MEDIUM | MEDIUM | 26.72s |  |
| 2 | TSLA | gemini-2.5-flash | - | - | - | - | 29.42s | ServerError: 503 UNAVAILABLE. {'error': {'code': 503, 'message': 'This model is  |
| 2 | TSLA | gemini-3-flash-preview | -65.0 | 75.0 | LOW | LOW | 10.79s |  |
| 2 | TSLA | gemini-3.1-flash-lite-preview | - | - | - | - | 12.38s | ServerError: 503 UNAVAILABLE. {'error': {'code': 503, 'message': 'This model is  |
| 3 | NVDA | gemini-2.5-flash | 45.0 | 68.9 | MEDIUM | MEDIUM | 26.02s |  |
| 3 | NVDA | gemini-3-flash-preview | 23.9 | 64.1 | MEDIUM | MEDIUM | 9.99s |  |
| 3 | NVDA | gemini-3.1-flash-lite-preview | -0.7 | 52.6 | HIGH | MEDIUM | 31.33s |  |
| 3 | WMT | gemini-2.5-flash | 47.6 | 20.5 | MEDIUM | LOW | 23.71s |  |
| 3 | WMT | gemini-3-flash-preview | 40.1 | 21.8 | MEDIUM | MEDIUM | 9.19s |  |
| 3 | WMT | gemini-3.1-flash-lite-preview | 14.5 | 17.1 | MEDIUM | MEDIUM | 12.24s |  |
| 3 | TSLA | gemini-2.5-flash | - | - | - | - | 9.7s | ServerError: 503 UNAVAILABLE. {'error': {'code': 503, 'message': 'This model is  |
| 3 | TSLA | gemini-3-flash-preview | -65.0 | 75.0 | LOW | LOW | 11.01s |  |
| 3 | TSLA | gemini-3.1-flash-lite-preview | -22.5 | 54.6 | LOW | MEDIUM | 22.02s |  |
| 4 | NVDA | gemini-2.5-flash | - | - | - | - | 1.69s | ServerError: 503 UNAVAILABLE. {'error': {'code': 503, 'message': 'This model is  |
| 4 | NVDA | gemini-3-flash-preview | 37.9 | 77.8 | MEDIUM | MEDIUM | 9.5s |  |
| 4 | NVDA | gemini-3.1-flash-lite-preview | 2.7 | 47.6 | MEDIUM | HIGH | 13.88s |  |
| 4 | WMT | gemini-2.5-flash | - | - | - | - | 21.08s | ServerError: 503 UNAVAILABLE. {'error': {'code': 503, 'message': 'This model is  |
| 4 | WMT | gemini-3-flash-preview | 37.2 | 28.3 | MEDIUM | MEDIUM | 10.44s |  |
| 4 | WMT | gemini-3.1-flash-lite-preview | 12.3 | 16.9 | MEDIUM | MEDIUM | 127.33s |  |
| 4 | TSLA | gemini-2.5-flash | -60.0 | 60.0 | LOW | LOW | 30.92s |  |
| 4 | TSLA | gemini-3-flash-preview | -65.0 | 75.0 | LOW | LOW | 11.14s |  |
| 4 | TSLA | gemini-3.1-flash-lite-preview | -22.3 | 37.8 | LOW | MEDIUM | 31.15s |  |
| 5 | NVDA | gemini-2.5-flash | - | - | - | - | 1.15s | ServerError: 503 UNAVAILABLE. {'error': {'code': 503, 'message': 'This model is  |
| 5 | NVDA | gemini-3-flash-preview | 17.8 | 64.1 | MEDIUM | MEDIUM | 9.66s |  |
| 5 | NVDA | gemini-3.1-flash-lite-preview | 3.0 | 47.2 | MEDIUM | HIGH | 12.08s |  |
| 5 | WMT | gemini-2.5-flash | - | - | - | - | 1.14s | ServerError: 503 UNAVAILABLE. {'error': {'code': 503, 'message': 'This model is  |
| 5 | WMT | gemini-3-flash-preview | 34.3 | 31.4 | MEDIUM | MEDIUM | 9.19s |  |
| 5 | WMT | gemini-3.1-flash-lite-preview | - | - | - | - | 8.37s | ServerError: 503 UNAVAILABLE. {'error': {'code': 503, 'message': 'This model is  |
| 5 | TSLA | gemini-2.5-flash | -60.0 | None | LOW | INSUFFICIENT | 31.17s |  |
| 5 | TSLA | gemini-3-flash-preview | -75.0 | 85.0 | LOW | LOW | 10.69s |  |
| 5 | TSLA | gemini-3.1-flash-lite-preview | - | - | - | - | 3.51s | ServerError: 503 UNAVAILABLE. {'error': {'code': 503, 'message': 'This model is  |
