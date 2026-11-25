# llm-auto-router

OpenAI-compatible auto-routing proxy for LLM chat completions.

Point your client at this service with model `auto`. It forwards each request
to an upstream OpenAI-compatible gateway and tries models in priority order:

- On 429 / 5xx / timeout / network error it switches to the next model
  immediately and returns the first successful result.
- On 400 / 422 (the request itself is invalid) it returns the error as-is —
  switching models would not help.
- Circuit breaker: a model that fails `FAIL_THRESHOLD` times in a row is
  benched for `BENCH_SECONDS`; after that it is probed with a real request
  and restored on success. 401/403 benches a model for `AUTH_BENCH_SECONDS`.
- Streaming: failover works until the first chunk is emitted; once output
  starts, the request is pinned to that model.
- No keys are stored: the client's `Authorization` header is passed through
  to the upstream as-is.

## Endpoints

- `POST /v1/chat/completions` — routed chat completions
- `GET /v1/models` — lists `auto` plus the configured models
- `GET /status` — breaker state and recent attempts

## Configuration (environment variables)

| Variable | Default |
| --- | --- |
| `PORT` | `3000` |
| `UPSTREAM_BASE` | `https://iwanmei.kdns.fr/v1` |
| `MODELS` | `agnes-3.0-flash,agnes-2.5-flash,deepseek-v4-flash,sensenova-6.8-flash,glm-5.2` |
| `FAIL_THRESHOLD` | `3` |
| `BENCH_SECONDS` | `30` |
| `AUTH_BENCH_SECONDS` | `300` |
| `FIRST_TOKEN_TIMEOUT_MS` | `20000` |
| `ATTEMPT_TIMEOUT_MS` | `90000` |

## Run locally

```sh
npm start
# or: node server.js
```

Zero dependencies; Node.js >= 18.
