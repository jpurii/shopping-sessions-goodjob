# Shopping Sessions — team "good job"

Builders Buildathon 2026, Databricks track. Reads the click-stream CSVs in
`$DATA_DIR`, applies `rules.md` exactly, and writes `user_daily/` and
`campaign_daily/` as Parquet to `$OUTPUT_DIR`.

One Rust binary with **no dependencies**: the CSV dialect, timestamp parsing,
hashing, threading and the Parquet writer are all in `src/`.

## How it works

| Phase | What happens |
| --- | --- |
| A. Parse | Every input file is memory-mapped and cut into ~8 MB chunks at line ends; threads parse chunks with a hand-written parser for the rules' dialect (quotes as syntax, `\r` as data, exact field counts). Each event is validated on its own (window, type, product, price, late cutoff) and partitioned by `event_id`. Clicks are validated (window minus 7 days, known campaign). |
| B. Links | Union-find over all link pairs; every linked ID maps to its person's byte-wise smallest ID. |
| C. Dedup | Per `event_id` partition, the copy with the smallest `ingest_ts` wins and its own validity decides. Survivors and clicks are mapped to persons and repartitioned by person. |
| D. Sessions | Per person: events sorted by `ts`, split on gaps over 30 min. Clicks sorted by `(ts, click_id)` feed a sliding 7-day window: the last click at or before the start credits the session (ties: smallest `click_id`), and each campaign's stretch of being in the window adds assists per day in one step. |
| E. Write | A minimal Parquet writer (PLAIN, uncompressed, `DATE` logical type), one file per thread. |

## Build and test

```sh
cargo build --release
python scripts/run_fixtures.py fixtures -- target/release/sessions      # 36/36
DATA_DIR=dev OUTPUT_DIR=out target/release/sessions
python scripts/compare.py out dev-answer                                 # MATCH
docker build --platform linux/amd64 -t ss-entry .
sh scripts/docker_run.sh dev out                                         # scorer flags
```

`dev/` comes from the organizers' generator (`generate --seed 1 --events 1000000
--clicks 250000 --output dev`) and `dev-answer/` from `dev-answer.zip`.

## Code we did not write

- Rust standard library only. The image builds from `rust:1.90-slim-bookworm`
  and runs on `debian:bookworm-slim`.
- `fixtures/`, `scripts/run_fixtures.py` and `scripts/compare.py` come from the
  organizers' starter kit (the test scripts need `polars`).
- Written with AI assistance (Claude).
