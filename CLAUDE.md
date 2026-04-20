# Portfolio Data Factory

Monorepo: 7 autonomicznych pipeline'ów ETL na Azure Functions v2 (Python 3.12) + Azure SQL (`PortfolioMasterDB`).

- **Architektura, wzorce, gotchas**: `docs/PROJECT_SPEC.md`
- **Aktualny stan i następny krok**: `docs/STATUS.md`

## Quick Run

```bash
# Job scrapers (all 3 + email report)
.venv\Scripts\python.exe -X utf8 pracuj_scraper\scraper_monitor.py

# Individual scrapers
.venv\Scripts\python.exe -X utf8 -m nfj_scraper.nfj_data_scraper
.venv\Scripts\python.exe -X utf8 just_join_scraper\just_join_scraper.py
.venv\Scripts\python.exe -X utf8 -m pracuj_scraper.pracuj_premium_scraper

# CEE FX
.venv\Scripts\python.exe -X utf8 -m cee_fx_volatility.main [--backfill N] [--fx-only] [--news-only] [--reclassify]

# Gov Spending
.venv\Scripts\python.exe -X utf8 -m gov_spending_radar.main [--backfill N] [--date YYYY-MM-DD] [--classify]

# Azure Functions locally
func start
```

**Zawsze** `-X utf8` na Windows.

## CSV-Only Mode

When Azure SQL is unavailable, set `CSV_ONLY=1` in `.env`. All pipelines save to `csv_staging/` instead of DB. DB-read features (--classify, --reclassify, --cleanup) are auto-skipped.

To restore:
1. Remove `CSV_ONLY=1` from `.env`
2. Run `python -X utf8 csv_to_db.py` to import staged data
3. Optionally `python -X utf8 csv_to_db.py --dry-run` to preview first

## Zasady pracy

- Czytaj `docs/STATUS.md` po każdym `/clear` — tam jest aktualny stan i następny krok
- context-mode tools obowiązkowe (reguły w parent `CLAUDE.md`)
- Odpowiedzi max 500 słów, artefakty do plików

# context-mode — MANDATORY routing rules

You have context-mode MCP tools available. These rules are NOT optional — they protect your context window from flooding. A single unrouted command can dump 56 KB into context and waste the entire session.

## BLOCKED commands — do NOT attempt these

### curl / wget — BLOCKED
Any Bash command containing `curl` or `wget` is intercepted and replaced with an error message. Do NOT retry.
Instead use:
- `ctx_fetch_and_index(url, source)` to fetch and index web pages
- `ctx_execute(language: "javascript", code: "const r = await fetch(...)")` to run HTTP calls in sandbox

### Inline HTTP — BLOCKED
Any Bash command containing `fetch('http`, `requests.get(`, `requests.post(`, `http.get(`, or `http.request(` is intercepted and replaced with an error message. Do NOT retry with Bash.
Instead use:
- `ctx_execute(language, code)` to run HTTP calls in sandbox — only stdout enters context

### WebFetch — BLOCKED
WebFetch calls are denied entirely. The URL is extracted and you are told to use `ctx_fetch_and_index` instead.
Instead use:
- `ctx_fetch_and_index(url, source)` then `ctx_search(queries)` to query the indexed content

## REDIRECTED tools — use sandbox equivalents

### Bash (>20 lines output)
Bash is ONLY for: `git`, `mkdir`, `rm`, `mv`, `cd`, `ls`, `npm install`, `pip install`, and other short-output commands.
For everything else, use:
- `ctx_batch_execute(commands, queries)` — run multiple commands + search in ONE call
- `ctx_execute(language: "shell", code: "...")` — run in sandbox, only stdout enters context

### Read (for analysis)
If you are reading a file to **Edit** it → Read is correct (Edit needs content in context).
If you are reading to **analyze, explore, or summarize** → use `ctx_execute_file(path, language, code)` instead. Only your printed summary enters context. The raw file content stays in the sandbox.

### Grep (large results)
Grep results can flood context. Use `ctx_execute(language: "shell", code: "grep ...")` to run searches in sandbox. Only your printed summary enters context.

## Tool selection hierarchy

1. **GATHER**: `ctx_batch_execute(commands, queries)` — Primary tool. Runs all commands, auto-indexes output, returns search results. ONE call replaces 30+ individual calls.
2. **FOLLOW-UP**: `ctx_search(queries: ["q1", "q2", ...])` — Query indexed content. Pass ALL questions as array in ONE call.
3. **PROCESSING**: `ctx_execute(language, code)` | `ctx_execute_file(path, language, code)` — Sandbox execution. Only stdout enters context.
4. **WEB**: `ctx_fetch_and_index(url, source)` then `ctx_search(queries)` — Fetch, chunk, index, query. Raw HTML never enters context.
5. **INDEX**: `ctx_index(content, source)` — Store content in FTS5 knowledge base for later search.

## Output constraints

- Keep responses under 500 words.
- Write artifacts (code, configs, PRDs) to FILES — never return them as inline text. Return only: file path + 1-line description.
- When indexing content, use descriptive source labels so others can `ctx_search(source: "label")` later.

## ctx commands

| Command | Action |
|---------|--------|
| `ctx stats` | Call the `ctx_stats` MCP tool and display the full output verbatim |
| `ctx doctor` | Call the `ctx_doctor` MCP tool, run the returned shell command, display as checklist |
| `ctx upgrade` | Call the `ctx_upgrade` MCP tool, run the returned shell command, display as checklist |
