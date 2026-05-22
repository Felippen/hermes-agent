# Sentinel Promptfoo Red-Team Pilot

This is a small Promptfoo pilot for the Hermes Sentinel profile. It is designed
to validate the red-team loop before expanding to every Hermes agent.

## Cheap config validation

Run without calling a live Hermes profile:

```bash
cd /Users/felipelamartine/.hermes/hermes-agent/redteam/sentinel
HERMES_PROMPTFOO_MOCK=1 npx promptfoo@latest eval -c promptfooconfig.smoke.yaml --no-cache --no-share -j 1
```

The full red-team config is `promptfooconfig.yaml`. Promptfoo may require email
verification before running red-team scans; the smoke config avoids that gate and
only verifies that the local provider wrapper works.

## Live Sentinel target

Point the provider at a Sentinel-compatible Hermes API server:

```bash
export OPENROUTER_API_KEY="..."
export HERMES_PROMPTFOO_SENTINEL_URL="http://127.0.0.1:8646/v1/chat/completions"
export HERMES_PROMPTFOO_SENTINEL_KEY="$API_SERVER_KEY"

cd /Users/felipelamartine/.hermes/hermes-agent/redteam/sentinel
npx promptfoo@latest redteam run --config promptfooconfig.yaml --env-file /Users/felipelamartine/.hermes/.env --no-cache --max-concurrency 1 --force
npx promptfoo@latest redteam report
```

Keep `numTests: 1` while tuning signal quality. Increase tests and plugins only
after the failures are useful and low-noise.
