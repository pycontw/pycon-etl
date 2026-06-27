# PoC: Social-media ingestion watermark via AIP-103 asset state store

**Status:** proof of concept (branch `poc/social-media-asset-watermark`, on top of `upgrade-to-3-3-0`)
**Requires:** Airflow 3.3 (asset state store / AIP-103)

## Problem

Every social-media ODS parser figures out "what's already ingested" by
re-querying the BigQuery sink on each run:

```python
# dags/utils/posts_insights/base.py (before)
last_post = self._query_last_post()          # SELECT created_at ... ORDER BY DESC LIMIT 1
new_posts = self._filter_new_posts(posts, last_post)
```

`_query_last_post()` is a hand-rolled watermark: the cursor is reconstructed
from the destination table every run. It costs an extra BigQuery query and
couples "where's my cursor" to the sink's contents.

## What AIP-103 gives us

Airflow 3.3 ships an **asset state store**: key/value state scoped to an asset
identity, persisted across Dag runs (not per task-instance). The Airflow docs
call out that it "replaces the common pattern of storing watermarks in Airflow
Variables." A task opts in by declaring the asset as an inlet/outlet and
receiving the injected `asset_state_store`:

```python
@task(inlets=[ASSET], outlets=[ASSET])
def load(asset_state_store=None):
    state = asset_state_store[ASSET]
    watermark = state.get("watermark", default="...")
    ...
    state.set("watermark", new_value)
```

## This PoC

Migrates **one** platform (Twitter) to demonstrate the mechanism end to end,
keeping the other three platforms on the legacy path unchanged.

### `dags/utils/posts_insights/base.py`

- `save_posts_and_insights(self, last_post=None) -> str | None`
  - accepts an injected watermark (`{"created_at": datetime}`); when omitted,
    falls back to the legacy `_query_last_post()` re-read, so **fb / ig /
    linkedin keep working untouched**.
  - returns the new watermark (newest `created_at`, ISO 8601 UTC) or `None`
    when nothing new was ingested.
- `_latest_watermark(posts_data)` — derives the watermark from the processed
  posts; defensive (only numeric epoch `created_at`), so non-migrated platforms
  that ignore the return value can never crash on it.

### `dags/ods/twitter_post_insights/dags.py`

- declares a logical asset `pycontw_twitter_posts`.
- the save task is now `@task(inlets=[...], outlets=[...])`, reads the watermark
  from the asset state store, passes it in, and advances it only when newer
  posts were stored.

### `dags/utils/posts_insights/twitter.py` (pre-existing regression fixed in passing)

The Twitter parser declared the **LinkedIn** table names
(`ods_pycontw_linkedin_posts` / `..._insights`). This was a regression
introduced by the dedup refactor `8b505ed` (2025-06-08): the pre-refactor
`twitter_post_insights/udfs.py` wrote to `ods_pycontw_twitter_posts` /
`..._insights`, but the extracted parser copy-pasted LinkedIn's table names.
Since then, tweets have been appended to the LinkedIn sink, colliding with
`linkedin.py`. Wiring the asset URI surfaced the mismatch, so the parser is
restored to `ods_pycontw_twitter_posts` / `..._insights`, matching the asset
URI.

> **Data note:** `ods_pycontw_twitter_posts*` is **not** a new table — it
> already holds the pre-2025-06-08 history. After this fix the Dag resumes
> writing there; with an empty asset watermark the first run falls back to
> `_query_last_post()` against that table and ingests tweets newer than the last
> historical row. The ~1 year of tweets misrouted into `ods_pycontw_linkedin_posts*`
> during the regression are not moved back (and now pollute the LinkedIn tables);
> reconciling/splitting that window is out of scope for this PoC.

## Verified

- `ruff check` — clean
- `mypy` (strict) — clean
- `pytest tests/test_dag_integrity.py` — Twitter Dag parses on 3.3.0b2

Not verified (needs live creds + running scheduler with the asset-state-store
table migrated): the actual cross-run watermark round-trip.

## If we adopt it

1. Roll the same wiring to fb / ig / linkedin (and YouTube's `publishedAfter`).
2. Drop `_query_last_post()` once every platform is migrated.
3. The downstream Blog Dag (see the social-media → Blog plan) can read the same
   asset state to learn which posts are new since the last blog build — the
   original reason the watermark was deferred to Airflow 3.3.
