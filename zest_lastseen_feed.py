#!/usr/bin/env python3
"""
Zest BD Rhythm — Nightly HubSpot -> last-seen.json feed
=======================================================

One-way nightly job: reads HubSpot activity data and publishes a JSON feed that
the BD Rhythm planner fetches on page load.

For each current-customer company it finds the most recent FACE-TO-FACE activity
(one of nine call/meeting activity types) and outputs a map keyed by the
company's Acumatica customer ID (property: acumaticaid):

    {
      "generated": "2026-06-15",
      "lastSeen": { "C11329": "2026-05-28", "C547": "2026-04-13", "C8155": "" }
    }

Direction: HubSpot -> feed -> planner.  Nothing is written back into HubSpot.

Run:    python zest_lastseen_feed.py
Env:    HUBSPOT_TOKEN   (required)
        FEED_OUTPUT_PATH            (optional, default ./last-seen.json)
        TZ=Australia/Sydney         (recommended)

Schedule once nightly, 04:00 Australia/Sydney (use a timezone-aware scheduler so
AEST/AEDT is handled automatically).

Portal: 24159248  |  Region: AP1
"""

import os
import sys
import json
import time
import logging
import tempfile
from datetime import datetime
from zoneinfo import ZoneInfo

import requests

# --------------------------------------------------------------------------- #
# Configuration
# --------------------------------------------------------------------------- #

HUBSPOT_BASE = "https://api.hubapi.com"
TOKEN = os.environ.get("HUBSPOT_TOKEN", "")
OUTPUT_PATH = os.environ.get("FEED_OUTPUT_PATH", "last-seen.json")
SYDNEY = ZoneInfo("Australia/Sydney")

# --- GitHub Pages publishing (optional) ------------------------------------ #
# When these are set, the script commits last-seen.json into a GitHub repo via
# the GitHub Contents API, and GitHub Pages serves it publicly for free.
# This is the recommended hosting for a Render cron deployment.
#   GITHUB_TOKEN     a fine-grained PAT with Contents: read & write on the repo
#   GITHUB_REPO      "owner/repo"  e.g. "zest-coffee/bd-rhythm-feed"
#   GITHUB_FILE_PATH path in the repo, e.g. "last-seen.json"
#   GITHUB_BRANCH    branch to commit to (default "main")
GITHUB_TOKEN = os.environ.get("GITHUB_TOKEN", "")
GITHUB_REPO = os.environ.get("GITHUB_REPO", "")
GITHUB_FILE_PATH = os.environ.get("GITHUB_FILE_PATH", "last-seen.json")
GITHUB_BRANCH = os.environ.get("GITHUB_BRANCH", "main")

# The nine activity types that count as a genuine face-to-face "visit".
# Stored on call / meeting records in the `hs_activity_type` field.
QUALIFYING_F2F = {
    "BD Install",
    "BD F2F Visit",
    "BD Explore F2F",
    "BD Tasting",
    "CR Proactive F2F",
    "CR Emergency Delivery",
    "CR On Site Issue Fix",
    "CR On Site Training",
    "CR On Site Dial In /Flavour Fix",
}

# Company property holding the Acumatica customer ID (the planner's match key).
ACUMATICA_PROP = "acumaticaid"

# Network behaviour
PAGE_LIMIT = 100          # HubSpot search page size (max 100 for v3 search)
BATCH_SIZE = 100          # batch-read page size for activities
MAX_RETRIES = 6
RETRY_BACKOFF = 2.0       # seconds, exponential

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
)
log = logging.getLogger("zest-feed")


# --------------------------------------------------------------------------- #
# HTTP helper with retry / rate-limit handling
# --------------------------------------------------------------------------- #

def _headers():
    return {
        "Authorization": f"Bearer {TOKEN}",
        "Content-Type": "application/json",
    }


def _request(method, path, **kwargs):
    """Single HubSpot request with backoff on 429 / 5xx."""
    url = path if path.startswith("http") else f"{HUBSPOT_BASE}{path}"
    for attempt in range(MAX_RETRIES):
        resp = requests.request(method, url, headers=_headers(), timeout=30, **kwargs)
        if resp.status_code == 429 or resp.status_code >= 500:
            # Respect Retry-After if present, else exponential backoff
            wait = float(resp.headers.get("Retry-After", RETRY_BACKOFF * (2 ** attempt)))
            log.warning("Rate/again (%s) on %s — waiting %.1fs (attempt %d/%d)",
                        resp.status_code, path, wait, attempt + 1, MAX_RETRIES)
            time.sleep(wait)
            continue
        if not resp.ok:
            raise RuntimeError(f"HubSpot {method} {path} -> {resp.status_code}: {resp.text[:400]}")
        return resp.json()
    raise RuntimeError(f"HubSpot {method} {path} failed after {MAX_RETRIES} retries")


# --------------------------------------------------------------------------- #
# Step 1 — fetch all customer-stage companies
# --------------------------------------------------------------------------- #

def fetch_customer_companies():
    """Return list of {id, name, acumaticaid} for lifecyclestage == customer."""
    companies = []
    after = None
    while True:
        body = {
            "filterGroups": [{
                "filters": [{
                    "propertyName": "lifecyclestage",
                    "operator": "EQ",
                    "value": "customer",
                }]
            }],
            "properties": ["name", "lifecyclestage", ACUMATICA_PROP],
            "limit": PAGE_LIMIT,
        }
        if after:
            body["after"] = after
        data = _request("POST", "/crm/v3/objects/companies/search", data=json.dumps(body))
        for r in data.get("results", []):
            p = r.get("properties", {})
            companies.append({
                "id": r["id"],
                "name": p.get("name", "") or "",
                "acumaticaid": (p.get(ACUMATICA_PROP) or "").strip(),
            })
        after = data.get("paging", {}).get("next", {}).get("after")
        if not after:
            break
    log.info("Fetched %d customer-stage companies", len(companies))
    return companies


# --------------------------------------------------------------------------- #
# Step 2 — associated call & meeting IDs per company
# --------------------------------------------------------------------------- #

def fetch_associated_ids(company_id, to_object):
    """Return list of associated activity IDs for one company.
    to_object is 'calls' or 'meetings'."""
    ids = []
    after = None
    while True:
        path = f"/crm/v4/objects/companies/{company_id}/associations/{to_object}?limit=500"
        if after:
            path += f"&after={after}"
        data = _request("GET", path)
        for r in data.get("results", []):
            # v4 returns {toObjectId, associationTypes:[...]}
            ids.append(str(r.get("toObjectId")))
        after = data.get("paging", {}).get("next", {}).get("after")
        if not after:
            break
    return ids


# --------------------------------------------------------------------------- #
# Step 3 — batch-read activity records (type + occurred date)
# --------------------------------------------------------------------------- #

# For calls the occurrence time is hs_timestamp; for meetings prefer
# hs_meeting_start_time, falling back to hs_timestamp.
CALL_PROPS = ["hs_activity_type", "hs_timestamp"]
MEETING_PROPS = ["hs_activity_type", "hs_meeting_start_time", "hs_timestamp"]


def batch_read_activities(object_type, ids, props):
    """Batch-read activity records. object_type is 'calls' or 'meetings'.
    Returns {id: {properties}}."""
    out = {}
    for i in range(0, len(ids), BATCH_SIZE):
        chunk = ids[i:i + BATCH_SIZE]
        body = {
            "properties": props,
            "inputs": [{"id": x} for x in chunk],
        }
        data = _request("POST", f"/crm/v3/objects/{object_type}/batch/read",
                        data=json.dumps(body))
        for r in data.get("results", []):
            out[r["id"]] = r.get("properties", {})
    return out


# --------------------------------------------------------------------------- #
# Date helpers
# --------------------------------------------------------------------------- #

def _to_sydney_date(raw):
    """Convert a HubSpot timestamp (ms epoch string or ISO) to a Sydney YYYY-MM-DD."""
    if not raw:
        return None
    try:
        if str(raw).isdigit():
            dt = datetime.fromtimestamp(int(raw) / 1000, tz=ZoneInfo("UTC"))
        else:
            # ISO 8601, may end with Z
            dt = datetime.fromisoformat(str(raw).replace("Z", "+00:00"))
        return dt.astimezone(SYDNEY).date().isoformat()
    except Exception as e:  # noqa
        log.debug("Unparseable date %r: %s", raw, e)
        return None


def occurred_date(props, is_meeting):
    """Pick the date the activity *happened* (not created/updated)."""
    if is_meeting:
        raw = props.get("hs_meeting_start_time") or props.get("hs_timestamp")
    else:
        raw = props.get("hs_timestamp")
    return _to_sydney_date(raw)


def is_qualifying(props):
    return (props.get("hs_activity_type") or "") in QUALIFYING_F2F


# --------------------------------------------------------------------------- #
# Step 4/5 — build the feed
# --------------------------------------------------------------------------- #

def build_last_seen_feed(companies):
    last_seen = {}
    exceptions = []          # customer-stage companies with no acumaticaid
    no_f2f = 0

    total = len(companies)
    for idx, co in enumerate(companies, 1):
        acu = co["acumaticaid"]
        if not acu:
            exceptions.append({"hubspotId": co["id"], "name": co["name"]})
            continue

        call_ids = fetch_associated_ids(co["id"], "calls")
        meeting_ids = fetch_associated_ids(co["id"], "meetings")

        dates = []
        if call_ids:
            calls = batch_read_activities("calls", call_ids, CALL_PROPS)
            for p in calls.values():
                if is_qualifying(p):
                    d = occurred_date(p, is_meeting=False)
                    if d:
                        dates.append(d)
        if meeting_ids:
            meetings = batch_read_activities("meetings", meeting_ids, MEETING_PROPS)
            for p in meetings.values():
                if is_qualifying(p):
                    d = occurred_date(p, is_meeting=True)
                    if d:
                        dates.append(d)

        if dates:
            last_seen[acu] = max(dates)   # ISO dates compare lexicographically
        else:
            last_seen[acu] = ""           # customer, but never an F2F on record
            no_f2f += 1

        if idx % 50 == 0 or idx == total:
            log.info("  processed %d/%d companies", idx, total)

    payload = {
        "generated": datetime.now(SYDNEY).date().isoformat(),
        "lastSeen": last_seen,
    }
    log.info("Feed built: %d keyed customers, %d with no F2F yet, %d missing acumaticaid",
             len(last_seen), no_f2f, len(exceptions))
    if exceptions:
        log.warning("DATA-QUALITY: %d customer companies have no acumaticaid (excluded):",
                    len(exceptions))
        for e in exceptions[:50]:
            log.warning("    [%s] %s", e["hubspotId"], e["name"])
        if len(exceptions) > 50:
            log.warning("    ... and %d more", len(exceptions) - 50)
    return payload, exceptions


# --------------------------------------------------------------------------- #
# Publish — atomic write so the planner never reads a half file
# --------------------------------------------------------------------------- #

def publish_json(payload, path):
    """Write atomically to a local path. For S3/R2/GCS, swap this for an
    upload call (see README) — keep the same atomic/whole-file guarantee."""
    d = os.path.dirname(os.path.abspath(path))
    fd, tmp = tempfile.mkstemp(dir=d, suffix=".tmp")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, separators=(",", ":"))
        os.replace(tmp, path)   # atomic on POSIX
    finally:
        if os.path.exists(tmp):
            os.remove(tmp)
    log.info("Published feed -> %s (%d bytes)", path, os.path.getsize(path))


def publish_github(payload):
    """Commit the feed JSON into a GitHub repo via the Contents API.
    GitHub Pages then serves it publicly. Returns True on success.
    Does nothing (returns False) if GitHub env vars aren't configured."""
    if not (GITHUB_TOKEN and GITHUB_REPO):
        return False
    import base64
    api = f"https://api.github.com/repos/{GITHUB_REPO}/contents/{GITHUB_FILE_PATH}"
    headers = {
        "Authorization": f"Bearer {GITHUB_TOKEN}",
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
    }
    body_json = json.dumps(payload, ensure_ascii=False, separators=(",", ":"))
    content_b64 = base64.b64encode(body_json.encode("utf-8")).decode("ascii")

    # Need the current file's SHA to update it (omit on first-ever create).
    sha = None
    try:
        r = requests.get(api, headers=headers,
                         params={"ref": GITHUB_BRANCH}, timeout=30)
        if r.status_code == 200:
            sha = r.json().get("sha")
    except Exception:  # noqa
        pass

    commit = {
        "message": f"Update last-seen feed {payload.get('generated','')}",
        "content": content_b64,
        "branch": GITHUB_BRANCH,
    }
    if sha:
        commit["sha"] = sha
    r = requests.put(api, headers=headers, data=json.dumps(commit), timeout=30)
    if not r.ok:
        raise RuntimeError(f"GitHub publish failed {r.status_code}: {r.text[:300]}")
    log.info("Published feed -> github.com/%s/%s (branch %s)",
             GITHUB_REPO, GITHUB_FILE_PATH, GITHUB_BRANCH)
    return True


# --------------------------------------------------------------------------- #
# Main
# --------------------------------------------------------------------------- #

def main():
    if not TOKEN:
        log.error("HUBSPOT_TOKEN is not set. Aborting (previous feed left intact).")
        sys.exit(2)
    try:
        companies = fetch_customer_companies()
        payload, _exceptions = build_last_seen_feed(companies)
        # Safety: never publish an empty feed over a good one.
        if not payload["lastSeen"]:
            log.error("Computed feed is empty — refusing to overwrite previous file.")
            sys.exit(3)
        # Prefer GitHub Pages publishing when configured; otherwise local file.
        if not publish_github(payload):
            publish_json(payload, OUTPUT_PATH)
        log.info("Done.")
    except Exception as e:  # noqa
        # On any failure, do NOT touch the existing published file.
        log.exception("Nightly feed FAILED — previous file left in place: %s", e)
        sys.exit(1)


if __name__ == "__main__":
    main()
