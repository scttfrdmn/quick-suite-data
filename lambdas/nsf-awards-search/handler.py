"""
nsf_awards_search: Search NSF awards via NSF Award Search API.

AgentCore Lambda target — invoked directly by the Gateway.
Event dict contains tool arguments. Returns a plain dict.

Public API — no authentication required.
Uses urllib.request + stdlib only. GET /services/v1/awards.json.
"""

import json
import logging
import urllib.error
import urllib.parse
import urllib.request
from typing import Any

logger = logging.getLogger()
logger.setLevel(logging.INFO)

_BASE_URL = "https://api.nsf.gov/services/v1/awards.json"
_MAX_RESULTS = 50
_PRINT_FIELDS = "id,title,piFirstName,piLastName,awardeeName,startDate,expDate,fundsObligatedAmt,abstractText"


def _search_nsf_awards(
    query: str,
    date_start: str | None,
    date_end: str | None,
    pi_name: str | None,
    max_results: int,
) -> list:
    """GET from NSF Award Search API and return normalized result dicts."""
    params: dict = {
        "keyword": query,
        "printFields": _PRINT_FIELDS,
        "rows": min(max_results, _MAX_RESULTS),
    }
    if date_start:
        params["dateStart"] = date_start
    if date_end:
        params["dateEnd"] = date_end
    if pi_name:
        params["PILastName"] = pi_name

    url = _BASE_URL + "?" + urllib.parse.urlencode(params)

    try:
        req = urllib.request.Request(
            url,
            headers={"Accept": "application/json", "User-Agent": "quick-suite-data/1.0"},
        )
        with urllib.request.urlopen(req, timeout=20) as resp:
            data = json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as e:
        logger.warning(json.dumps({"nsf_http_error": e.code, "url": url}))
        return []
    except Exception as e:
        logger.warning(json.dumps({"nsf_error": str(e)}))
        return []

    items = data.get("response", {}).get("award", [])[:max_results]
    query_words = [w.lower() for w in query.split() if w]

    results = []
    for item in items:
        award_id = item.get("id") or ""
        title = item.get("title") or ""
        pi_first = item.get("piFirstName") or ""
        pi_last = item.get("piLastName") or ""
        pi_full = " ".join(filter(None, [pi_first, pi_last])).strip()
        awardee = item.get("awardeeName") or ""
        start_date = item.get("startDate") or ""
        exp_date = item.get("expDate") or ""
        funds = item.get("fundsObligatedAmt") or 0
        abstract = (item.get("abstractText") or "")[:500]

        text = (title + " " + abstract + " " + awardee).lower()
        if query_words:
            matches = sum(1 for w in query_words if w in text)
            score = min(matches / len(query_words), 1.0)
        else:
            score = 0.5

        results.append({
            "source_id": f"nsf/{award_id or title[:40]}",
            "source_type": "nsf_awards",
            "display_name": title,
            "award_id": award_id,
            "pi_name": pi_full,
            "awardee_name": awardee,
            "start_date": start_date,
            "exp_date": exp_date,
            "funds_obligated_amt": funds,
            "abstract_text": abstract,
            "match_score": score,
            "quality_score": None,
        })

    results.sort(key=lambda r: r["match_score"], reverse=True)
    return results


def handler(event: dict, context: Any) -> dict:
    """
    Search NSF awards via NSF Award Search API.

    Tool arguments:
    - query: str (required) — keyword search
    - date_start: str (optional) — start date filter (MM/DD/YYYY)
    - date_end: str (optional) — end date filter (MM/DD/YYYY)
    - pi_name: str (optional) — principal investigator last name filter
    - max_results: int (optional, default 20, max 50)
    """
    _tool_name = "unknown"
    try:
        raw = context.client_context.custom["bedrockAgentCoreToolName"]
        _tool_name = raw.split("___")[-1]
    except Exception:
        pass
    logger.info(json.dumps({"tool": _tool_name, "event": event}))

    query = (event.get("query") or "").strip()
    if not query:
        return {"error": "query is required"}

    date_start = (event.get("date_start") or "").strip() or None
    date_end = (event.get("date_end") or "").strip() or None
    pi_name = (event.get("pi_name") or "").strip() or None

    try:
        max_results = int(event.get("max_results", 20))
    except (TypeError, ValueError):
        max_results = 20
    max_results = min(max(1, max_results), _MAX_RESULTS)

    results = _search_nsf_awards(query, date_start, date_end, pi_name, max_results)

    return {
        "source_type": "nsf_awards",
        "query": query,
        "results": results,
        "count": len(results),
    }
