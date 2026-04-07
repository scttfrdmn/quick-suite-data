"""
ipeds_search: Search IPEDS data via Urban Institute Education Data Portal API.

AgentCore Lambda target — invoked directly by the Gateway.
Event dict contains tool arguments. Returns a plain dict.

Public API — no authentication required.
Uses urllib.request + stdlib only.
"""

import json
import logging
import urllib.error
import urllib.parse
import urllib.request
from typing import Any

logger = logging.getLogger()
logger.setLevel(logging.INFO)

_BASE_URL = "https://educationdata.urban.org/api/v1/"

_SURVEY_ENDPOINTS = {
    "graduation_rates": "college-university/ipeds/graduation-rates/",
    "enrollment": "college-university/ipeds/enrollment-full-time-equivalent/",
    "retention": "college-university/ipeds/institutional-characteristics/",
    "finance": "college-university/ipeds/finance/",
}

_MAX_RESULTS = 50


def _search_ipeds(query: str, survey: str | None, max_results: int) -> list:
    """
    Call the Education Data Portal API and return matched result dicts.

    The API returns data at the series/variable level. We query the
    `/api/v1/` endpoint with a keyword search via the `keyword` param
    where supported, otherwise use the variables listing.
    """
    query_words = [w.lower() for w in query.split() if w]

    # Use variables endpoint for keyword search — returns series metadata
    params = {
        "keyword": query,
        "page[size]": min(max_results, _MAX_RESULTS),
    }
    if survey and survey in _SURVEY_ENDPOINTS:
        endpoint = _BASE_URL + _SURVEY_ENDPOINTS[survey] + "variables/"
    else:
        endpoint = _BASE_URL + "college-university/ipeds/variables/"

    url = endpoint + "?" + urllib.parse.urlencode(params)

    try:
        req = urllib.request.Request(
            url,
            headers={"Accept": "application/json", "User-Agent": "quick-suite-data/1.0"},
        )
        with urllib.request.urlopen(req, timeout=20) as resp:
            data = json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as e:
        logger.warning(json.dumps({"ipeds_http_error": e.code, "url": url}))
        return []
    except Exception as e:
        logger.warning(json.dumps({"ipeds_error": str(e)}))
        return []

    results = []
    items = data if isinstance(data, list) else data.get("results", data.get("data", []))
    for item in items[:max_results]:
        var_name = item.get("varTitle") or item.get("varname") or item.get("label") or ""
        series = item.get("categoryLabel") or item.get("category") or ""
        year_start = item.get("surveyYear") or item.get("year") or ""
        description = item.get("definition") or item.get("description") or ""

        # Score by keyword match against name + description
        text = (var_name + " " + series + " " + description).lower()
        if query_words:
            matches = sum(1 for w in query_words if w in text)
            score = min(matches / len(query_words), 1.0)
        else:
            score = 0.5

        results.append({
            "source_id": f"ipeds/{var_name or series}",
            "source_type": "ipeds",
            "display_name": var_name or series or "IPEDS variable",
            "series_slug": series,
            "year_range": str(year_start) if year_start else "",
            "description": description[:300],
            "match_score": score,
            "quality_score": None,
        })

    results.sort(key=lambda r: r["match_score"], reverse=True)
    return results


def handler(event: dict, context: Any) -> dict:
    """
    Search IPEDS data via Urban Institute Education Data Portal API.

    Tool arguments:
    - query: str (required) — keyword search
    - survey: str (optional) — one of: graduation_rates, enrollment, retention, finance
    - year_range: str (optional) — filter hint (e.g. "2020-2023"); informational only
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

    survey = (event.get("survey") or "").strip() or None
    if survey and survey not in _SURVEY_ENDPOINTS:
        return {"error": f"survey must be one of: {', '.join(sorted(_SURVEY_ENDPOINTS))}"}

    try:
        max_results = int(event.get("max_results", 20))
    except (TypeError, ValueError):
        max_results = 20
    max_results = min(max(1, max_results), _MAX_RESULTS)

    results = _search_ipeds(query, survey, max_results)

    return {
        "source_type": "ipeds",
        "query": query,
        "results": results,
        "count": len(results),
    }
