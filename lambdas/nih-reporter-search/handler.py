"""
nih_reporter_search: Search NIH funded research grants via NIH Reporter v2 API.

AgentCore Lambda target — invoked directly by the Gateway.
Event dict contains tool arguments. Returns a plain dict.

Public API — no authentication required.
Uses urllib.request + stdlib only. POST /v2/projects/search.
"""

import json
import logging
import urllib.error
import urllib.request
from typing import Any

logger = logging.getLogger()
logger.setLevel(logging.INFO)

_BASE_URL = "https://api.reporter.nih.gov/v2/projects/search"
_MAX_RESULTS = 50
_ABSTRACT_MAX_CHARS = 500


def _search_nih_reporter(
    query: str,
    fiscal_year: int | None,
    institution: str | None,
    pi_name: str | None,
    max_results: int,
) -> list:
    """POST to NIH Reporter v2 and return normalized result dicts."""
    criteria: dict = {
        "text_search": {
            "operator": "and",
            "search_field": "all",
            "terms": query,
        }
    }
    if fiscal_year:
        criteria["fiscal_years"] = [fiscal_year]
    if institution:
        criteria["org_names"] = [institution]
    if pi_name:
        criteria["pi_names"] = [{"any_name": pi_name}]

    body = {
        "criteria": criteria,
        "limit": min(max_results, _MAX_RESULTS),
        "offset": 0,
        "fields": [
            "ProjectNum", "ProjectTitle", "PiNames", "FiscalYear",
            "AwardAmount", "AbstractText", "Organization",
        ],
    }
    body_bytes = json.dumps(body).encode("utf-8")

    try:
        req = urllib.request.Request(
            _BASE_URL,
            data=body_bytes,
            method="POST",
            headers={
                "Content-Type": "application/json",
                "Accept": "application/json",
                "User-Agent": "quick-suite-data/1.0",
            },
        )
        with urllib.request.urlopen(req, timeout=20) as resp:
            data = json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as e:
        logger.warning(json.dumps({"nih_reporter_http_error": e.code}))
        return []
    except Exception as e:
        logger.warning(json.dumps({"nih_reporter_error": str(e)}))
        return []

    items = data.get("results", [])[:max_results]
    query_words = [w.lower() for w in query.split() if w]

    results = []
    for item in items:
        title = item.get("ProjectTitle") or item.get("project_title") or ""
        core_num = item.get("ProjectNum") or item.get("project_num") or ""
        pi_names_raw = item.get("PiNames") or item.get("pi_names") or []
        pi_list = []
        for pi in pi_names_raw:
            if isinstance(pi, dict):
                name = " ".join(filter(None, [
                    pi.get("first_name", ""), pi.get("last_name", "")
                ])).strip()
                if name:
                    pi_list.append(name)
            elif isinstance(pi, str):
                pi_list.append(pi)

        fy = item.get("FiscalYear") or item.get("fiscal_year") or ""
        award = item.get("AwardAmount") or item.get("award_amount") or 0
        abstract = (item.get("AbstractText") or item.get("abstract_text") or "")[:_ABSTRACT_MAX_CHARS]

        text = (title + " " + abstract).lower()
        if query_words:
            matches = sum(1 for w in query_words if w in text)
            score = min(matches / len(query_words), 1.0)
        else:
            score = 0.5

        results.append({
            "source_id": f"nih/{core_num or title[:40]}",
            "source_type": "nih_reporter",
            "display_name": title,
            "core_project_num": core_num,
            "pi_names": pi_list,
            "fiscal_year": fy,
            "award_amount": award,
            "abstract_text": abstract,
            "match_score": score,
            "quality_score": None,
        })

    results.sort(key=lambda r: r["match_score"], reverse=True)
    return results


def handler(event: dict, context: Any) -> dict:
    """
    Search NIH-funded research grants via NIH Reporter API v2.

    Tool arguments:
    - query: str (required) — keyword search terms
    - fiscal_year: int (optional) — filter to specific fiscal year
    - institution: str (optional) — filter by institution name
    - pi_name: str (optional) — filter by principal investigator name
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

    fiscal_year: int | None = None
    if event.get("fiscal_year") is not None:
        try:
            fiscal_year = int(event["fiscal_year"])
        except (TypeError, ValueError):
            return {"error": "fiscal_year must be an integer"}

    institution = (event.get("institution") or "").strip() or None
    pi_name = (event.get("pi_name") or "").strip() or None

    try:
        max_results = int(event.get("max_results", 20))
    except (TypeError, ValueError):
        max_results = 20
    max_results = min(max(1, max_results), _MAX_RESULTS)

    results = _search_nih_reporter(query, fiscal_year, institution, pi_name, max_results)

    return {
        "source_type": "nih_reporter",
        "query": query,
        "results": results,
        "count": len(results),
    }
