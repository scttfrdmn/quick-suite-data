"""
Unit tests for research source Lambda handlers:
  - lambdas/ipeds-search/handler.py    (TestIpedsSearch)
  - lambdas/nih-reporter-search/handler.py  (TestNihReporterSearch)
  - lambdas/nsf-awards-search/handler.py    (TestNsfAwardsSearch)
  - lambdas/federated-search/handler.py     (TestFederatedSearchResearchSources)

All external HTTP calls are mocked via unittest.mock.patch on urllib.request.urlopen.
"""

import importlib
import importlib.util
import json
import os
import sys
from io import BytesIO
from unittest.mock import MagicMock, patch

REPO_ROOT = os.path.join(os.path.dirname(__file__), "..")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_urlopen_response(body):
    """Create a mock urllib response context manager for a JSON body."""
    encoded = json.dumps(body).encode("utf-8")
    mock_resp = MagicMock()
    mock_resp.read.return_value = encoded
    mock_resp.__enter__ = lambda s: s
    mock_resp.__exit__ = MagicMock(return_value=False)
    return mock_resp


def _load_handler(lambda_dir: str, alias: str, env: dict | None = None):
    path = os.path.join(REPO_ROOT, "lambdas", lambda_dir, "handler.py")
    spec = importlib.util.spec_from_file_location(alias, path)
    mod = importlib.util.module_from_spec(spec)
    sys.modules[alias] = mod
    with patch.dict(os.environ, env or {}):
        spec.loader.exec_module(mod)
    return mod


_ipeds = _load_handler("ipeds-search", "_ipeds_handler")
_nih = _load_handler("nih-reporter-search", "_nih_handler")
_nsf = _load_handler("nsf-awards-search", "_nsf_handler")


def _mock_ctx():
    ctx = MagicMock()
    ctx.client_context.custom = {"bedrockAgentCoreToolName": "target___ipeds_search"}
    return ctx


# ---------------------------------------------------------------------------
# IPEDS
# ---------------------------------------------------------------------------

_IPEDS_API_RESPONSE = {
    "results": [
        {"varTitle": "Graduation Rate 4-Year", "categoryLabel": "Graduation Rates", "definition": "Percentage of full-time students graduating within 4 years"},
        {"varTitle": "Enrollment Headcount", "categoryLabel": "Enrollment", "definition": "Total enrollment headcount for fall term"},
        {"varTitle": "Tuition Revenue", "categoryLabel": "Finance", "definition": "Total tuition revenue reported"},
    ]
}


class TestIpedsSearch:
    def test_happy_path_returns_results(self):
        with patch("urllib.request.urlopen", return_value=_make_urlopen_response(_IPEDS_API_RESPONSE)):
            result = _ipeds.handler({"query": "graduation rate"}, _mock_ctx())
        assert result["source_type"] == "ipeds"
        assert result["count"] > 0
        assert result["results"][0]["source_type"] == "ipeds"
        assert "match_score" in result["results"][0]

    def test_empty_query_returns_error(self):
        result = _ipeds.handler({}, _mock_ctx())
        assert "error" in result
        assert "query" in result["error"]

    def test_max_results_capped_at_50(self):
        big_response = {"results": [
            {"varTitle": f"Var{i}", "definition": "enrollment graduation rate"} for i in range(60)
        ]}
        with patch("urllib.request.urlopen", return_value=_make_urlopen_response(big_response)):
            result = _ipeds.handler({"query": "enrollment", "max_results": 100}, _mock_ctx())
        assert result["count"] <= 50

    def test_empty_api_response_returns_empty_results(self):
        with patch("urllib.request.urlopen", return_value=_make_urlopen_response({"results": []})):
            result = _ipeds.handler({"query": "nonexistent topic xyz"}, _mock_ctx())
        assert result["count"] == 0
        assert result["results"] == []

    def test_invalid_survey_returns_error(self):
        result = _ipeds.handler({"query": "test", "survey": "invalid_survey"}, _mock_ctx())
        assert "error" in result

    def test_valid_survey_accepted(self):
        with patch("urllib.request.urlopen", return_value=_make_urlopen_response(_IPEDS_API_RESPONSE)):
            result = _ipeds.handler({"query": "graduation", "survey": "graduation_rates"}, _mock_ctx())
        assert "error" not in result
        assert result["source_type"] == "ipeds"

    def test_api_error_returns_empty_results(self):
        import urllib.error
        with patch("urllib.request.urlopen", side_effect=urllib.error.HTTPError(
            url="https://educationdata.urban.org", code=500, msg="Server Error", hdrs={}, fp=None
        )):
            result = _ipeds.handler({"query": "graduation"}, _mock_ctx())
        assert "error" not in result
        assert result["count"] == 0


# ---------------------------------------------------------------------------
# NIH Reporter
# ---------------------------------------------------------------------------

_NIH_RESPONSE = {
    "results": [
        {
            "ProjectNum": "R01CA123456",
            "ProjectTitle": "Machine Learning for Cancer Detection",
            "PiNames": [{"first_name": "Jane", "last_name": "Smith"}],
            "FiscalYear": 2023,
            "AwardAmount": 500000,
            "AbstractText": "This project develops machine learning algorithms for early cancer detection using imaging data.",
        },
        {
            "ProjectNum": "R01GM654321",
            "ProjectTitle": "Genomic Analysis of Rare Diseases",
            "PiNames": [{"first_name": "John", "last_name": "Doe"}],
            "FiscalYear": 2022,
            "AwardAmount": 350000,
            "AbstractText": "Genome-wide association study for rare genetic diseases affecting pediatric populations.",
        },
    ]
}


class TestNihReporterSearch:
    def test_happy_path_returns_results(self):
        with patch("urllib.request.urlopen", return_value=_make_urlopen_response(_NIH_RESPONSE)):
            result = _nih.handler({"query": "machine learning cancer"}, _mock_ctx())
        assert result["source_type"] == "nih_reporter"
        assert result["count"] > 0
        r = result["results"][0]
        assert r["core_project_num"] == "R01CA123456"
        assert r["fiscal_year"] == 2023
        assert "pi_names" in r

    def test_empty_query_returns_error(self):
        result = _nih.handler({}, _mock_ctx())
        assert "error" in result

    def test_fiscal_year_filter_sent_in_request(self):
        captured = {}

        def capture_urlopen(req, timeout=None):
            body = json.loads(req.data.decode())
            captured["criteria"] = body["criteria"]
            return _make_urlopen_response({"results": []})

        with patch("urllib.request.urlopen", side_effect=capture_urlopen):
            _nih.handler({"query": "cancer", "fiscal_year": 2023}, _mock_ctx())
        assert 2023 in captured["criteria"]["fiscal_years"]

    def test_empty_results_returns_zero_count(self):
        with patch("urllib.request.urlopen", return_value=_make_urlopen_response({"results": []})):
            result = _nih.handler({"query": "zzz_nonexistent_topic"}, _mock_ctx())
        assert result["count"] == 0

    def test_invalid_fiscal_year_returns_error(self):
        result = _nih.handler({"query": "cancer", "fiscal_year": "not-a-year"}, _mock_ctx())
        assert "error" in result

    def test_max_results_capped_at_50(self):
        big_response = {"results": [
            {"ProjectNum": f"R{i}", "ProjectTitle": f"cancer study {i}", "AbstractText": "cancer research machine learning"}
            for i in range(60)
        ]}
        with patch("urllib.request.urlopen", return_value=_make_urlopen_response(big_response)):
            result = _nih.handler({"query": "cancer", "max_results": 100}, _mock_ctx())
        assert result["count"] <= 50

    def test_api_error_returns_empty_results(self):
        import urllib.error
        with patch("urllib.request.urlopen", side_effect=urllib.error.HTTPError(
            url="https://api.reporter.nih.gov", code=503, msg="Unavailable", hdrs={}, fp=None
        )):
            result = _nih.handler({"query": "cancer"}, _mock_ctx())
        assert "error" not in result
        assert result["count"] == 0

    def test_abstract_truncated_to_500_chars(self):
        long_abstract = "x" * 1000
        response = {"results": [{"ProjectNum": "R01", "ProjectTitle": "cancer study", "AbstractText": long_abstract}]}
        with patch("urllib.request.urlopen", return_value=_make_urlopen_response(response)):
            result = _nih.handler({"query": "cancer"}, _mock_ctx())
        # abstract_text in result should not exceed 500 chars
        for r in result["results"]:
            assert len(r.get("abstract_text", "")) <= 500


# ---------------------------------------------------------------------------
# NSF Awards
# ---------------------------------------------------------------------------

_NSF_RESPONSE = {
    "response": {
        "award": [
            {
                "id": "2301234",
                "title": "Deep Learning Methods for Climate Modeling",
                "piFirstName": "Alice",
                "piLastName": "Johnson",
                "awardeeName": "University of Example",
                "startDate": "09/01/2023",
                "expDate": "08/31/2026",
                "fundsObligatedAmt": 450000,
                "abstractText": "This award funds research into deep learning approaches for improved climate model accuracy and uncertainty quantification.",
            },
            {
                "id": "2305678",
                "title": "Quantum Computing for Chemistry Simulation",
                "piFirstName": "Bob",
                "piLastName": "Williams",
                "awardeeName": "State Research University",
                "startDate": "01/01/2024",
                "expDate": "12/31/2026",
                "fundsObligatedAmt": 800000,
                "abstractText": "Research into quantum algorithms for simulating molecular chemistry relevant to drug discovery.",
            },
        ]
    }
}


class TestNsfAwardsSearch:
    def test_happy_path_returns_results(self):
        with patch("urllib.request.urlopen", return_value=_make_urlopen_response(_NSF_RESPONSE)):
            result = _nsf.handler({"query": "deep learning climate"}, _mock_ctx())
        assert result["source_type"] == "nsf_awards"
        assert result["count"] > 0
        r = result["results"][0]
        assert r["award_id"] == "2301234"
        assert "awardee_name" in r
        assert "funds_obligated_amt" in r

    def test_empty_query_returns_error(self):
        result = _nsf.handler({}, _mock_ctx())
        assert "error" in result

    def test_pi_name_filter_sent_in_request(self):
        captured_url = {}

        def capture_urlopen(req, timeout=None):
            captured_url["url"] = req.get_full_url()
            return _make_urlopen_response({"response": {"award": []}})

        with patch("urllib.request.urlopen", side_effect=capture_urlopen):
            _nsf.handler({"query": "climate", "pi_name": "Johnson"}, _mock_ctx())
        assert "Johnson" in captured_url["url"]

    def test_empty_response_returns_zero_count(self):
        with patch("urllib.request.urlopen", return_value=_make_urlopen_response({"response": {"award": []}})):
            result = _nsf.handler({"query": "zzz_nonexistent"}, _mock_ctx())
        assert result["count"] == 0

    def test_max_results_capped_at_50(self):
        big_response = {"response": {"award": [
            {"id": str(i), "title": f"deep learning climate {i}", "abstractText": "deep learning climate modeling"} for i in range(60)
        ]}}
        with patch("urllib.request.urlopen", return_value=_make_urlopen_response(big_response)):
            result = _nsf.handler({"query": "climate", "max_results": 100}, _mock_ctx())
        assert result["count"] <= 50

    def test_api_error_returns_empty_results(self):
        import urllib.error
        with patch("urllib.request.urlopen", side_effect=urllib.error.HTTPError(
            url="https://api.nsf.gov", code=500, msg="Internal Error", hdrs={}, fp=None
        )):
            result = _nsf.handler({"query": "climate"}, _mock_ctx())
        assert "error" not in result
        assert result["count"] == 0


# ---------------------------------------------------------------------------
# Federated search dispatch for research sources
# ---------------------------------------------------------------------------

def _load_federated():
    path = os.path.join(REPO_ROOT, "lambdas", "federated-search", "handler.py")
    alias = "_fed_handler_research"
    spec = importlib.util.spec_from_file_location(alias, path)
    mod = importlib.util.module_from_spec(spec)
    sys.modules[alias] = mod
    with patch.dict(os.environ, {"REGISTRY_TABLE": "qs-data-source-registry", "CATALOG_TABLE": "qs-catalog"}):
        spec.loader.exec_module(mod)
    return mod


_fed = _load_federated()


class TestFederatedSearchResearchSources:
    def test_ipeds_source_dispatches_to_search_ipeds(self):
        ipeds_source = {
            "source_id": "ipeds-main",
            "type": "ipeds",
            "display_name": "IPEDS Higher Ed Data",
            "description": "IPEDS institutional data",
            "data_classification": "public",
        }
        mock_reg = MagicMock()
        mock_reg.scan.return_value = {"Items": [ipeds_source]}

        with patch.object(_fed, "dynamodb") as mock_ddb, \
             patch.object(_fed, "_search_ipeds", return_value=[{
                 "source_id": "ipeds-main", "source_type": "ipeds",
                 "display_name": "Graduation Rate", "match_score": 0.8,
                 "description": "graduation rate data", "quality_score": None,
             }]) as mock_fn:
            mock_ddb.Table.return_value = mock_reg
            ctx = MagicMock()
            ctx.client_context.custom = {"bedrockAgentCoreToolName": "t___federated_search"}
            result = _fed.handler({"query": "graduation rate", "caller_clearance": "public"}, ctx)

        mock_fn.assert_called_once()
        assert result["total"] == 1
        assert result["results"][0]["source_type"] == "ipeds"

    def test_nih_reporter_source_dispatches_to_search_nih_reporter(self):
        nih_source = {
            "source_id": "nih-reporter",
            "type": "nih_reporter",
            "display_name": "NIH Reporter",
            "description": "NIH funded grants",
            "data_classification": "public",
        }
        mock_reg = MagicMock()
        mock_reg.scan.return_value = {"Items": [nih_source]}

        with patch.object(_fed, "dynamodb") as mock_ddb, \
             patch.object(_fed, "_search_nih_reporter", return_value=[{
                 "source_id": "nih-reporter", "source_type": "nih_reporter",
                 "display_name": "Cancer grant", "match_score": 0.9,
                 "description": "cancer research", "quality_score": None,
             }]) as mock_fn:
            mock_ddb.Table.return_value = mock_reg
            ctx = MagicMock()
            ctx.client_context.custom = {"bedrockAgentCoreToolName": "t___federated_search"}
            result = _fed.handler({"query": "cancer research"}, ctx)

        mock_fn.assert_called_once()
        assert result["total"] == 1
        assert result["results"][0]["source_type"] == "nih_reporter"

    def test_nsf_awards_source_dispatches_to_search_nsf_awards(self):
        nsf_source = {
            "source_id": "nsf-awards",
            "type": "nsf_awards",
            "display_name": "NSF Awards",
            "description": "NSF-funded research awards",
            "data_classification": "public",
        }
        mock_reg = MagicMock()
        mock_reg.scan.return_value = {"Items": [nsf_source]}

        with patch.object(_fed, "dynamodb") as mock_ddb, \
             patch.object(_fed, "_search_nsf_awards", return_value=[{
                 "source_id": "nsf-awards", "source_type": "nsf_awards",
                 "display_name": "Climate ML Award", "match_score": 1.0,
                 "description": "deep learning climate", "quality_score": None,
             }]) as mock_fn:
            mock_ddb.Table.return_value = mock_reg
            ctx = MagicMock()
            ctx.client_context.custom = {"bedrockAgentCoreToolName": "t___federated_search"}
            result = _fed.handler({"query": "machine learning"}, ctx)

        mock_fn.assert_called_once()
        assert result["total"] == 1
        assert result["results"][0]["source_type"] == "nsf_awards"

    def test_unknown_source_type_skipped_without_error(self):
        unknown_source = {
            "source_id": "unknown-src",
            "type": "unknown_api",
            "display_name": "Unknown",
            "description": "some unknown data source",
            "data_classification": "public",
        }
        mock_reg = MagicMock()
        mock_reg.scan.return_value = {"Items": [unknown_source]}

        with patch.object(_fed, "dynamodb") as mock_ddb:
            mock_ddb.Table.return_value = mock_reg
            ctx = MagicMock()
            ctx.client_context.custom = {"bedrockAgentCoreToolName": "t___federated_search"}
            result = _fed.handler({"query": "anything"}, ctx)

        assert result["total"] == 0
        assert result["skipped_sources"] == []  # unknown types are silently skipped (no dispatch fn)
