import asyncio

import pytest

from app.core.market_data_pipeline import (
    PermanentProviderError,
    StockTarget,
    YahooChartClient,
    _split_nse_symbol,
    parse_yahoo_chart,
)


def test_split_nse_symbol_accepts_equity_series_only():
    assert _split_nse_symbol("RELIANCE-EQ") == ("RELIANCE", "EQ")
    assert _split_nse_symbol("M&M-EQ") == ("M&M", "EQ")
    assert _split_nse_symbol("794TS36-SG") is None


def test_parse_yahoo_chart_builds_existing_chart_shape():
    result = parse_yahoo_chart(
        7,
        "TEST.NS",
        {
            "chart": {
                "error": None,
                "result": [
                    {
                        "meta": {"regularMarketPrice": 102.5},
                        "timestamp": [1_700_000_000, 1_700_086_400],
                        "indicators": {
                            "quote": [
                                {
                                    "close": [100.25, 102.5],
                                    "high": [101.0, 103.0],
                                    "low": [99.0, 100.0],
                                    "volume": [1_000, 2_000],
                                }
                            ]
                        },
                    }
                ],
            }
        },
    )

    assert result.current_price == 102.5
    assert result.day_high == 103.0
    assert result.day_low == 100.0
    assert result.history[-1] == [1_700_086_400_000, 102.5, "", None, None, 2_000]


def test_parse_yahoo_chart_rejects_invalid_market_ranges():
    with pytest.raises(ValueError, match="high/low"):
        parse_yahoo_chart(
            7,
            "BROKEN.NS",
            {
                "chart": {
                    "error": None,
                    "result": [{
                        "meta": {"regularMarketPrice": 100, "regularMarketDayHigh": 90, "regularMarketDayLow": 110},
                        "timestamp": [1_700_000_000],
                        "indicators": {"quote": [{"close": [100], "volume": [10]}]},
                    }],
                }
            },
        )


def test_parse_yahoo_chart_quarantines_payload_without_a_price():
    with pytest.raises(PermanentProviderError, match="no usable price"):
        parse_yahoo_chart(
            8,
            "NO_PRICE.BO",
            {
                "chart": {
                    "error": None,
                    "result": [{
                        "meta": {},
                        "timestamp": [],
                        "indicators": {"quote": [{}]},
                    }],
                }
            },
        )


def test_fetch_many_separates_permanent_provider_failures(monkeypatch):
    client = YahooChartClient()

    async def fail_permanently(target, *, full_history):
        raise PermanentProviderError(target.yahoo_symbol)

    monkeypatch.setattr(client, "fetch", fail_permanently)
    completed, failed, permanent = asyncio.run(
        client.fetch_many(
            [StockTarget(company_id=1, yahoo_symbol="NOT_ON_YAHOO.BO")],
            full_history=False,
        )
    )

    assert completed == []
    assert failed == []
    assert permanent == ["NOT_ON_YAHOO.BO"]
