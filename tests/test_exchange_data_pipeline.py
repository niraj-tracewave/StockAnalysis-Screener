import pytest

from app.core.exchange_data_pipeline import (
    ExchangeTarget,
    PermanentExchangeError,
    parse_bse_quote,
    parse_nse_quote,
)


def test_parse_nse_quote_maps_existing_database_fields():
    target = ExchangeTarget(7, "nse", "RELIANCE", "EQ")
    result = parse_nse_quote(
        target,
        {"activeSeries": ["EQ"], "marketType": "N"},
        {
            "equityResponse": [
                {
                    "metaData": {"dayHigh": 1200.5, "dayLow": 1150.25},
                    "tradeInfo": {
                        "lastPrice": 1175.5,
                        "faceValue": 10,
                        "totalMarketCap": 15_801_945_465_679.4,
                    },
                    "secInfo": {
                        "pdSymbolPe": "17.92",
                        "macro": "Energy",
                        "sector": "Oil Gas",
                        "industryInfo": "Petroleum",
                        "basicIndustry": "Refineries",
                    },
                }
            ]
        },
    )

    assert result.current_price == 1175.5
    assert result.market_cap == 1_580_194.55
    assert result.pe_ratio == 17.92
    assert result.macro_sector == "Energy"


def test_parse_bse_quote_accepts_comma_formatted_values():
    target = ExchangeTarget(8, "bse", "500325")
    result = parse_bse_quote(
        target,
        {"FaceVal": "10", "PE": "22.5", "ROE": "14.2"},
        {"Header": {"LTP": "1,175.50", "High": "1,200", "Low": "1,150"}},
        {"MktCapFull": "15,80,194.55"},
    )

    assert result.current_price == 1175.5
    assert result.high_price == 1200
    assert result.low_price == 1150
    assert result.market_cap == 1580194.55
    assert result.roe == 14.2


def test_exchange_parsers_reject_missing_prices():
    target = ExchangeTarget(9, "bse", "500000")
    with pytest.raises(PermanentExchangeError, match="no usable price"):
        parse_bse_quote(target, {}, {"Header": {}}, {})


def test_exchange_parsers_reject_invalid_ranges():
    target = ExchangeTarget(10, "nse", "BROKEN")
    with pytest.raises(ValueError, match="high/low"):
        parse_nse_quote(
            target,
            {},
            {
                "equityResponse": [
                    {
                        "metaData": {"dayHigh": 90, "dayLow": 110},
                        "tradeInfo": {"lastPrice": 100},
                        "secInfo": {},
                    }
                ]
            },
        )
