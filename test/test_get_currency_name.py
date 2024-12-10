import pytest, re

from src.processing_lambda import get_currency_name


def test_get_currency_name_returns_names_of_ISO_4217_currencies():
    GBP = get_currency_name("GBP")
    assert isinstance(GBP, str)
    assert "pound" in GBP.lower()
    assert "sterling" in GBP.lower()

    LYD = get_currency_name("LYD")
    assert isinstance(LYD, str)
    assert "libya" in LYD.lower()

    USD = get_currency_name("USD") 
    assert isinstance(USD, str)
    assert re.search(r'(?i)(u\.? ?s\.?)|(united states)', USD.lower())
    assert "dollar" in USD.lower()

    KES = get_currency_name("KES")
    assert isinstance(KES, str)
    assert "kenya" in KES.lower()
    assert "shilling" in KES.lower()

    EUR = get_currency_name("EUR")
    assert isinstance(EUR, str)
    assert "euro" in EUR.lower()

    HKD = get_currency_name("HKD")
    assert isinstance(HKD, str)
    assert "hong kong" in HKD.lower()
    assert "dollar" in HKD.lower()


def test_get_currency_name_raises_ValueError_for_unrecognised_currency():
    with pytest.raises(ValueError):
        get_currency_name("BLP")
    
    with pytest.raises(ValueError):
        get_currency_name("DJANGO REINHARDT")
    
    with pytest.raises(ValueError):
        get_currency_name("OOP")

    with pytest.raises(ValueError):
        get_currency_name("NCL")

    with pytest.raises(ValueError):
        get_currency_name("MCR")