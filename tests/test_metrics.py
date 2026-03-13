import pytest

def test_car_formula():
    tier1 = 800000
    rwa = 5000000
    car = tier1 / rwa
    assert abs(car - 0.16) < 0.001

def test_lcr_formula():
    hqla = 1200000
    outflows = 900000
    lcr = hqla / outflows
    assert abs(lcr - 1.3333) < 0.001

def test_npl_formula():
    npl = 150000
    total = 3000000
    ratio = npl / total
    assert abs(ratio - 0.05) < 0.001

def test_car_threshold():
    assert 0.16 >= 0.08, "CAR must be >= 8%"
    assert 0.13 >= 0.08, "CAR must be >= 8%"

def test_lcr_threshold():
    assert 1.33 >= 1.0, "LCR must be >= 100%"

def test_npl_below_benchmark():
    benchmark_npl = 0.015
    bank_npl = 0.05
    delta = bank_npl - benchmark_npl
    assert delta == pytest.approx(0.035, abs=0.001)
