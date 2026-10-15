import numpy as np
import pytest

lk = pytest.importorskip("lightkurve")

from antsearch.pipeline import BLSResult, flatten, period_error, plot_folded, run_bls, transit_mask

PERIOD = 3.2  # days
T0 = 1.1
DURATION = 0.12
DEPTH = 0.005


def synthetic_light_curve(seed=0):
    """27 days of 2-minute cadence: box transits on a slow sinusoidal trend, plus white noise."""
    rng = np.random.default_rng(seed)
    time = np.arange(0, 27, 2 / 60 / 24)
    phase = (time - T0 + 0.5 * PERIOD) % PERIOD - 0.5 * PERIOD
    flux = 1 + 0.01 * np.sin(2 * np.pi * time / 9) + rng.normal(0, 0.001, time.size)
    flux[np.abs(phase) < DURATION / 2] -= DEPTH
    return lk.LightCurve(time=time, flux=flux, flux_err=np.full(time.size, 0.001))


def test_bls_recovers_period_of_flattened_synthetic_transit():
    lc = flatten(synthetic_light_curve())
    result = run_bls(lc.time.value, lc.flux.value, min_period=1, max_period=10)

    assert period_error(result.period, PERIOD) < 0.01


def test_masked_second_pass_restores_the_depth():
    # An unmasked flatten eats much of the dip, so only the masked pass is checked for depth.
    raw = synthetic_light_curve().remove_nans()
    first = run_bls(*_arrays(flatten(raw)), min_period=1, max_period=10)
    lc = flatten(raw, mask=transit_mask(raw.time.value, first))
    second = run_bls(*_arrays(lc), min_period=1, max_period=10)

    assert period_error(second.period, PERIOD) < 0.01
    assert second.depth == pytest.approx(DEPTH, rel=0.1)


def test_transit_mask_flags_only_in_transit_cadences():
    result = BLSResult(period=PERIOD, t0=T0, duration=DURATION, depth=DEPTH, power=1.0)
    time = np.array([T0, T0 + PERIOD, T0 + 0.05, T0 + 0.5, T0 + PERIOD / 2])

    assert transit_mask(time, result).tolist() == [True, True, True, False, False]


def test_plot_folded_writes_png(tmp_path):
    lc = flatten(synthetic_light_curve())
    result = run_bls(lc.time.value, lc.flux.value, min_period=1, max_period=10)

    path = plot_folded(lc.time.value, lc.flux.value, result, tmp_path / "plots" / "fold.png")

    assert path.read_bytes().startswith(b"\x89PNG")


def _arrays(lc):
    return lc.time.value, lc.flux.value
