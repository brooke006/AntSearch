import json

import pytest

pytestmark = pytest.mark.science

# numpy and requests come with the science extra, so they're imported only once lightkurve is found.
lk = pytest.importorskip("lightkurve")
import numpy as np
import requests

from antsearch import pipeline
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


class FakeSearchResult:
    """Stands in for lightkurve's SearchResult: one row, with download() scripted per call."""

    def __init__(self, downloads, row=None):
        self.downloads = downloads
        self.table = [row or {"obs_collection": "TESS", "obs_id": "obs-1", "productFilename": "lc.fits"}]
        self.mission = ["TESS Sector 02"]

    def __len__(self):
        return len(self.table)

    def __getitem__(self, index):
        return self

    def download(self, **kwargs):
        outcome = self.downloads.pop(0)
        if isinstance(outcome, Exception):
            raise outcome
        return outcome


@pytest.fixture
def no_sleep(monkeypatch):
    waits = []
    monkeypatch.setattr(pipeline.time, "sleep", waits.append)
    return waits


def fake_search(monkeypatch, *results):
    """Make lk.search_lightcurve return (or raise) each of results in turn."""
    calls = list(results)

    def search(*args, **kwargs):
        outcome = calls.pop(0)
        if isinstance(outcome, Exception):
            raise outcome
        return outcome

    monkeypatch.setattr(lk, "search_lightcurve", search)


def test_download_retries_network_errors_with_backoff(monkeypatch, tmp_path, no_sleep):
    lc = synthetic_light_curve()
    fake_search(monkeypatch, requests.ConnectionError("MAST down"), FakeSearchResult([TimeoutError("slow"), lc]),
                FakeSearchResult([lc]))

    assert pipeline.download_light_curve("WASP-18", download_dir=tmp_path, backoff=1) is lc
    assert no_sleep == [1, 2]


def test_download_gives_up_after_attempts(monkeypatch, tmp_path, no_sleep):
    fake_search(monkeypatch, *[requests.ConnectionError("MAST down")] * 3)

    with pytest.raises(pipeline.DownloadError, match="after 3 attempts"):
        pipeline.download_light_curve("WASP-18", download_dir=tmp_path)
    assert len(no_sleep) == 2


def test_download_deletes_corrupt_cached_file_and_retries(monkeypatch, tmp_path, no_sleep):
    from lightkurve.utils import LightkurveError

    lc = synthetic_light_curve()
    result = FakeSearchResult([LightkurveError("This file may be corrupt"), lc])
    cached = pipeline.cache_path(result.table[0], tmp_path)
    cached.parent.mkdir(parents=True)
    cached.write_bytes(b"truncated")
    fake_search(monkeypatch, result, result)

    assert pipeline.download_light_curve("WASP-18", download_dir=tmp_path) is lc
    assert not cached.exists()


def test_empty_search_is_not_retried(monkeypatch, tmp_path, no_sleep):
    empty = FakeSearchResult([])
    empty.table = []
    fake_search(monkeypatch, empty)

    with pytest.raises(pipeline.NoDataError, match="no SPOC 2-minute"):
        pipeline.download_light_curve("WASP-18", sector=99, download_dir=tmp_path)
    assert no_sleep == []


def test_unresolvable_target_is_not_retried(monkeypatch, tmp_path, no_sleep):
    from lightkurve.search import SearchError

    fake_search(monkeypatch, SearchError("could not resolve 'Not A Star'"))

    with pytest.raises(pipeline.NoDataError, match="could not resolve"):
        pipeline.download_light_curve("Not A Star", download_dir=tmp_path)
    assert no_sleep == []


def test_second_download_reads_the_cache_without_searching_mast(monkeypatch, tmp_path, no_sleep):
    lc = synthetic_light_curve()
    result = FakeSearchResult([lc])
    cached = pipeline.cache_path(result.table[0], tmp_path)
    reads = []
    monkeypatch.setattr(lk, "read", lambda path, **kwargs: reads.append(path) or lc)
    fake_search(monkeypatch, result)  # a second search would fail: only one is scripted

    pipeline.download_light_curve("TOI-102", download_dir=tmp_path)

    assert pipeline.download_light_curve("TOI-102", download_dir=tmp_path) is lc
    assert reads == [cached]


def test_cache_is_kept_per_target_and_sector(tmp_path):
    keys = {
        pipeline.index_path("TOI-102", None, "SPOC", tmp_path),
        pipeline.index_path("TOI-102", 2, "SPOC", tmp_path),
        pipeline.index_path("TOI-103", None, "SPOC", tmp_path),
    }

    assert len(keys) == 3
    assert pipeline.index_path("Pi Men", 1, "SPOC", tmp_path).name == "pi_men_spoc_sector-1.txt"


def test_unreadable_cached_file_is_downloaded_again(monkeypatch, tmp_path, no_sleep):
    lc = synthetic_light_curve()
    result = FakeSearchResult([lc])
    cached = pipeline.cache_path(result.table[0], tmp_path)
    cached.parent.mkdir(parents=True)
    cached.write_bytes(b"truncated")
    index = pipeline.index_path("TOI-102", None, "SPOC", tmp_path)
    pipeline.write_index(index, cached.relative_to(tmp_path))
    fake_search(monkeypatch, result)

    assert pipeline.download_light_curve("TOI-102", download_dir=tmp_path) is lc
    assert index.exists()


@pytest.mark.parametrize(
    "error, code",
    [(pipeline.NoDataError("none"), pipeline.EXIT_NO_DATA), (pipeline.DownloadError("down"), pipeline.EXIT_DOWNLOAD_FAILED)],
)
def test_main_exit_code_separates_download_failures(monkeypatch, tmp_path, error, code):
    def fail(*args, **kwargs):
        raise error

    monkeypatch.setattr(pipeline, "download_light_curve", fail)

    assert pipeline.main(["--out", str(tmp_path / "fold.png")]) == code


@pytest.mark.parametrize("known, code", [(PERIOD, pipeline.EXIT_OK), (PERIOD * 1.5, pipeline.EXIT_PERIOD_MISMATCH)])
def test_main_exit_code_reports_period_check(monkeypatch, tmp_path, known, code):
    monkeypatch.setattr(pipeline, "download_light_curve", lambda *args, **kwargs: synthetic_light_curve())
    argv = ["--known-period", str(known), "--min-period", "1", "--max-period", "10", "--out", str(tmp_path / "f.png")]

    assert pipeline.main(argv) == code
    assert (tmp_path / "f.png").exists()


def test_run_task_returns_json_ready_signal_and_timings(monkeypatch):
    lc = synthetic_light_curve()
    lc.meta["SECTOR"] = 2
    monkeypatch.setattr(pipeline, "download_light_curve", lambda star_id: lc)

    result = pipeline.run_task("TOI-102", min_period=1, max_period=10)

    assert json.loads(json.dumps(result)) == result
    assert (result["star_id"], result["sector"]) == ("TOI-102", 2)
    assert period_error(result["period"], PERIOD) < 0.01
    assert result["depth"] == pytest.approx(DEPTH, rel=0.1)
    assert result["power"] > 0
    assert result["download_seconds"] >= 0 and result["compute_seconds"] > 0
