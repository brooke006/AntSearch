"""Single-star science spike: download a TESS light curve, flatten it, run BLS, plot the fold.

    python -m antsearch.pipeline                      # WASP-18 b, TESS sector 2
    python -m antsearch.pipeline --target "Pi Men" --known-period 6.2678 --sector 1

Exit codes:
    0  period recovered within --tolerance
    1  period recovered but outside --tolerance
    3  no matching data on MAST, or the target name can't be resolved (retrying won't help)
    4  download failed after every retry (MAST unreachable or kept failing; try again later)
(2 is argparse's code for bad command-line arguments.)
"""

import argparse
import logging
import sys
import time
from dataclasses import dataclass
from pathlib import Path

import numpy as np
from astropy.timeseries import BoxLeastSquares

log = logging.getLogger("antsearch.pipeline")

EXIT_OK = 0
EXIT_PERIOD_MISMATCH = 1
EXIT_NO_DATA = 3
EXIT_DOWNLOAD_FAILED = 4

# Trial transit durations in days (about 1 h to 6 h).
DEFAULT_DURATIONS = (0.04, 0.06, 0.08, 0.12, 0.16, 0.25)


@dataclass(frozen=True)
class BLSResult:
    period: float  # days
    t0: float  # mid-transit time, same units as the input time
    duration: float  # days
    depth: float  # fractional flux drop
    power: float


class NoDataError(LookupError):
    """MAST has no matching light curve, or can't resolve the target name. Retrying won't help."""


class DownloadError(RuntimeError):
    """MAST was unreachable or kept failing on every attempt. Worth trying again later."""


def download_light_curve(
    target: str,
    sector: int | None = None,
    author: str = "SPOC",
    attempts: int = 3,
    backoff: float = 2.0,
    download_dir: str | Path | None = None,
):
    """Fetch one 2-minute-cadence TESS light curve from MAST as a lightkurve LightCurve.

    Network and MAST errors are retried up to `attempts` times, waiting backoff, 2 x backoff, ...
    seconds in between. A cached file that fails to read (usually a download cut off on an earlier
    run) is deleted before the next attempt, so it gets downloaded again.
    """
    import lightkurve as lk
    from astroquery.exceptions import RemoteServiceError
    from lightkurve.search import SearchError
    from lightkurve.utils import LightkurveError

    download_dir = str(download_dir or lk.config.get_cache_dir())
    error = None
    for attempt in range(1, attempts + 1):
        cached = None
        try:
            results = lk.search_lightcurve(target, mission="TESS", author=author, exptime=120, sector=sector)
            if len(results) == 0:
                raise NoDataError(f"no {author} 2-minute TESS light curve found for {target!r} (sector={sector})")
            cached = cache_path(results.table[0], download_dir)
            log.info("downloading %s", results[0].mission[0])
            return results[0].download(quality_bitmask="default", download_dir=download_dir)
        except SearchError as exc:
            raise NoDataError(f"MAST could not resolve {target!r}: {exc}") from exc
        except (OSError, RemoteServiceError, LightkurveError) as exc:
            error = exc
            if cached is not None and cached.exists():
                log.warning("deleting cached file %s, which may be corrupt", cached)
                cached.unlink()
        if attempt < attempts:
            wait = backoff * 2 ** (attempt - 1)
            log.warning("download attempt %d/%d failed (%s); retrying in %g s", attempt, attempts, error, wait)
            time.sleep(wait)
    raise DownloadError(f"could not download {target!r} after {attempts} attempts: {error}") from error


def cache_path(row, download_dir: str | Path) -> Path:
    """Where lightkurve keeps the downloaded file for one search-result row."""
    return Path(download_dir, "mastDownload", str(row["obs_collection"]), str(row["obs_id"]), str(row["productFilename"]))


def flatten(lc, window_length: int = 901, mask=None):
    """Normalise, remove stellar variability, then drop NaNs and upward outliers.

    window_length is in cadences (901 x 2 min is about 30 h); it must be much longer than a transit.
    mask marks in-transit cadences to leave out of the trend fit, so the filter does not eat the dip.
    When passing a mask, pass lc.remove_nans() so the mask lines up with the cadences.
    """
    lc = lc.remove_nans().normalize()
    return lc.flatten(window_length, mask=mask).remove_outliers(sigma_upper=4, sigma_lower=np.inf)


def run_bls(
    time,
    flux,
    min_period: float = 0.5,
    max_period: float = 15.0,
    durations=DEFAULT_DURATIONS,
) -> BLSResult:
    """Search a flattened light curve for the strongest box-shaped periodic dip."""
    time = np.asarray(time, dtype=float)
    flux = np.asarray(flux, dtype=float)
    durations = [d for d in durations if d < min_period]
    bls = BoxLeastSquares(time, flux)
    periods = bls.autoperiod(durations, minimum_period=min_period, maximum_period=max_period, frequency_factor=0.5)
    periodogram = bls.power(periods, durations, objective="snr")
    best = int(np.argmax(periodogram.power))
    return BLSResult(
        period=float(periodogram.period[best]),
        t0=float(periodogram.transit_time[best]),
        duration=float(periodogram.duration[best]),
        depth=float(periodogram.depth[best]),
        power=float(periodogram.power[best]),
    )


def transit_mask(time, result: BLSResult, width: float = 1.5):
    """True for cadences within width transit durations of a predicted mid-transit."""
    time = np.asarray(time, dtype=float)
    phase = (time - result.t0 + 0.5 * result.period) % result.period - 0.5 * result.period
    return np.abs(phase) < 0.5 * width * result.duration


def plot_folded(time, flux, result: BLSResult, path: Path, title: str = "") -> Path:
    """Save a phase-folded plot centred on the transit, with binned points."""
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    time = np.asarray(time, dtype=float)
    flux = np.asarray(flux, dtype=float)
    phase_hours = ((time - result.t0 + 0.5 * result.period) % result.period - 0.5 * result.period) * 24
    window = 3 * result.duration * 24
    near = np.abs(phase_hours) < window

    edges = np.linspace(-window, window, 61)
    centres = 0.5 * (edges[1:] + edges[:-1])
    idx = np.digitize(phase_hours[near], edges) - 1
    binned = np.array([np.median(flux[near][idx == i]) if np.any(idx == i) else np.nan for i in range(len(centres))])

    fig, ax = plt.subplots(figsize=(8, 4.5))
    ax.plot(phase_hours[near], flux[near], ".", ms=2, color="0.6", alpha=0.5, label="2-min cadence")
    ax.plot(centres, binned, "o", ms=4, color="C0", label="binned")
    ax.set_xlabel("Hours from mid-transit")
    ax.set_ylabel("Normalised flux")
    ax.set_title(title or f"P = {result.period:.5f} d, depth = {result.depth * 1e6:.0f} ppm")
    ax.legend(loc="lower right")
    fig.tight_layout()
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(path, dpi=150)
    plt.close(fig)
    return path


def period_error(found: float, known: float) -> float:
    """Fractional difference between a recovered and a known period."""
    return abs(found - known) / known


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--target", default="WASP-18", help="star name or TIC ID resolvable by MAST")
    parser.add_argument("--known-period", type=float, default=0.94145, help="published period in days")
    parser.add_argument("--sector", type=int, default=2, help="TESS sector (default 2)")
    parser.add_argument("--min-period", type=float, default=0.5)
    parser.add_argument("--max-period", type=float, default=15.0)
    parser.add_argument("--tolerance", type=float, default=0.01, help="allowed fractional period error")
    parser.add_argument("--attempts", type=int, default=3, help="download attempts before giving up")
    parser.add_argument("--out", type=Path, default=Path("output/folded_transit.png"))
    args = parser.parse_args(argv)

    try:
        raw = download_light_curve(args.target, sector=args.sector, attempts=args.attempts).remove_nans()
    except NoDataError as exc:
        log.error("%s", exc)
        return EXIT_NO_DATA
    except DownloadError as exc:
        log.error("%s", exc)
        return EXIT_DOWNLOAD_FAILED
    lc = flatten(raw)
    result = run_bls(lc.time.value, lc.flux.value, min_period=args.min_period, max_period=args.max_period)
    # Second pass: re-flatten with the transits masked out of the trend, then search again.
    lc = flatten(raw, mask=transit_mask(raw.time.value, result))
    result = run_bls(lc.time.value, lc.flux.value, min_period=args.min_period, max_period=args.max_period)
    error = period_error(result.period, args.known_period)

    title = (
        f"{args.target}, TESS sector {args.sector}: P = {result.period:.5f} d "
        f"(known {args.known_period} d, error {error:.3%})"
    )
    plot = plot_folded(lc.time.value, lc.flux.value, result, args.out, title=title)
    log.info(
        "period %.5f d (known %s d, error %.3f%%), duration %.2f h, depth %.0f ppm",
        result.period, args.known_period, error * 100, result.duration * 24, result.depth * 1e6,
    )
    log.info("folded transit plot saved to %s", plot)

    if error > args.tolerance:
        log.error("recovered period is off by %.3f%%, more than %.3f%%", error * 100, args.tolerance * 100)
        return EXIT_PERIOD_MISMATCH
    return EXIT_OK


if __name__ == "__main__":
    sys.exit(main())
