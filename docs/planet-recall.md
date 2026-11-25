# Planet recall on the star list

All 200 stars in `tess_data_mixed.csv` (100 confirmed or known planets, 100 TOIs ruled false positives), searched on 24 November 2026 with six workers on one Mac, each star run once (`REPLICAS=1`). The run log, metrics and per-star results are in [runs/](runs/): `20261124-181848-round-robin.jsonl`, `.txt`, `-recall.txt` and `-recall.csv`. Reproduce the numbers with `make recall RUN=docs/runs/20261124-181848-round-robin.jsonl`.

## Results

| | |
|---|---|
| Stars with a result | 189 of 200; 11 failed with no 2-minute light curve on MAST, all of them false-positive TOIs |
| Recall | **82 of 100** known planets recovered (82.0%), period within 1% |
| Recall within the searched range | 82 of 89 planets with a known period of 0.5 to 15 days (92.1%) |
| Found at a harmonic | 2 (TOI-1054 and TOI-1055, both at half the known period, which is over 15 days) |
| Found at another period | 16, of which 9 have a known period over 15 days |
| Unvetted strong signals | 85 of 89 false-positive TOIs have a signal with SNR of at least 7.1 |
| ... the same signal that was ruled out | 64 |
| ... a new signal | 21 |

The whole run took 19 minutes, with 1.7% of worker time wasted, all of it on the 11 failed downloads.

## Notes

The SNR threshold of 7.1 keeps every real detection: the weakest recovered planet has SNR 7.6. Raising it to 10 would drop 8 recovered planets and only 2 unvetted signals, so 7.1 stays.

Most unvetted signals are expected. These stars became TOIs because TESS saw a signal on them, later shown to be an eclipsing binary or something else that isn't a planet, so the search finding that signal again (64 of 85) agrees with the catalogue.

Signals between 12 and 15 days deserve suspicion. That's close to TESS's 13.7-day orbit, and with one sector of data a period that long shows only one or two transits. 9 of the 21 new unvetted signals and 7 of the 18 missed planets were found there, against 3 of the 82 recovered planets. Many of them are probably systematics from the spacecraft's orbit rather than astrophysical signals.

The 7 misses inside the searched range haven't been looked at one by one. The search reports only one signal per star, so a second planet in the system, leftover stellar variability or an orbit systematic that beats the listed planet counts as a miss.
