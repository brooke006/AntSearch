
## Daniel: intro and overview

- The problem: search 200 TESS stars for planets using volunteer computers that can be slow, crash, or give wrong answers.
- **Split:** we cut it into five parts, one file each: the search on one star (`pipeline.py`), handing out work (`coordinator.py`), doing the work (`worker.py`), choosing who gets it (`scheduler.py`), and trusting the answer (`validation.py`).
- **Split:** show the big-picture diagram in [architecture.md](architecture.md). Each part is tested on its own, so we could build them in parallel.
- **Network:** workers pull work over HTTP. A worker only makes outgoing requests, so a laptop behind home Wi-Fi can join without opening a port.
- **Code:** the worker loop in [worker.py](../src/antsearch/worker.py): `register`, `request_task`, `execute`, `report_result`, with `send_heartbeats` on a background thread.
- **Network:** it runs on one laptop with `make up`, or across machines with `make coordinator` and `make worker` over the same network or Tailscale.
- Live: `curl localhost:8000/workers` to show the workers online, then `make submit STARS=10` and hand over while it runs.

## Jessi: coordinator

- **Code:** [coordinator.py](../src/antsearch/coordinator.py) is a FastAPI service. Workers register, send heartbeats, ask for a task and post a result, each as one endpoint.
- **Algorithms:** all state is in Redis, described at the top of the file: a list for the task queue, a hash for each worker and each task, and sets for the tasks a worker holds and has already run.
- **Code:** why Redis and not Python dictionaries: the coordinator can restart in the middle of a run and lose nothing.
- **Network:** status codes are the protocol. 204 means "nothing for you, ask again", 404 means "register first", 409 means "you were marked offline, drop that result".
- **Network:** failure detection. `mark_offline` flags a worker whose heartbeats stop, and its tasks go back to the front of the queue for someone else.
- **Algorithms:** two workers asking at the same moment must not get the same task. `choose_task` uses a Redis WATCH/MULTI transaction, which retries if anything it read has changed.
- **Algorithms:** every star runs on two workers. `decide` in [validation.py](../src/antsearch/validation.py) accepts when two answers match within 1%, and asks a third worker to break a tie.
- **Code:** a worker that is outvoted is flagged, and shows up as `flagged` in `GET /workers`.

## Brooke: scheduler

- **Split:** every scheduler in [scheduler.py](../src/antsearch/scheduler.py) has the same two methods, `assign` and `on_result`, so the coordinator doesn't care which one is running.
- **Algorithms:** four baselines to compare against: first-come, round-robin, random, and greedy, which always picks the worker with the lowest average task time.
- **Algorithms:** the ant scheduler, in `AntScheduler.probabilities`: each worker's weight is `pheromone ** alpha * speed ** beta`, and one is drawn at random by weight.
- **Code:** why random and not "always the best": a slower worker still gets tried now and then, so we notice if it speeds up.
- **Algorithms:** `AntScheduler.on_result`: after every task all pheromone fades (evaporation), then a good run adds some back, more if it was faster than typical.
- **Code:** failed, late and outvoted results earn nothing, and time is measured on the coordinator's clock, so a worker can't cheat by reporting false timings.
- **Code:** levels are kept between 0.2 and 1. The ceiling stops one worker from taking everything, and the floor keeps a punished worker in the running.
- **Algorithms:** the pheromone table in [pheromone.py](../src/antsearch/pheromone.py) is a Redis hash of worker to level for each task type, and `add` is atomic so two deposits at once both count.

## Matthew: lightkurve and BLS star math

- A planet crossing its star blocks a little light, so the star's brightness dips at a regular interval. We look for that repeating dip.
- **Code:** `download_light_curve` in [pipeline.py](../src/antsearch/pipeline.py) fetches the star's TESS 2-minute brightness data from NASA's MAST archive with lightkurve, and caches it on disk.
- **Network:** downloads are retried 3 times with a growing wait, and a star with no data is reported as an error, not retried forever.
- **Algorithms:** `flatten` removes the star's own slow brightness changes with a 30-hour sliding window, so only short dips are left.
- **Algorithms:** `run_bls` is Box Least Squares. It tries every period from 0.5 to 15 days and six transit durations, fits a box-shaped dip for each, and keeps the strongest.
- **Code:** `search` does this twice. The first pass finds the transits, then we flatten again with them masked so the filter doesn't erase the dip.
- **Algorithms:** `noise` measures scatter with the median absolute deviation, which outliers don't throw off, and SNR is the signal's power over that scatter.
- **Split:** `run_task` wraps all of it into one function that takes a star name and returns a small dictionary, which is all a worker needs to know.
- Live: `python -m antsearch.pipeline`, then show `output/folded_transit.png` and the recovered period for WASP-18 b against the known 0.94145 days.

## Estefania: reports and experiments

- **Algorithms:** the coordinator appends one JSON line per event to a run log. `compute` in [metrics.py](../src/antsearch/metrics.py) replays those events to get completion time, throughput and wasted work.
- **Code:** wasted work is worker time that produced no answer: runs lost in a crash, failed runs, and outvoted runs.
- **Split:** experiments are scripted in [scripts/](../scripts/): identical workers, uneven workers, and failing workers. Each runs all five schedulers on the same stars.
- **Network:** [scenario.py](../src/antsearch/scenario.py) makes failures repeatable: it limits a worker's CPU, adds network delay, and kills and restarts workers on a timeline.
- Show the charts in [figures/](figures/). On uneven workers with 50 stars, ant finished in 176 s and greedy in 184 s, against 201 s for round-robin and 264 s for first-come.
- **Code:** [recall.py](../src/antsearch/recall.py) checks our answers against the catalogue. On all 200 stars we recovered 82 of 100 known planets, and 82 of the 89 within the period range we search ([planet-recall.md](planet-recall.md)).
- **Algorithms:** [sweep.py](../src/antsearch/sweep.py) simulated 4,080 runs to choose the ant settings. Beta, the weight on speed, matters most; alpha and evaporation barely change the result ([parameter-sweep.md](parameter-sweep.md)).
- Close: `make test` covers each part on its own, and CI runs it on every push. Then questions.
