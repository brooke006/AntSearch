# Ant scheduler parameter sweep

We swept the ant scheduler's three main settings (alpha, beta and the evaporation rate) over
simulated workloads to pick defaults and to see how much they matter.

**Chosen settings:** alpha = 1, beta = 4, evaporation = 0.2. They are the defaults in
[scheduler.py](../src/antsearch/scheduler.py) and in [compose.yaml](../compose.yaml), and a test
checks that the two agree. The only change from before is beta, which went from 2 to 4. The late
factor (3) and the pheromone floor (0.2) were not swept.

Full results: [parameter-sweep-results.md](parameter-sweep-results.md) (tables) and
[parameter-sweep-runs.csv](parameter-sweep-runs.csv) (one row per simulation).

## Method

`make sweep` (or `python -m antsearch.sweep`) runs the real coordinator and scheduler code with
simulated workers on a fake clock. Each simulation pushes the 200 stars of the bundled TESS list
through four workers. Stars arrive at random times, at a rate that keeps the cluster 50% or 80%
busy, and a star whose run fails is queued again. A run takes 10 s for a short task and 20 s for a
multi-sector one, with ±20% noise, times the worker's slowness. These times are stand-ins, not
measurements.

| Workload | Workers |
|---|---|
| mixed-speeds | two normal; one 2× slower; one normal on short tasks but 4× slower on multi-sector ones |
| slowdown | four equal; one becomes 5× slower a third of the way through |
| flaky | four equal; one fails half of its runs, halfway through each |

**Score:** the mean time from a star being queued to its first successful result, divided by
round-robin's on the same work. The score is averaged over the 3 workloads, 2 loads and 10
seeds. Lower is better, so 0.96 means stars finish 4% sooner than under round-robin.

**Grid:**
- alpha ∈ {0, 0.5, 1, 2}
- beta ∈ {0, 1, 2, 4, 8}
- evaporation ∈ {0.05, 0.1, 0.2, 0.4}
- round-robin, random and greedy as baselines.

With alpha = 0 the pheromone has no effect, so those settings run once per beta rather than once
for every evaporation rate. That gives 68 settings and 4,080 simulations, which take about 11
minutes on 10 cores. The same command always gives the same numbers.

All settings see the same arrivals, run times and failures for a given seed. So two settings can be
compared seed by seed ("paired"), which cancels most of the noise. The ± values below are standard
errors.

## Results

| Setting | Score |
|---|--:|
| ant, chosen (alpha 1, beta 4, evaporation 0.2) | 0.964 |
| ant, best in the grid (alpha 0.5, beta 8, evaporation 0.2) | 0.959 |
| ant, previous defaults (alpha 1, beta 2, evaporation 0.2) | 0.969 |
| ant, speed only (alpha 0, beta 4) | 0.966 |
| ant, pheromone only (alpha 2, beta 0, evaporation 0.2) | 0.980 |
| ant, uniform (alpha 0, beta 0) | 0.995 |
| greedy | 0.964 |
| random | 0.995 |
| round-robin | 1.000 |

How sensitive the score is to each setting, changing one at a time from the chosen settings:

- **Beta matters most.** The score goes from 0.989 at beta 0 to 0.964 at beta 4. Moving from 2 to 4
  gains 0.005 ± 0.002, but moving from 4 to 8 gains only 0.002 ± 0.004, which is within noise.
  We stopped at 4 because a higher beta makes the scheduler behave almost like greedy. A worker
  that was slow then rarely gets a task, so it rarely gets a chance to show it has recovered.
- **Alpha barely matters once beta is set.** Every value from 0 to 2 lands within 0.003 of the
  chosen score. Pheromone on its own does help: alpha 2 with beta 0 scores 0.980, against 0.995 for
  a uniform choice, because deposits grow with speed. The speed estimate just carries the same
  information more directly.
- **Evaporation makes no measurable difference.** Every rate from 0.05 to 0.4 is within 0.001 of
  the chosen score. We kept 0.2, the value #16 was tuned with: a worker that slows down loses its
  share within a few rounds, and equal workers don't drift too far apart.
- **Load matters more than any setting.** Almost all of the gain comes when the cluster is 50% busy,
  for example 0.87 on mixed-speeds. At 80% busy, every scheduler is within about 3% of round-robin,
  because there is seldom more than one idle worker to choose from.

The best ant settings tie with greedy. In these workloads, the ant scheduler's edge comes from its
speed term.

## What the sweep doesn't fix

On the flaky workload, every scheduler had between 34 and 38 failed runs per simulation, and no
setting did better. The pheromone does punish failures, but the scheduler only chooses among idle
workers. A worker that keeps failing finishes early, so it is often the only idle worker, and then
it gets the next task whatever its pheromone. Avoiding it would take a scheduler that can hold a
task for a better worker that is still busy. That change is outside this sweep.

## Limits

- **Simulated workers:** run times are stand-ins and there are only four workers. Real runs on the
  compose cluster (`make scenario`) are needed to confirm the gains.
- **Untested failure modes:** no crashes, network delays or disputed results are simulated.
- **Settings not swept:** the late factor and the pheromone floor stayed at 3 and 0.2.
