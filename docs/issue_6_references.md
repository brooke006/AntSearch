# Reference Check  
| Ref | Status | Notes |
|-----|--------|-------|
| [1] Dorigo et al. 1996 | ✅ Verified | No changes. Supports Eqs. 1, 3, 4 (p. 31) |
| [2] Dorigo & Gambardella 1997 | ✅ Verified | No changes (confirm DOI link). Supports both [1, 2] citations: Sec. II p. 54, Eqs. 1-2 p. 55. Uses "stigmergy" (p. 55). Lead: Schoonderwoerd et al. 1997 (p. 64) |
| [3] Anderson 2004 | ✅ Verified | No changes. Supports SETI@home/BOINC claim (Sec. 1 p. 4; Sec. 2.2 p. 5). |
| [4] Ricker et al. 2015 | ⚠️ Corrected | Year changed 2014 → 2015. Published online Oct. 24, 2014, but assigned to Vol. 1(1), Jan–Mar 2015 (see journal citation line, p. 014003-1). |
| [5] Kovács et al. 2002 | ✅ Verified | No changes. Original BLS paper; defines Box-fitting Least Squares (Sec. 2). |
| [6] Deneubourg et al. 1990 | ✅ Verified | No changes to citation. Supports ant trail-laying/following. Flag: Discussion cites [6] for "ant and termite" stigmergy; paper covers ants only and summary doesn't use "stigmergy." Suggest citing [2, 6] (stigmergy on [2] p. 55). |
| [7] Lightkurve 2018 | ✏️ Updated | Details verified against ASCL record and developers' preferred citation (ADS 2018ascl.soft12013L). Added URL https://ascl.net/1812.013 since no DOI exists. |
| [8] Astropy 2022 | ✅ Verified | No changes. BLS periodogram in astropy.timeseries (Sec. II.2.1, "Time series"). Also: MAST contributes to astroquery, which Lightkurve uses (Sec. IV.3). |

# New Sources
## Huang, Deng & Wen (2009): Trust-based ACO for Grid Resource Scheduling

**Citation:**  
Huang, W., Deng, Z., & Wen, P. (2009). Trust-based ant colony
optimization for grid resource scheduling. In 2009 Third International
Conference on Genetic and Evolutionary Computing (pp. 288–292). IEEE.
https://doi.org/10.1109/WGEC.2009.180

**What they did:**  
ACO grid scheduler where each node has its own pheromone and
a trust score based on task completion rate. Success raises the score, failure
lowers it (penalty about twice the reward), and failed tasks are rescheduled.
Beat Min-Min in a 6-node GridSim test.

**How it relates to AntSearch:**  
Closest match to our worker scoring: per-worker
pheromone, reward/penalty, and evaporation. Difference: it checks whether a task
finished, not whether the result was correct, which our redundant execution handles.

**Useful for:**  
Literature review; scheduler tuning (reward vs. penalty sizes).

## Tawfeek et al. (2013): Cloud Task Scheduling Based on ACO

**Citation:**  
Tawfeek, M. A., El-Sisi, A., Keshk, A. E., & Torkey, F. A. (2013).
Cloud task scheduling based on ant colony optimization. In 2013 8th International
Conference on Computer Engineering & Systems (ICCES) (pp. 64–69). IEEE.
https://doi.org/10.1109/ICCES.2013.6707172

**What they did:**  
ACO scheduler that assigns batches of incoming tasks to
heterogeneous cloud VMs to minimize makespan. Tuned parameters (including
evaporation) one at a time, then beat FCFS and round-robin on makespan and
load balance in CloudSim.

**How it relates to AntSearch:**  
Same round-robin baseline and parameter sweep
we plan. Difference: pheromone is on task-VM pairs and planned per batch, not a
live per-worker score, and it doesn't handle failures or bad results.

**Useful for:**  
Literature review (cloud setting); experiment design (baselines,
parameter sweep, degree-of-imbalance metric).