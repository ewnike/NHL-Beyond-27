
Our study will include onlr even strength, 5 on 5 situations.

EV = Even Strength — i.e., hockey played with the same number of skaters on each side (no power play or penalty kill). 

Minutes filter: Require a minimum EV TOI per season (e.g., ≥ 500 EV minutes) to avoid noise.

Age definition: NHL standard is age on Feb 1 of the season. Compute integer age that way.
a player’s “age-27 season” as the season in which he is 27 years old on Feb 1 of that season. Then use that entire season’s 5v5 Corsi metrics.
Why this is better:
Matches NHL age convention (rosters, public datasets).
Keeps data aligned to one season (same team/linemates/context).
Avoids slicing seasons across calendar years.
So for 2015–16, “age-27 season” = born 1988-02-02 through 1989-02-01 (inclusive). You’d use each player’s full 2015–16 5v5 CF/CA (with a minutes filter), not a Feb-to-Jan calendar window.

Outcome: Prefer score- & venue-adjusted 5v5 CF% (or at least raw 5v5 CF%). Also keep CF/60 and CA/60 as secondary outcomes.

Weights: Use EV minutes as analytical weights (seasons with more minutes are estimated more precisely).
