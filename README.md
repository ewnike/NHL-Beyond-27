
Our study will include only even strength, 5 on 5 situations.

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

## Quickstart

### Prerequisites
- Python 3.10+ (tested on 3.13) and `pip`
- A PostgreSQL database you can access (local or remote)
- *(Optional)* AWS creds if you’ll ingest from S3

### Setup
```bash
# clone
git clone https://github.com/ewnike/NHL-Beyond-27.git
cd NHL-Beyond-27

# create & activate a virtualenv (pyenv example)
pyenv virtualenv 3.13.7 nhl_beyond27-3.13.7
pyenv activate nhl_beyond27-3.13.7
# (or: python -m venv .venv && source .venv/bin/activate)

# install the package (editable)
pip install -e .
```
## Download & Restore the Database (from S3)

This pulls the latest dump from `s3://$S3_BUCKET_NAME/backups/`, verifies integrity, and restores it into your local Postgres.

### Prerequisites
- **AWS CLI** configured with a profile that can read the bucket (one-time):
  ```bash
  aws configure --profile nhl-beyond
  # Region: us-east-2 (or your bucket's region)


## Project layout
```mermaid
flowchart TD
  A[NHL-Beyond-27/] --> B[src/]
  B --> C[nhl_beyond27/]
  C --> C1[__init__.py]
  C --> C2[cli.py]
  C --> C3[settings.py]
  C --> C4[logging_utils.py]
  C --> C5[backup.py]
  C --> C6[restore.py]
  C --> C7[pipeline.py]
  C --> D[builders/]
  D --> D1[__init__.py]
  D --> D2[ingest.py]
  D --> D3[aligned.py]
  D --> D4[z_player.py]
  D --> D5[z_cohort.py]
  C --> E[db/]
  E --> E1[__init__.py]
  E --> E2[utils.py]
  A --> F[tests/]
  F --> F1[test_pipeline_sanity.py]
  A --> G[pyproject.toml]
  A --> H[.pylintrc]
  A --> I[.pre-commit-config.yaml]
  A --> J[docs/project-structure.txt]
