
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

Pulls the latest dump from `s3://$S3_BUCKET_NAME/backups/`, verifies integrity, and restores it into your local Postgres.

### Prerequisites
- **AWS CLI** configured with a profile that can read the bucket (one-time):
```bash
aws configure --profile nhl-beyond
# Region: us-east-2
```

- **Dump to S3 (maintainer):**
```bash
make db-dump \
  AWS_PROFILE=nhl-beyond AWS_REGION=us-east-2 S3_BUCKET_NAME=ewnike-mads593-nhl \
  PGHOST=127.0.0.1 PGPORT=5432 PGUSER=postgres PGPASSWORD='YOUR_PASSWORD' PGDATABASE=nhl_beyond
```

- **Restore latest from S3 (to nhl_beyond_test):**
```bash
make db-restore \
  AWS_PROFILE=nhl-beyond AWS_REGION=us-east-2 S3_BUCKET_NAME=ewnike-mads593-nhl \
  PGHOST=127.0.0.1 PGPORT=5432 PGUSER=postgres PGPASSWORD='YOUR_PASSWORD' \
  PGDATABASE=nhl_beyond_test TARGET_DB=nhl_beyond_test
```

- **Verify:**
```bash
psql -h 127.0.0.1 -p 5432 -U postgres -d nhl_beyond_test -c "\dt"
```

Swap `YOUR_PASSWORD` for your actual password (keep the quotes).



## Project layout
```mermaid
flowchart TD
  A[NHL-Beyond-27/]

  subgraph S[configs & meta]
    G[pyproject.toml]
    H[.pre-commit-config.yaml]
    P[.env (ignored)]
    R[README.md]
  end

  subgraph N[notebooks (top-level files)]
    N1[NHL_Beyond27_Book1_EH_download_and_DB.ipynb]
    N2[NHL_Beyond27_Book2_HockeyRef_Scrape_and_Cleaning.ipynb]
    N3[NHL_Beyond27_Book3_Analysis.ipynb]
  end

  subgraph U[utilities]
    U1[constants.py]
    U2[log_utils.py]
    U3[s3_utils.py]
    U4[time_utils.py]
    U5[db_utils.py]
  end

  subgraph W[scrapers]
    W1[scrap_hcky_ref_evenstrength.py]
    W2[scrap_hockey_ref_player.py]
    W3[download_ref_hockey.py]
  end

  subgraph PIP[pipeline & build]
    P1[ingest_peak_season.py]
    P2[build_player_streaks_and_aligned.py]
    P3[build_player_five_year_aligned_z.py]
    P4[build_ref_hockey.py]
    P5[drop_goalies_etal_inplace.py]
    P6[build_ref_hockey_data_table.py]
    P7[diff_players_by_season.py]
  end

  subgraph D[data/]
    D1[seasons/]
    D2[even_strength/]
    D3[goalies/]
    D4[outputs/]
    D5[peak_player_season_stats.csv]
  end

  subgraph Q[sql/]
    Q1[v_player_spicy_by_rel_age.sql]
  end

  L[logs/]

  A --- S
  A --- N
  A --- U
  A --- W
  A --- PIP
  A --- D
  A --- Q
  A --- L

  %% logical flows
  W3 --> D1 & D2 & D3
  W1 --> D2
  W2 --> D1
  P4 --> D4
  P5 --> D4
  P6 --> D4
  P1 --> D5
  P2 -->|reads DB: player_peak_season| Q
  P3 -->|creates: player_five_year_aligned_z| Q1

  U3 -. S3 I/O .- D
  U5 -. DB Engine .- Q

