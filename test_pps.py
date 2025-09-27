import pandas as pd

# df = pd.read_csv(r"data/goalies/peak_player_season_stats.csv")
# # print(df.columns)
# print(len(df))

df1 = pd.read_csv(r"data/outputs/hockeyref_final.csv")
# # print(len(df1))
print(df1.columns)
print(df1.head())
# print(df1["toi_seconds_total_ev"].dtype)


# 18,000 seconds = 300 minutes
# mask = (df1["toi_seconds_total_ev"], errors="coerce") > 18000
# df1_toi_min = df1.loc[mask]
# count = mask.sum()
# print(count)
# df1_toi_min = df1.query("toi_seconds_total_ev > 18000")
# print(len(df1_toi_min))
# import pandas as pd

# clean + coerce both columns to numeric
# toi_pg = pd.to_numeric(
#     df1["toi_seconds_total_ev"].astype(str).str.replace(r"[^\d\.\-]", "", regex=True),
#     errors="coerce",
# )
# gp = pd.to_numeric(df1["gp"], errors="coerce")

# # season EV seconds = per-game * games played
# season_ev_sec = toi_pg * gp

# # filter: > 18,000 seconds (== 300 minutes)
# mask = season_ev_sec > 18000

# # Using .loc: resulting rows and count
# df_gt = df1.loc[mask]
# count_gt = len(df_gt)  # or: mask.sum()

# print(count_gt)

df = pd.read_csv(r"data/outputs/hockeyref_final.csv")

# pick columns that exist
player_col = (
    "player"
    if "player" in df.columns
    else ("player_name" if "player_name" in df.columns else "name")
)
season_col = "season_end" if "season_end" in df.columns else "season"

# start from (player, season) uniques
x = (
    df[[player_col, season_col]]
    .copy()
    .assign(**{season_col: pd.to_numeric(df[season_col], errors="coerce")})
    .dropna(subset=[season_col])
    .astype({season_col: int})
    .drop_duplicates(subset=[player_col, season_col])
    .sort_values([player_col, season_col])
)

# compute run key via (season - row_number) trick
x["rownum"] = x.groupby(player_col).cumcount()
x["run_key"] = x[season_col] - x["rownum"]

# run length for each row = size of its (player, run_key) group
x["run_len"] = x.groupby([player_col, "run_key"])["run_key"].transform("size")

# optional: the longest run per player
max_run_per_player = x.groupby(player_col)["run_len"].max().sort_values(ascending=False)

# filter rows that are part of a run ≥ 5 consecutive seasons
x_5plus_rows = x[x["run_len"] >= 5].copy()

# unique players with ≥ 5 consecutive seasons
players_5plus = x_5plus_rows[player_col].drop_duplicates().sort_values(key=lambda s: s.str.lower())

# quick peeks
len(players_5plus), players_5plus.head(10).tolist(), x_5plus_rows.head()
