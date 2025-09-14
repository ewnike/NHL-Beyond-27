# view_utils.py
from sqlalchemy import text

ONE_ROW_VIEW_SQL = """
CREATE VIEW public.player_peak_season_one_row AS
WITH sums AS (
  SELECT
    player, season,
    MIN(eh_id)  AS eh_id,
    MIN(api_id) AS api_id,
    CASE WHEN COUNT(DISTINCT team)=1 THEN MIN(team) ELSE 'MULTI' END AS team,
    MIN(position) AS position,
    MIN(shoots)   AS shoots,
    MIN(birthday) AS birthday,
    MAX(age)      AS age,
    MIN(draft_year)    AS draft_year,
    MIN(draft_rnd)     AS draft_rnd,
    MIN(draft_overall) AS draft_overall,
    SUM(games_played)  AS games_played,
    SUM("time_on_ice") AS toi_sum,

    -- turn per-60 into counts from the raw table’s quoted cols
    SUM(("CF/60"  * "time_on_ice")/60.0) AS cf_cnt,
    SUM(("CA/60"  * "time_on_ice")/60.0) AS ca_cnt,
    SUM(("FF/60"  * "time_on_ice")/60.0) AS ff_cnt,
    SUM(("FA/60"  * "time_on_ice")/60.0) AS fa_cnt,
    SUM(("SF/60"  * "time_on_ice")/60.0) AS sf_cnt,
    SUM(("SA/60"  * "time_on_ice")/60.0) AS sa_cnt,
    SUM(("GF/60"  * "time_on_ice")/60.0) AS gf_cnt,
    SUM(("GA/60"  * "time_on_ice")/60.0) AS ga_cnt,
    SUM(("xGF/60" * "time_on_ice")/60.0) AS xgf_cnt,
    SUM(("xGA/60" * "time_on_ice")/60.0) AS xga_cnt
  FROM public.player_peak_season
  GROUP BY player, season
)
SELECT
  player, season, eh_id, api_id, team, position, shoots, birthday, age,
  draft_year, draft_rnd, draft_overall, games_played,

  -- clean, Python-friendly names
  toi_sum                                   AS time_on_ice,
  60.0 * (cf_cnt/NULLIF(toi_sum,0))         AS cf60,
  60.0 * (ca_cnt/NULLIF(toi_sum,0))         AS ca60,
  100.0 * cf_cnt/NULLIF(cf_cnt+ca_cnt,0)    AS cf_pct,

  60.0 * (ff_cnt/NULLIF(toi_sum,0))         AS ff60,
  60.0 * (fa_cnt/NULLIF(toi_sum,0))         AS fa60,
  100.0 * ff_cnt/NULLIF(ff_cnt+fa_cnt,0)    AS ff_pct,

  60.0 * (sf_cnt/NULLIF(toi_sum,0))         AS sf60,
  60.0 * (sa_cnt/NULLIF(toi_sum,0))         AS sa60,
  CASE WHEN sf_cnt > 0 THEN 100.0 * gf_cnt/sf_cnt END                 AS sh_pct,
  CASE WHEN sa_cnt > 0 THEN 100.0 * (1.0 - ga_cnt/sa_cnt) END         AS sv_pct,

  60.0 * (gf_cnt/NULLIF(toi_sum,0))         AS gf60,
  60.0 * (ga_cnt/NULLIF(toi_sum,0))         AS ga60,
  100.0 * gf_cnt/NULLIF(gf_cnt+ga_cnt,0)    AS gf_pct,

  60.0 * (xgf_cnt/NULLIF(toi_sum,0))        AS xgf60,
  60.0 * (xga_cnt/NULLIF(toi_sum,0))        AS xga60,
  100.0 * xgf_cnt/NULLIF(xgf_cnt+xga_cnt,0) AS xgf_pct
FROM sums;
"""

def create_one_row_view(engine) -> None:
    with engine.begin() as conn:
        # Drop first so we can change output column names safely
        conn.exec_driver_sql('DROP VIEW IF EXISTS public.player_peak_season_one_row')
        # Now create with **no params**
        conn.exec_driver_sql(ONE_ROW_VIEW_SQL)   # <-- no second argument
