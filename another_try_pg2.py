"""
Code to scrape the even strength 5v5 toi information for
NHL player seasons 203-14 - 2024-25.
"""

import os
import re
import sys
import time
from contextlib import suppress

import pandas as pd
from bs4 import BeautifulSoup, Comment
from dotenv import load_dotenv
from selenium import webdriver
from selenium.common.exceptions import InvalidSessionIdException
from selenium.webdriver import ActionChains
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from webdriver_manager.chrome import ChromeDriverManager

# =========================
# CONFIG
# =========================
load_dotenv()  # read SR_EMAIL/SR_PASSWORD (or STATHEAD_*) from .env if present

HR_HOME = "https://www.hockey-reference.com/"
# Set the season end year you want: 2025 for 2024-25, 2024 for 2023-24, etc.
SEASON_END_YEAR = 2014

# Credentials: SR_* preferred, fallback to STATHEAD_*
EMAIL = os.getenv("SR_EMAIL") or os.getenv("STATHEAD_EMAIL") or "YOUR_EMAIL_HERE"
PASSWORD = os.getenv("SR_PASSWORD") or os.getenv("STATHEAD_PASSWORD") or "YOUR_PASSWORD_HERE"

# We will look for either SkaterStandard or the classic skaters table
# PLAYER_KEYS = ("name_display", "player")
# TEAM_KEYS = ("team_name_abbr", "team_id")
# GP_KEYS = ("games", "games_played")
# TOI_KEYS = ("time_on_ice",)
# AGE_KEYS = ("age",)
# Keys for the TOI page (robust across SR variants)
PLAYER_KEYS = ("name_display", "player")
TEAM_KEYS = ("team_name_abbr", "team_id")
POS_KEYS = ("pos",)
SHIFT_KEYS = ("shifts", "shift")  # page uses "shifts"
GP_KEYS = ("games", "games_played")
TOI_KEYS = ("time_on_ice", "toi")
CFREL_KEYS = ("corsi_for_pct_rel", "cf_pct_rel", "cf% r", "cf%_rel", "corsi_rel")


# If you want to anchor on your visible table first:
XPATH_TABLE = (
    # "/html/body/div[4]/div[5]/div[1]/div[4]/div[2]/table"  # your SkaterStandard <table> path
    "/html/body/div[4]/div[5]/div[1]/div[2]/table/tbody"
)


# =========================
# UTILS
# =========================
def _get_first_tr_text(tr, keys):
    """Return text from the first <td> in this <tr> whose data-stat matches any key (or from TH for player)."""
    # player might be in <th> OR <td>
    for k in keys:
        el = tr.find(["td", "th"], attrs={"data-stat": k})
        if el:
            return el.get_text(strip=True)
    return ""


def _extract_rows_from_bs_table(tbl) -> list[dict]:
    """
    Extract rows by data-stat for the Skater TOI page.
    Returns a list of dicts with: Player, Tm, Pos, Shift, GP, TOI (EV), CF% R (EV)
    """
    tbody = tbl.find("tbody")
    if not tbody:
        return []

    # Build a header-label map (lowest header row)
    header_labels = []
    thead = tbl.find("thead")
    if thead:
        hdr_trs = thead.find_all("tr")
        if hdr_trs:
            last_hdr = hdr_trs[-1]
            header_labels = [
                th.get_text(strip=True).lower() for th in last_hdr.find_all(["th", "td"])
            ]

    def get_any(row, keys, allow_th=False):
        """
        Try to extract a value from this row for any of the provided keys.
        Strategy:
          1) exact data-stat
          2) lowercase exact data-stat
          3) substring match on data-stat
          4) match by header cell text (fallback)
        """
        tags = ["td", "th"] if allow_th else ["td"]

        # 1) exact data-stat
        for k in keys:
            el = row.find(tags, attrs={"data-stat": k})
            if el:
                return el.get_text(strip=True)

        # 2) lowercase exact
        for k in keys:
            el = row.find(tags, attrs={"data-stat": k.lower()})
            if el:
                return el.get_text(strip=True)

        # 3) substring match on data-stat
        for cell in row.find_all(tags):
            ds = (cell.get("data-stat") or "").lower()
            for k in keys:
                kk = k.lower()
                if kk and kk in ds:
                    return cell.get_text(strip=True)

        # 4) fallback by header text mapping (best-effort)
        if header_labels:
            cells = row.find_all(tags)
            for i, cell in enumerate(cells):
                label = header_labels[i] if i < len(header_labels) else ""
                # normalize keys to compare with header labels
                for k in keys:
                    normk = k.replace("_", " ").strip().lower()
                    if normk and normk in label:
                        return cell.get_text(strip=True)

        return ""

    out = []

    # Prefer EV-specific fields, then fall back to generic
    EV_TOI_KEYS = ("ev_toi", "even_strength_toi", "toi_ev", "time_on_ice", "toi")
    EV_CFREL_KEYS = (
        "ev_cf_pct_rel",
        "cf_pct_rel_ev",
        "corsi_for_pct_rel_ev",
        "corsi_for_pct_rel",
        "cf_pct_rel",
        "cf% r",
        "cf%_rel",
        "corsi_rel",
    )

    for tr in tbody.find_all("tr"):
        # skip header separators within tbody
        if "thead" in (tr.get("class") or []):
            continue

        player = get_any(tr, PLAYER_KEYS, allow_th=True)
        if not player or player == "Player":
            continue

        tm = get_any(tr, TEAM_KEYS)
        pos = get_any(tr, POS_KEYS)
        shift = get_any(tr, SHIFT_KEYS)
        gp = get_any(tr, GP_KEYS)
        ev_toi = get_any(tr, EV_TOI_KEYS)
        ev_rel = get_any(tr, EV_CFREL_KEYS)

        out.append(
            {
                "Player": player,
                "Tm": tm,
                "Pos": pos,
                "Shift": shift,
                "GP": gp,
                "TOI": ev_toi,  # EV TOI
                "CF% R": ev_rel,  # EV CF% Rel
            }
        )

    return out


def wait(driver, cond, timeout=20):
    return WebDriverWait(driver, timeout).until(cond)


def dismiss_cookie_banners(driver):
    texts = [
        "Agree",
        "I Agree",
        "Accept",
        "Accept All",
        "Got it",
        "OK",
        "Continue",
        "Understood",
        "I accept",
        "Accept Cookies",
    ]
    selectors = [
        "//button[normalize-space()='{t}']",
        "//a[normalize-space()='{t}']",
        "//input[@value='{t}']",
        "//*[@id='onetrust-accept-btn-handler']",
        "//*[@id='truste-consent-button']",
    ]
    for t in texts:
        for sel in selectors:
            xp = sel.format(t=t)
            try:
                el = WebDriverWait(driver, 1.2).until(EC.element_to_be_clickable((By.XPATH, xp)))
                el.click()
                time.sleep(0.2)
                return
            except Exception:
                pass


def make_driver():
    options = webdriver.ChromeOptions()
    options.add_argument("--window-size=1400,1000")
    # options.add_argument("--headless=new")
    service = Service(ChromeDriverManager().install())  # ← no ChromeType
    d = webdriver.Chrome(service=service, options=options)
    d.set_page_load_timeout(60)
    return d


# =========================
# SCRAPE HELPERS
# =========================
def _normalize_toi(toi: str) -> str:
    s = (toi or "").strip()
    if re.match(r"^\d+:\d{2}(:\d{2})?$", s):
        parts = list(map(int, s.split(":")))
        if len(parts) == 2:
            m, sec = parts
            h, m = divmod(m, 60)
            return f"{h:02d}:{m:02d}:{sec:02d}"
        if len(parts) == 3:
            h, m, sec = parts
            return f"{h:02d}:{m:02d}:{sec:02d}"
    if s.isdigit():
        m = int(s)
        h, m = divmod(m, 60)
        return f"{h:02d}:{m:02d}:00"
    return s


def _to_int_safe(x):
    if pd.isna(x):
        return None
    m = re.search(r"-?\d+", str(x))
    return int(m.group()) if m else None


def _collapse_rows_toi(raw: pd.DataFrame, season_end_year: int) -> pd.DataFrame:
    df = raw.copy()

    # Coerce types / tidy
    df["Player"] = df["Player"].astype(str).str.strip()
    df = df[df["Player"].ne("Player")]
    for c in ("GP", "Shift"):
        if c in df.columns:
            df[c] = df[c].apply(_to_int_safe)
    if "TOI" in df.columns:
        df["TOI"] = df["TOI"].astype(str).str.strip()
    if "Tm" in df.columns:
        df["Tm"] = df["Tm"].astype(str).str.strip()

    # Split TOT vs team rows
    is_tot = df["Tm"].eq("TOT") if "Tm" in df.columns else pd.Series(False, index=df.index)
    tot_rows = df[is_tot].copy()
    team_rows = df[~is_tot].copy()

    # Build teams list per player (excluding TOT/nan)
    teams_per_player = {}
    if not team_rows.empty and "Tm" in team_rows.columns:
        teams_per_player = (
            team_rows.groupby("Player")["Tm"]
            .apply(
                lambda s: ",".join(
                    sorted({t for t in s if t and t.upper() != "TOT" and t.lower() != "nan"})
                )
            )
            .to_dict()
        )

    out = []

    # Prefer TOT row if present (keeps GP/TOI aggregated)
    for _, r in tot_rows.iterrows():
        out.append(
            {
                "season_end": season_end_year,
                "player": r["Player"],
                "tm": teams_per_player.get(r["Player"], r.get("Tm", "")),
                "pos": r.get("Pos", ""),
                "shift": r.get("Shift", None),
                "gp": r.get("GP", 0),
                "toi": _normalize_toi(r.get("TOI", "")),
                "cf_rel": r.get("CF% R", ""),
            }
        )

    # Players without TOT → keep single-team row
    singles = team_rows[~team_rows["Player"].isin(set(tot_rows["Player"]))].copy()
    for _, r in singles.iterrows():
        out.append(
            {
                "season_end": season_end_year,
                "player": r["Player"],
                "tm": r.get("Tm", ""),
                "pos": r.get("Pos", ""),
                "shift": r.get("Shift", None),
                "gp": r.get("GP", 0),
                "toi": _normalize_toi(r.get("TOI", "")),
                "cf_rel": r.get("CF% R", ""),
            }
        )

    out_df = pd.DataFrame(
        out, columns=["season_end", "player", "tm", "pos", "shift", "gp", "toi", "cf_rel"]
    )

    # Keep only GP >= 1; if that empties result, keep all (debugging/partial seasons)
    filtered = out_df[pd.to_numeric(out_df["gp"], errors="coerce").fillna(0) >= 1].reset_index(
        drop=True
    )
    if filtered.empty:
        print("[warn] All rows had GP < 1; keeping zero-GP rows for inspection.")
        return out_df.reset_index(drop=True)
    return filtered


# --- NEW: column normalization helpers ---
_COL_ALIASES = {
    "player": ["player"],
    "age": ["age"],
    "gp": ["gp", "gms", "games"],
    "toi": ["toi", "time on ice", "time_on_ice"],
    "tm": ["tm", "team"],
}


def _flatten_headers(df: pd.DataFrame) -> pd.DataFrame:
    """Flatten MultiIndex columns and normalize names (lower, strip)."""
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = [
            " ".join([str(x) for x in tup if str(x) != "nan"]).strip() for tup in df.columns.values
        ]
    df.columns = [str(c).strip() for c in df.columns]
    return df


def _standardize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Map various header spellings to canonical: Player, Age, GP, TOI, Tm."""
    name_map = {}
    lower_cols = {c.lower(): c for c in df.columns}

    def find_any(keys):
        for k in keys:
            if k in lower_cols:
                return lower_cols[k]
        return None

    player_c = find_any(_COL_ALIASES["player"])
    age_c = find_any(_COL_ALIASES["age"])
    gp_c = find_any(_COL_ALIASES["gp"])
    toi_c = find_any(_COL_ALIASES["toi"])
    tm_c = find_any(_COL_ALIASES["tm"])

    # Require at least Player + GP + Tm (TOI/Age can be missing occasionally)
    needed = [player_c, gp_c, tm_c]
    if any(x is None for x in needed):
        raise ValueError("Required columns not found after normalization")

    name_map[player_c] = "Player"
    name_map[gp_c] = "GP"
    name_map[tm_c] = "Tm"
    if age_c:
        name_map[age_c] = "Age"
    if toi_c:
        name_map[toi_c] = "TOI"

    return df.rename(columns=name_map)


# --- REPLACE: _parse_from_comments ---
def _parse_visible_dom_tables(html: str) -> pd.DataFrame | None:
    soup = BeautifulSoup(html, "lxml")
    candidates = soup.select("table#stats, table.stats_table") or soup.select("table")
    for tbl in candidates:
        rows = _extract_rows_from_bs_table(tbl)
        if rows and any(r.get("GP") for r in rows):
            return pd.DataFrame(rows)
    return None


def _parse_from_comments(html: str) -> pd.DataFrame:
    soup = BeautifulSoup(html, "lxml")
    spaces = soup.select("div[id^=all_]") + [soup]
    for space in spaces:
        for node in space.find_all(string=lambda t: isinstance(t, Comment)):
            if "<table" not in node:
                continue
            inner = BeautifulSoup(node, "lxml")
            for tbl in inner.select("table#stats, table.stats_table, table"):
                rows = _extract_rows_from_bs_table(tbl)
                if rows:
                    return pd.DataFrame(rows)
    raise RuntimeError("TOI skaters table not found inside HTML comments.")


# --- REPLACE: _collapse_rows ---
def _collapse_rows(
    raw: pd.DataFrame, season_end_year: int, keep_zero_gp_when_empty: bool = True
) -> pd.DataFrame:
    # Robust integer parsing for GP and Age
    def to_int(x):
        if pd.isna(x):
            return None
        m = re.search(r"\d+", str(x))
        return int(m.group()) if m else None

    raw = raw.copy()
    raw["Player"] = raw["Player"].astype(str).str.strip()
    raw = raw[raw["Player"].ne("Player")]  # drop header repeats
    raw["Age"] = raw["Age"].apply(to_int) if "Age" in raw.columns else None
    raw["GP"] = raw["GP"].apply(to_int)
    raw["Tm"] = raw["Tm"].astype(str).str.strip()

    # Split TOT vs team rows
    is_tot = raw["Tm"].eq("TOT")
    tot_rows = raw[is_tot].copy()
    team_rows = raw[~is_tot].copy()

    # Build teams list per player (excluding TOT/nan)
    if not team_rows.empty:
        teams_per_player = (
            team_rows.groupby("Player")["Tm"]
            .apply(
                lambda s: ",".join(
                    sorted({t for t in s if t and t.upper() != "TOT" and t.lower() != "nan"})
                )
            )
            .to_dict()
        )
    else:
        teams_per_player = {}

    # Assemble output
    out_rows = []

    # Use TOT row when present
    for _, r in tot_rows.iterrows():
        out_rows.append(
            {
                "season_end": season_end_year,
                "player": r["Player"],
                "age": r.get("Age", None),
                "gp": r["GP"] or 0,
                "toi": _normalize_toi(r.get("TOI", "")),
                "teams": teams_per_player.get(r["Player"], r["Tm"]),
            }
        )

    # Players without TOT → single row
    singles = team_rows[~team_rows["Player"].isin(set(tot_rows["Player"]))].copy()
    for _, r in singles.iterrows():
        out_rows.append(
            {
                "season_end": season_end_year,
                "player": r["Player"],
                "age": r.get("Age", None),
                "gp": r["GP"] or 0,
                "toi": _normalize_toi(r.get("TOI", "")),
                "teams": r["Tm"] if r["Tm"] != "TOT" else "",
            }
        )

    out = pd.DataFrame(out_rows, columns=["season_end", "player", "age", "gp", "toi", "teams"])
    # Filter gp>=1, but keep zero-GP rows if everything would disappear (debug-friendly)
    filtered = out[pd.to_numeric(out["gp"], errors="coerce").fillna(0) >= 1].reset_index(drop=True)
    if filtered.empty and keep_zero_gp_when_empty:
        print("[warn] All rows had GP < 1; keeping zero-GP rows for inspection.")
        return out.reset_index(drop=True)
    return filtered


def scrape_one_season(driver, season_end_year: int) -> pd.DataFrame:
    """
    Scrape the TOI skaters page (visible DOM first; fallback to comment-wrapped).
    Returns: season_end, player, tm, pos, shift, gp, toi, cf_rel
    """
    time.sleep(0.2)
    html = driver.page_source

    df = _parse_visible_dom_tables(html)
    if df is None:
        df = _parse_from_comments(html)

    return _collapse_rows_toi(df, season_end_year)


# =========================
# NAV STEPS
# =========================
def step1_open_home(driver):
    driver.get(HR_HOME)
    wait(driver, EC.presence_of_element_located((By.ID, "header")))
    dismiss_cookie_banners(driver)
    print("STEP 1 ✅ On Hockey-Reference home")


def step2_click_ad_free_login(driver):
    try:
        link = wait(
            driver,
            EC.element_to_be_clickable((By.CSS_SELECTOR, "li.user.not_logged_in a.login")),
        )
        ActionChains(driver).move_to_element(link).pause(0.1).click(link).perform()
        time.sleep(0.6)  # give the login page/tab time to spin up
        print("STEP 2 ✅ Clicked Ad-Free Login")
    except Exception:
        link = wait(
            driver,
            EC.element_to_be_clickable(
                (
                    By.XPATH,
                    "//li[contains(@class,'user') and contains(@class,'not_logged_in')]//a[@class='login' and normalize-space()='Ad-Free Login']",
                )
            ),
        )
        link.click()
        time.sleep(0.6)
        print("STEP 2 ✅ Clicked Ad-Free Login (fallback)")

    # If a new tab opened, switch to it
    WebDriverWait(driver, 8).until(lambda d: len(d.window_handles) >= 1)
    if len(driver.window_handles) > 1:
        driver.switch_to.window(driver.window_handles[-1])

    wait(
        driver,
        EC.presence_of_element_located(
            (By.XPATH, "//input[@type='email' or @name='email' or @name='username']")
        ),
    )
    dismiss_cookie_banners(driver)


def step3_login(driver):
    def find_first(locators):
        last = None
        for how, val in locators:
            try:
                return wait(driver, EC.visibility_of_element_located((how, val)))
            except Exception as e:
                last = e
        raise last

    email_input = find_first(
        [
            (By.ID, "email"),
            (By.NAME, "email"),
            (By.NAME, "username"),
            (By.CSS_SELECTOR, "input[type='email']"),
        ]
    )
    pwd_input = find_first(
        [
            (By.ID, "password"),
            (By.NAME, "password"),
            (By.CSS_SELECTOR, "input[type='password']"),
        ]
    )

    email_input.clear()
    email_input.send_keys(EMAIL)
    pwd_input.clear()
    pwd_input.send_keys(PASSWORD)

    for loc in [
        (By.XPATH, "//button[@type='submit']"),
        (By.XPATH, "//input[@type='submit']"),
        (
            By.XPATH,
            "//button[contains(.,'Sign in') or contains(.,'Log in') or contains(.,'Login')]",
        ),
    ]:
        try:
            btn = WebDriverWait(driver, 6).until(EC.element_to_be_clickable(loc))
            btn.click()
            break
        except Exception:
            continue

    WebDriverWait(driver, 25).until(
        lambda d: "hockey-reference.com" in d.current_url or "users" in d.current_url
    )
    print("STEP 3 ✅ Submitted login")


def step4_verify_back_on_home(driver):
    if "hockey-reference.com" not in driver.current_url:
        driver.get(HR_HOME)
    wait(driver, EC.presence_of_element_located((By.ID, "header")))
    with suppress(Exception):
        wait(
            driver,
            EC.presence_of_element_located(
                (By.XPATH, "//a[contains(.,'Your Account') or contains(.,'Logout')]")
            ),
            timeout=5,
        )
    print("STEP 4 ✅ On Hockey-Reference home after login")


def step5_open_seasons_dropdown(driver):
    seasons_li = wait(driver, EC.visibility_of_element_located((By.ID, "header_leagues")))
    ActionChains(driver).move_to_element(seasons_li).perform()
    wait(
        driver,
        EC.presence_of_element_located(
            (
                By.XPATH,
                "//li[@id='header_leagues' and contains(@class,'hasmore')]//div[contains(@class,'list')]",
            )
        ),
    )
    time.sleep(0.2)
    print("STEP 5 ✅ Hovered Seasons (dropdown visible)")


def step6_go_to_skaters_toi_for_season(driver, season_end_year: int):
    """
    Prefer clicking the dropdown link if present; otherwise navigate directly to:
      /leagues/NHL_<year>_skaters-time-on-ice.html or
    """
    href = f"/leagues/NHL_{season_end_year}_skaters-time-on-ice.html"
    # Try to click from the Seasons dropdown (some pages include it)
    try:
        locator = (By.XPATH, f"//li[@id='header_leagues']//a[@href='{href}']")
        link = WebDriverWait(driver, 4).until(EC.element_to_be_clickable(locator))
        ActionChains(driver).move_to_element(link).pause(0.1).click(link).perform()
        print(f"STEP 6 ✅ Clicked Skaters (TOI) for {season_end_year}")
    except Exception:
        # Fallback: direct nav works reliably
        driver.get("https://www.hockey-reference.com" + href)
        print(f"STEP 6 ✅ Navigated directly to {href} (TOI)")


def step7_wait_for_skaters_page(driver, season_end_year: int):
    # Accept either standard or TOI page
    base = f"NHL_{season_end_year}_skaters"
    WebDriverWait(driver, 20).until(
        EC.any_of(
            EC.url_contains(base + ".html"),
            EC.url_contains(base + "-time-on-ice.html"),
        )
    )
    wait(driver, EC.presence_of_element_located((By.ID, "wrap")))
    print(f"STEP 7 ✅ Skaters page loaded for {season_end_year} (no scraping)")


# =========================
# ROBUST LOGIN WRAPPER
# =========================
def do_login_flow_with_retry():
    """
    Start fresh browser, open HR, Ad-Free Login, and submit creds.
    If the session dies, restart once. Returns a live, logged-in driver.
    """

    def run(d):
        step1_open_home(d)
        step2_click_ad_free_login(d)
        step3_login(d)
        step4_verify_back_on_home(d)
        return d

    d = make_driver()
    try:
        return run(d)
    except InvalidSessionIdException:
        with suppress(Exception):
            d.quit()

        d2 = make_driver()
        return run(d2)


# =========================
# MAIN
# =========================
def main():
    if EMAIL.startswith("YOUR_") or PASSWORD.startswith("YOUR_"):
        sys.exit(
            "Set SR_EMAIL/SR_PASSWORD (or STATHEAD_EMAIL/STATHEAD_PASSWORD) in your environment (or .env)."
        )

    driver = None
    try:
        # Login (robust)
        driver = do_login_flow_with_retry()

        # Seasons → Skaters for your year
        step5_open_seasons_dropdown(driver)
        # step6_click_skaters_for_season(driver, SEASON_END_YEAR)
        step6_go_to_skaters_toi_for_season(driver, SEASON_END_YEAR)
        step7_wait_for_skaters_page(driver, SEASON_END_YEAR)

        # Scrape one season
        df = scrape_one_season(driver, season_end_year=SEASON_END_YEAR)
        out_path = f"nhl_player_even_toi_{SEASON_END_YEAR}.csv"
        df.to_csv(out_path, index=False)
        print(f"rows scraped for {SEASON_END_YEAR}: {len(df)} (saved to {out_path})")

    finally:
        if driver:
            with suppress(Exception):
                driver.quit()


if __name__ == "__main__":
    main()
