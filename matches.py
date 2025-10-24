#!/usr/bin/env python3
"""
Mega Football Aggregator – merges match data from OneFootball, WherestheMatch, DaddyLive, and AllFootball.
✅ Filters out non-football sports, youth & women’s matches, and fake “Extra Stream” entries.
✅ Saves all football matches (with emojis) to matches.txt.
"""

import re
import json
import unicodedata
import logging
from datetime import datetime
from bs4 import BeautifulSoup
import pytz
import requests
import cloudscraper
from rapidfuzz import fuzz

# ---------- LOGGING ----------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("matches.log", encoding="utf-8")
    ]
)
logger = logging.getLogger(__name__)

# ---------- FILTER RULES ----------
def load_banned_tournaments(filepath="banned.txt"):
    try:
        with open(filepath, "r", encoding="utf-8") as f:
            tournaments = {line.strip().lower() for line in f if line.strip()}
        logger.info(f"Loaded {len(tournaments)} banned tournaments from {filepath}")
        return tournaments
    except FileNotFoundError:
        logger.warning("banned.txt not found, continuing with empty ban list")
        return set()

BANNED_TOURNAMENTS_LOWER = load_banned_tournaments()

def is_banned_match(home: str, away: str, competition: str) -> bool:
    """Filters out women/youth/reserve/academy teams or banned tournaments."""
    lname = (competition or "").lower()

    if lname in BANNED_TOURNAMENTS_LOWER:
        return True
    if "women" in lname or "nwsl" in lname:
        return True
    if any(p in lname for p in ["u18", "u19", "u20", "u21", "u23", "youth", "reserve", "reserves", "academy"]):
        return True
    if "women" in home.lower() or "women" in away.lower():
        return True
    return False

# ---------- NAME HELPERS ----------
def normalize_team_name(name: str) -> str:
    n = unicodedata.normalize("NFKD", name or "").encode("ascii", "ignore").decode("utf-8").lower()
    n = re.sub(r"[^a-z ]", " ", n)
    n = re.sub(r"\s+", " ", n).strip()
    stopwords = {"fc", "cf", "club", "the", "team", "deportivo"}
    return " ".join(t for t in n.split() if t not in stopwords)

def names_equivalent(n1, n2):
    t1, t2 = normalize_team_name(n1), normalize_team_name(n2)
    if not t1 or not t2:
        return False
    if set(t1.split()) & set(t2.split()):
        return True
    if fuzz.ratio(t1, t2) >= 80 or fuzz.partial_ratio(t1, t2) >= 80:
        return True
    return t1 in t2 or t2 in t1

def teams_match(h1, a1, h2, a2):
    return names_equivalent(h1, h2) and names_equivalent(a1, a2)

# ---------- ONEFOOTBALL ----------
def fetch_onefootball_matches():
    logger.info("⚽ Fetching matches from OneFootball...")
    url = "https://onefootball.com/en/matches"
    headers = {"User-Agent": "Mozilla/5.0"}
    matches = []
    try:
        resp = requests.get(url, headers=headers, timeout=15)
        resp.raise_for_status()
        m = re.search(r'<script id="__NEXT_DATA__" type="application/json">({.*?})</script>', resp.text, re.DOTALL)
        if not m:
            logger.warning("No JSON found on OneFootball page")
            return []
        data = json.loads(m.group(1))
        containers = data.get("props", {}).get("pageProps", {}).get("containers", [])
        for c in containers:
            comp = c.get("type", {}).get("fullWidth", {}).get("component", {})
            if comp.get("contentType", {}).get("$case") == "matchCardsList":
                for mc in comp["contentType"]["matchCardsList"]["matchCards"]:
                    try:
                        competition = mc.get("trackingEvents", [None])[0].get("typedServerParameter", {}).get("competition", {}).get("value", "Unknown")
                    except Exception:
                        competition = "Unknown"
                    match_id = mc.get("matchId", "")
                    home = mc.get("homeTeam", {}).get("name", "Unknown")
                    away = mc.get("awayTeam", {}).get("name", "Unknown")
                    home_logo = mc.get("homeTeam", {}).get("imageObject", {}).get("path", "")
                    away_logo = mc.get("awayTeam", {}).get("imageObject", {}).get("path", "")
                    home_score = str(mc.get("homeTeam", {}).get("score") or "0")
                    away_score = str(mc.get("awayTeam", {}).get("score") or "0")

                    kickoff_utc = datetime.strptime(mc["kickoff"], "%Y-%m-%dT%H:%M:%SZ")
                    gmt3 = pytz.timezone("Africa/Nairobi")
                    kickoff_gmt3 = kickoff_utc.replace(tzinfo=pytz.utc).astimezone(gmt3)
                    kickoff_str = kickoff_gmt3.strftime("%Y-%m-%d %H:%M")

                    matches.append({
                        "match_id": match_id,
                        "home": home,
                        "away": away,
                        "competition": competition,
                        "kickoff": kickoff_str,
                        "home_logo": home_logo,
                        "away_logo": away_logo,
                        "home_score": home_score,
                        "away_score": away_score
                    })
        logger.info(f"✅ OneFootball: {len(matches)} matches")
    except Exception as e:
        logger.error(f"Error fetching OneFootball: {e}")
    return matches

# ---------- WHERESTHEMATCH ----------
def fetch_wheresthematch_matches():
    logger.info("📺 Fetching matches from WherestheMatch...")
    url = "https://www.wheresthematch.com/football-today/"
    headers = {"User-Agent": "Mozilla/5.0"}
    matches = []
    try:
        resp = requests.get(url, headers=headers, timeout=15)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")
        for row in soup.find_all("tr"):
            fx = row.find("td", class_="fixture-details")
            tm = row.find("td", class_="start-details")
            ch = row.find("td", class_="channel-details")
            if not fx or not tm:
                continue
            links = fx.find_all("a")
            if len(links) < 2:
                continue
            home, away = links[0].text.strip(), links[1].text.strip()
            comp = fx.find("span", class_="fixture-comp")
            comp = comp.get_text(" ", strip=True) if comp else "Unknown"
            time_span = tm.find("span", class_="time")
            kickoff = "Unknown"
            if time_span:
                try:
                    bst = pytz.timezone("Europe/London")
                    gmt3 = pytz.timezone("Africa/Nairobi")
                    bst_time = bst.localize(datetime.strptime(f"{datetime.today().date()} {time_span.text.strip()}", "%Y-%m-%d %H:%M"))
                    kickoff = bst_time.astimezone(gmt3).strftime("%Y-%m-%d %H:%M")
                except Exception:
                    pass
            channels = [img.get("title", "Unknown") for img in ch.find_all("img")] if ch else []
            matches.append({"home": home, "away": away, "competition": comp, "kickoff": kickoff, "channels": channels or ["Not specified"]})
        logger.info(f"✅ WherestheMatch: {len(matches)} matches")
    except Exception as e:
        logger.error(f"Error fetching WherestheMatch: {e}")
    return matches

# ---------- DADDYLIVE (EXTRA STREAM FILTERED) ----------
def fetch_daddylive_matches():
    logger.info("📡 Fetching matches from DaddyLive...")
    url = "https://daddylivestream.com/schedule/schedule-generated.php"
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Referer": "https://daddylivestream.com/",
        "Origin": "https://daddylivestream.com/"
    }
    matches = []
    try:
        resp = requests.get(url, headers=headers, timeout=15)
        resp.raise_for_status()
        data = resp.json()
        for _, cats in data.items():
            for sport, events in cats.items():
                if "soccer" not in sport.lower():
                    continue
                for e in events:
                    title = e.get("event", "")
                    if ":" not in title or "vs" not in title.lower():
                        continue
                    comp, fixture = title.split(":", 1)
                    parts = re.split(r"\s+vs\.?\s+", fixture, flags=re.I)
                    if len(parts) != 2:
                        continue
                    home, away = [p.strip() for p in parts]

                    def ch_extract(c_list):
                        out = []
                        for c in c_list:
                            if isinstance(c, dict):
                                out.append(c.get("channel_name", "Unknown"))
                            elif isinstance(c, str):
                                out.append(c)
                        return out

                    ch1 = ch_extract(e.get("channels", []))
                    ch2 = ch_extract(e.get("channels2", []))
                    all_ch = ch1 + ch2
                    if not all_ch or all("extra stream" in ch.lower() for ch in all_ch):
                        continue

                    matches.append({"home": home, "away": away, "competition": comp.strip(), "channels": all_ch})
        logger.info(f"✅ DaddyLive: {len(matches)} valid football matches")
    except Exception as e:
        logger.error(f"Error fetching DaddyLive: {e}")
    return matches

# ---------- ALLFOOTBALL ----------
def fetch_allfootball_matches():
    logger.info("🌍 Fetching matches from AllFootball...")
    url = "https://m.allfootballapp.com/matchs"
    scraper = cloudscraper.create_scraper()
    matches = []
    try:
        resp = scraper.get(url, timeout=15)
        resp.raise_for_status()
        text = resp.text
        idx = text.find('"matchListStore":')
        if idx == -1:
            return []
        snippet = "{" + text[idx:text.find('}</script>', idx)] + "}"
        data = json.loads(snippet)
        raw = data.get("matchListStore", {}).get("currentListData", [])
        gmt3 = pytz.timezone("Africa/Nairobi")
        today = datetime.now(gmt3).date()
        for m in raw:
            dt_str = f"{m.get('date_utc')} {m.get('time_utc','00:00:00')}"
            match_utc = pytz.utc.localize(datetime.strptime(dt_str, "%Y-%m-%d %H:%M:%S"))
            match_gmt3 = match_utc.astimezone(gmt3)
            if match_gmt3.date() != today:
                continue
            matches.append({
                "home": m.get("team_A_name", ""),
                "away": m.get("team_B_name", ""),
                "competition": m.get("competition_name", "Unknown"),
                "kickoff": match_gmt3.strftime("%Y-%m-%d %H:%M"),
                "home_logo": m.get("team_A_logo", ""),
                "away_logo": m.get("team_B_logo", ""),
                "home_score": str(m.get("fs_A", "0")),
                "away_score": str(m.get("fs_B", "0"))
            })
        logger.info(f"✅ AllFootball: {len(matches)} matches")
    except Exception as e:
        logger.error(f"Error fetching AllFootball: {e}")
    return matches

# ---------- MERGE ----------
def merge_matches():
    logger.info("🚀 Starting match merge...")
    onefootball = fetch_onefootball_matches()
    wtm = fetch_wheresthematch_matches()
    daddylive = fetch_daddylive_matches()
    allfootball = fetch_allfootball_matches()

    merged = []
    for om in onefootball:
        if is_banned_match(om["home"], om["away"], om["competition"]):
            continue

        channels = []
        for src in (wtm + daddylive + allfootball):
            if teams_match(om["home"], om["away"], src.get("home", ""), src.get("away", "")):
                channels.extend(src.get("channels", []))

        seen, clean = set(), []
        for ch in channels:
            if ch and ch.lower() not in seen:
                seen.add(ch.lower())
                clean.append(ch)

        om["channels"] = clean or ["Not specified"]
        merged.append(om)

    merged.sort(key=lambda m: datetime.strptime(m["kickoff"], "%Y-%m-%d %H:%M"))
    logger.info(f"🎯 Final merged matches: {len(merged)}")

    # Write to file (with emojis, UTF-8 safe)
    try:
        with open("matches.txt", "w", encoding="utf-8") as f:
            for m in merged:
                f.write(f"\U0001F3DF️ Match: {m['home']} Vs {m['away']}\n")
                f.write(f"\U0001F194 Match ID: {m.get('match_id', 'N/A')}\n")
                f.write(f"\U0001F552 Start: {m['kickoff']} (GMT+3)\n")
                f.write(f"\U0001F4CD Tournament: {m['competition']}\n")
                f.write(f"\U0001F4FA Channels: {', '.join(m['channels'])}\n")
                f.write(f"\U0001F5BC️ Home Logo: {m.get('home_logo', 'N/A')}\n")
                f.write(f"\U0001F5BC️ Away Logo: {m.get('away_logo', 'N/A')}\n")
                f.write("-" * 50 + "\n")
        logger.info(f"✅ Saved {len(merged)} matches to matches.txt")
    except Exception as e:
        logger.error(f"Error writing to matches.txt: {e}")

# ---------- MAIN ----------
if __name__ == "__main__":
    logger.info("=== ⚽ Starting matches.py ===")
    merge_matches()
    logger.info("=== ✅ Finished matches.py ===")
