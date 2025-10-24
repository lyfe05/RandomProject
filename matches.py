import requests
import re
import json
import unicodedata
import logging
from datetime import datetime, timedelta
from bs4 import BeautifulSoup
import pytz
from rapidfuzz import fuzz
import cloudscraper

# ---------- SETUP LOGGING ----------
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),  # Log to console
        logging.FileHandler('matches.log')  # Log to file
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
        logger.warning(f"banned.txt not found, continuing with empty ban list")
        return set()

BANNED_TOURNAMENTS_LOWER = load_banned_tournaments()

def is_banned_match(home: str, away: str, competition: str) -> bool:
    if not competition:
        competition = ""
    lname = competition.strip().lower()

    if lname in BANNED_TOURNAMENTS_LOWER:
        logger.debug(f"Match banned due to tournament: {competition}")
        return True

    if "women" in lname or "nwsl" in lname:
        logger.debug(f"Match banned due to women's tournament: {competition}")
        return True

    youth_patterns = ["u18", "u19", "u21", "u23", "youth", "reserve", "reserves", "academy"]
    if any(p in lname for p in youth_patterns):
        logger.debug(f"Match banned due to youth tournament: {competition}")
        return True

    if "women" in home.lower() or "women" in away.lower():
        logger.debug(f"Match banned due to women's team: {home} vs {away}")
        return True
    if any(p in home.lower() for p in youth_patterns) or any(p in away.lower() for p in youth_patterns):
        logger.debug(f"Match banned due to youth team: {home} vs {away}")
        return True

    return False

# ---------- HELPERS ----------
def normalize_team_name(name: str) -> str:
    if not name:
        return ""
    n = unicodedata.normalize("NFKD", name).encode("ascii", "ignore").decode("utf-8").lower()
    n = re.sub(r"[^a-z ]", " ", n)
    n = re.sub(r"\s+", " ", n).strip()
    stopwords = {"fc", "cf", "club", "the", "team", "deportivo"}
    tokens = [t for t in n.split() if t not in stopwords]
    return " ".join(tokens)

def names_equivalent(n1, n2):
    t1, t2 = normalize_team_name(n1), normalize_team_name(n2)
    if not t1 or not t2:
        return False
    s1, s2 = set(t1.split()), set(t2.split())
    if s1 & s2:
        return True
    if fuzz.ratio(t1, t2) >= 80:
        return True
    if fuzz.partial_ratio(t1, t2) >= 80:
        return True
    if t1 in t2 or t2 in t1:
        return True
    return False

def teams_match(h1, a1, h2, a2):
    return names_equivalent(h1, h2) and names_equivalent(a1, a2)

# ---------- ONEFOOTBALL ----------
def fetch_onefootball_matches():
    logger.info("Fetching matches from OneFootball...")
    url = "https://onefootball.com/en/matches"
    headers = {'User-Agent': 'Mozilla/5.0'}
    matches = []
    try:
        response = requests.get(url, headers=headers, timeout=15)
        response.raise_for_status()
        json_pattern = r'<script id="__NEXT_DATA__" type="application/json">({.*?})</script>'
        match = re.search(json_pattern, response.text, re.DOTALL)
        if not match:
            logger.warning("No JSON data found in OneFootball response")
            return matches
        json_data = json.loads(match.group(1))
        containers = json_data.get("props", {}).get("pageProps", {}).get("containers", [])
        for container in containers:
            comp = container.get("type", {}).get("fullWidth", {}).get("component", {})
            if comp.get("contentType", {}).get("$case") == "matchCardsList":
                for m in comp["contentType"]["matchCardsList"]["matchCards"]:
                    try:
                        competition = m.get("trackingEvents", [None])[0].get("typedServerParameter", {}).get("competition", {}).get("value", "Unknown Tournament")
                    except Exception:
                        competition = "Unknown Tournament"

                    # Extract match ID (try two sources)
                    match_id = m.get("matchId") or m.get("trackingEvents", [{}])[0].get("typedServerParameter", {}).get("match_id", {}).get("value", "")

                    home_team = m.get("homeTeam", {}).get("name", "Unknown")
                    away_team = m.get("awayTeam", {}).get("name", "Unknown")
                    home_logo = m.get("homeTeam", {}).get("imageObject", {}).get("path", "No logo")
                    away_logo = m.get("awayTeam", {}).get("imageObject", {}).get("path", "No logo")
                    home_score = str(m.get("homeTeam", {}).get("score") or "0")
                    away_score = str(m.get("awayTeam", {}).get("score") or "0")
                    
                    # Convert UTC to GMT+3 (East Africa Time)
                    kickoff_utc = datetime.strptime(m["kickoff"], "%Y-%m-%dT%H:%M:%SZ")
                    gmt3 = pytz.timezone('Africa/Nairobi')  # GMT+3
                    kickoff_gmt3 = kickoff_utc.replace(tzinfo=pytz.utc).astimezone(gmt3)
                    kickoff_str = kickoff_gmt3.strftime("%Y-%m-%d %H:%M")
                    
                    matches.append({
                        "match_id": match_id,
                        "home": home_team,
                        "away": away_team,
                        "competition": competition,
                        "kickoff": kickoff_str,
                        "home_logo": home_logo,
                        "away_logo": away_logo,
                        "home_score": home_score,
                        "away_score": away_score
                    })
        logger.info(f"Successfully fetched {len(matches)} matches from OneFootball")
    except Exception as e:
        logger.error(f"Error fetching OneFootball: {e}")
    return matches

# ---------- WHERESTHEMATCH ----------
def fetch_wheresthematch_matches():
    logger.info("Fetching matches from WherestheMatch...")
    url = "https://www.wheresthematch.com/football-today/"
    headers = {'User-Agent': 'Mozilla/5.0'}
    matches = []
    try:
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()
    except requests.RequestException as e:
        logger.error(f"Error fetching WherestheMatch: {e}")
        return matches
    soup = BeautifulSoup(response.text, 'html.parser')
    for row in soup.find_all('tr'):
        fixture_cell = row.find('td', class_='fixture-details')
        time_cell = row.find('td', class_='start-details')
        channels_cell = row.find('td', class_='channel-details')
        if not fixture_cell or not time_cell:
            continue
        team_links = fixture_cell.find_all('a')
        if len(team_links) >= 2:
            home_team = team_links[0].get_text(strip=True)
            away_team = team_links[1].get_text(strip=True)
        else:
            continue
        comp_span = fixture_cell.find('span', class_='fixture-comp')
        competition = comp_span.get_text(" ", strip=True) if comp_span else "Unknown competition"
        time_span = time_cell.find('span', class_='time')
        kickoff_str = "Unknown"
        if time_span:
            try:
                # Convert BST to GMT+3
                bst = pytz.timezone('Europe/London')
                gmt3 = pytz.timezone('Africa/Nairobi')
                bst_time = bst.localize(datetime.strptime(f"{datetime.today().date()} {time_span.get_text(strip=True)}", "%Y-%m-%d %H:%M"))
                kickoff_str = bst_time.astimezone(gmt3).strftime("%Y-%m-%d %H:%M")
            except Exception as e:
                logger.warning(f"Could not parse time for {home_team} vs {away_team}: {e}")
                kickoff_str = "Unknown"
        channels = []
        if channels_cell:
            channels = [img.get('title', 'Unknown Channel') for img in channels_cell.find_all('img', class_='channel')]
        if not channels:
            channels = ["Not specified"]
        matches.append({
            "home": home_team,
            "away": away_team,
            "competition": competition,
            "kickoff": kickoff_str,
            "channels": channels
        })
    logger.info(f"Successfully fetched {len(matches)} matches from WherestheMatch")
    return matches

# ---------- DADDYLIVE ----------
def fetch_daddylive_matches():
    logger.info("Fetching matches from DaddyLive (HTML via dlhd.dad)...")
    import pycurl
    from io import BytesIO

    URL = "https://dlhd.dad/"
    UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
          "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36")

    def fetch_html():
        buf = BytesIO()
        c = pycurl.Curl()
        c.setopt(c.URL, URL)
        c.setopt(c.HTTPHEADER, [f"User-Agent: {UA}", "Accept: text/html,*/*;q=0.8"])
        c.setopt(c.WRITEDATA, buf)
        c.setopt(c.SSL_VERIFYPEER, 0)
        c.setopt(c.SSL_VERIFYHOST, 0)
        c.setopt(c.FOLLOWLOCATION, 1)
        c.setopt(c.TIMEOUT, 0)
        try:
            c.perform()
            if c.getinfo(c.RESPONSE_CODE) != 200:
                raise RuntimeError("non-200 response")
        finally:
            c.close()
        return buf.getvalue().decode("utf-8", errors="ignore")

    def html_time_to_gmt3(time_str: str, base_date: datetime) -> str:
        """Convert UK time (GMT/BST) to Africa/Nairobi GMT+3"""
        try:
            h, m = map(int, time_str.split(":"))
            uk = base_date.replace(hour=h, minute=m, second=0, microsecond=0,
                                   tzinfo=timezone.utc)
            return uk.astimezone(pytz.timezone("Africa/Nairobi")).strftime("%Y-%m-%d %H:%M")
        except Exception:
            return time_str

    BLOCKED_KEYWORDS = [
        "tennis", "basketball", "hockey", "volleyball", "handball",
        "table tennis", "snooker", "darts", "esport", "counter-strike",
        "mlb", "nfl", "rugby", "cricket", "boxing", "mma", "ufc",
        "wwe", "badminton", "futsal", "cycling", "motogp", "formula",
        "nascar", "golf", "chess", "kabaddi"
    ]

    matches = []
    seen = set()

    try:
        html = fetch_html()
        soup = BeautifulSoup(html, "html5lib")

        for day_block in soup.select("div.schedule__day"):
            day_title = day_block.select_one(".schedule__dayTitle")
            if not day_title:
                continue

            m = re.search(r"(\d{1,2})\w{2}\s+(\w+)\s+(\d{4})", day_title.get_text(" ", strip=True))
            if not m:
                continue

            day, month_abbr, year = m.groups()
            base_date = datetime(int(year), datetime.strptime(month_abbr[:3], "%b").month, int(day))

            for row in day_block.select("div.schedule__event"):
                header = row.select_one("div.schedule__eventHeader")
                if not header:
                    continue

                time_raw = header.select_one("span.schedule__time").get_text(strip=True)
                title_raw = header.get("data-title") or header.select_one("span.schedule__eventTitle").get_text(strip=True)
                title_raw = re.sub(r"\s*\(?\b\d{1,2}:\d{2}\)?\s*$", "", title_raw)

                if " vs " not in title_raw.lower():
                    continue

                comp, _, fixture = title_raw.partition(":")
                comp = comp.strip() or "Unknown Competition"
                fixture = fixture.strip()

                # Skip unwanted sports
                text = f"{comp.lower()} {fixture.lower()}"
                if any(bad in text for bad in BLOCKED_KEYWORDS):
                    continue

                teams = re.split(r"\s+vs\.?\s+", fixture, flags=re.I)
                if len(teams) != 2:
                    continue
                home, away = [t.strip() for t in teams]

                channels = [a.get("title") or a.get_text(strip=True)
                            for a in row.select("div.schedule__channels a")]
                if not channels:
                    channels = ["Not specified"]

                # Skip fake "Extra Stream" listings
                if all("extra stream" in ch.lower() for ch in channels):
                    continue

                key = (home.lower(), away.lower(), comp.lower())
                if key in seen:
                    continue
                seen.add(key)

                kickoff = html_time_to_gmt3(time_raw, base_date)
                matches.append({
                    "home": home,
                    "away": away,
                    "competition": comp,
                    "kickoff": kickoff,
                    "channels": channels
                })

        logger.info(f"Successfully fetched {len(matches)} matches from DaddyLive (HTML)")
    except Exception as e:
        logger.error(f"Error scraping DaddyLive: {e}")

    return matches

# ---------- ALLFOOTBALL ----------
def fetch_allfootball_matches():
    logger.info("Fetching matches from AllFootball...")
    url = "https://m.allfootballapp.com/matchs"
    headers = {"User-Agent": "Mozilla/5.0"}
    matches = []
    scraper = cloudscraper.create_scraper()
    try:
        resp = scraper.get(url, headers=headers, timeout=15)
        resp.raise_for_status()
        text = resp.text
        idx = text.find('"matchListStore":')
        if idx == -1:
            logger.warning("No matchListStore found in AllFootball response")
            return matches
        snippet = text[idx:]
        end_idx = snippet.find('}</script>')
        if end_idx != -1:
            snippet = snippet[:end_idx+1]
        snippet = "{" + snippet
        data = json.loads(snippet)
        raw_matches = data.get("matchListStore", {}).get("currentListData", [])
        gmt3 = pytz.timezone("Africa/Nairobi")
        today_gmt3 = datetime.now(gmt3).date()
        for m in raw_matches:
            try:
                dt_str = f"{m.get('date_utc','')} {m.get('time_utc','00:00:00')}"
                match_utc = datetime.strptime(dt_str, "%Y-%m-%d %H:%M:%S")
                match_utc = pytz.utc.localize(match_utc)
                match_gmt3 = match_utc.astimezone(gmt3)
                if match_gmt3.date() != today_gmt3:
                    continue
                kickoff_str = match_gmt3.strftime("%Y-%m-%d %H:%M")
                matches.append({
                    "home": m.get("team_A_name", "Unknown"),
                    "away": m.get("team_B_name", "Unknown"),
                    "competition": m.get("competition_name", "Unknown Tournament"),
                    "kickoff": kickoff_str,
                    "home_logo": m.get("team_A_logo", "No logo"),
                    "away_logo": m.get("team_B_logo", "No logo"),
                    "home_score": str(m.get("fs_A", "0")),
                    "away_score": str(m.get("fs_B", "0"))
                })
            except Exception as e:
                logger.debug(f"Skipping match due to parsing error: {e}")
                continue
        logger.info(f"Successfully fetched {len(matches)} matches from AllFootball")
    except Exception as e:
        logger.error(f"Error fetching AllFootball: {e}")
    return matches

# ---------- MERGE AND SAVE ----------
def merge_matches():
    logger.info("Starting match merging process...")
    
    onefootball = fetch_onefootball_matches()
    wtm = fetch_wheresthematch_matches()
    daddylive = fetch_daddylive_matches()
    allfootball = fetch_allfootball_matches()
    
    logger.info(f"Source counts - OneFootball: {len(onefootball)}, WherestheMatch: {len(wtm)}, DaddyLive: {len(daddylive)}, AllFootball: {len(allfootball)}")
    
    merged = []

    # ---------- FIRST PASS: build allowed tournaments ----------
    allowed_tournaments = set()
    for om in onefootball:
        if is_banned_match(om.get("home",""), om.get("away",""), om.get("competition","")):
            continue

        wtm_matches = [wm for wm in wtm if teams_match(om["home"], om["away"], wm["home"], wm["away"])]
        af_matches = [am for am in allfootball if teams_match(om["home"], om["away"], am["home"], am["away"])]

        if wtm_matches or af_matches:
            allowed_tournaments.add(om["competition"].lower())

    logger.info(f"Found {len(allowed_tournaments)} allowed tournaments")

    # ---------- SECOND PASS: build final matches ----------
    for om in onefootball:
        if is_banned_match(om.get("home",""), om.get("away",""), om.get("competition","")):
            continue

        wtm_matches = [wm for wm in wtm if teams_match(om["home"], om["away"], wm["home"], wm["away"])]
        af_matches = [am for am in allfootball if teams_match(om["home"], om["away"], am["home"], am["away"])]
        channels = []

        if wtm_matches:
            for wm in wtm_matches:
                channels.extend(wm.get("channels", []))
            for dm in daddylive:
                if teams_match(om["home"], om["away"], dm["home"], dm["away"]):
                    channels.extend(dm.get("channels", []))

        elif af_matches:
            for dm in daddylive:
                if teams_match(om["home"], om["away"], dm["home"], dm["away"]):
                    channels.extend(dm.get("channels", []))

        elif om["competition"].lower() in allowed_tournaments:
            for dm in daddylive:
                if teams_match(om["home"], om["away"], dm["home"], dm["away"]):
                    channels.extend(dm.get("channels", []))
        else:
            continue

        seen, clean_channels = set(), []
        for ch in channels:
            key = (ch or "").strip().lower()
            if key and key not in seen:
                seen.add(key)
                clean_channels.append(ch)

        merged.append({**om, "channels": clean_channels})

    def kickoff_key(match):
        try:
            return datetime.strptime(match["kickoff"], "%Y-%m-%d %H:%M")
        except Exception:
            return datetime.max
    merged.sort(key=kickoff_key)

    logger.info(f"Final merged matches: {len(merged)}")

    # Write to matches.txt
    try:
        with open("matches.txt", "w", encoding="utf-8") as f:
            for m in merged:
                f.write(f"🏟️ Match: {m['home']} Vs {m['away']}\n")
                f.write(f"🆔 Match ID: {m.get('match_id', 'N/A')}\n")
                f.write(f"🕒 Start: {m['kickoff']} (GMT+3)\n")
                f.write(f"📍 Tournament: {m['competition']}\n")
                f.write(f"📺 Channels: {', '.join(m['channels']) if m['channels'] else 'Not specified'}\n")
                f.write(f"🖼️ Home Logo: {m.get('home_logo', 'N/A')}\n")
                f.write(f"🖼️ Away Logo: {m.get('away_logo', 'N/A')}\n")
                f.write("-" * 50 + "\n")
        logger.info(f"Successfully wrote {len(merged)} matches to matches.txt")
    except Exception as e:
        logger.error(f"Error writing to matches.txt: {e}")

if __name__ == "__main__":
    logger.info("=== Starting matches.py ===")
    merge_matches()
    logger.info("=== Finished matches.py ===")
