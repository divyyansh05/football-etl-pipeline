"""
FotMob scraper constants.

All team IDs were discovered via __NEXT_DATA__ JSON extraction from
www.fotmob.com/teams/{id}/squad/{slug} pages (verified 2026-04-01).

IMPORTANT: These are FotMob website team IDs, NOT api.fotmob.com IDs.
The api.fotmob.com endpoint now requires x-fm-req HMAC auth and must not
be used. The website HTML __NEXT_DATA__ approach requires no auth.
"""

# ── League IDs ────────────────────────────────────────────────────────────────
# FotMob league IDs (embedded in league page URLs: /leagues/{id}/...)
FOTMOB_LEAGUE_IDS = {
    "Premier League": 47,
    "La Liga": 87,
    "Serie A": 55,
    "Bundesliga": 54,
    "Ligue 1": 239,
}

# ── Team IDs ──────────────────────────────────────────────────────────────────
# Discovered from __NEXT_DATA__ table.data.table.all[] on squad pages.
# Each entry: fotmob_team_id -> (fotmob_name, url_slug)
FOTMOB_TEAM_IDS = {
    "Premier League": {
        8678: ("AFC Bournemouth", "afc-bournemouth"),
        9825: ("Arsenal", "arsenal"),
        10252: ("Aston Villa", "aston-villa"),
        9937: ("Brentford", "brentford"),
        10204: ("Brighton & Hove Albion", "brighton-hove-albion"),
        8191: ("Burnley", "burnley"),
        8455: ("Chelsea", "chelsea"),
        9826: ("Crystal Palace", "crystal-palace"),
        8668: ("Everton", "everton"),
        9879: ("Fulham", "fulham"),
        8463: ("Leeds United", "leeds-united"),
        8650: ("Liverpool", "liverpool"),
        8456: ("Manchester City", "manchester-city"),
        10260: ("Manchester United", "manchester-united"),
        10261: ("Newcastle United", "newcastle-united"),
        10203: ("Nottingham Forest", "nottingham-forest"),
        8472: ("Sunderland", "sunderland"),
        8586: ("Tottenham Hotspur", "tottenham-hotspur"),
        8654: ("West Ham United", "west-ham-united"),
        8602: ("Wolverhampton Wanderers", "wolverhampton-wanderers"),
    },
    "La Liga": {
        8315: ("Athletic Club", "athletic-club"),
        9906: ("Atletico Madrid", "atletico-madrid"),
        8634: ("Barcelona", "barcelona"),
        9910: ("Celta Vigo", "celta-vigo"),
        9866: ("Deportivo Alaves", "deportivo-alaves"),
        10268: ("Elche", "elche"),
        8558: ("Espanyol", "espanyol"),
        8305: ("Getafe", "getafe"),
        7732: ("Girona", "girona"),
        8581: ("Levante", "levante"),
        8661: ("Mallorca", "mallorca"),
        8371: ("Osasuna", "osasuna"),
        8370: ("Rayo Vallecano", "rayo-vallecano"),
        8603: ("Real Betis", "real-betis"),
        8633: ("Real Madrid", "real-madrid"),
        8670: ("Real Oviedo", "real-oviedo"),
        8560: ("Real Sociedad", "real-sociedad"),
        8302: ("Sevilla", "sevilla"),
        10267: ("Valencia", "valencia"),
        10205: ("Villarreal", "villarreal"),
    },
    "Serie A": {
        8524: ("Atalanta", "atalanta"),
        9857: ("Bologna", "bologna"),
        8529: ("Cagliari", "cagliari"),
        10171: ("Como", "como"),
        7801: ("Cremonese", "cremonese"),
        8535: ("Fiorentina", "fiorentina"),
        10233: ("Genoa", "genoa"),
        9876: ("Hellas Verona", "hellas-verona"),
        8636: ("Inter", "inter"),
        9885: ("Juventus", "juventus"),
        8543: ("Lazio", "lazio"),
        9888: ("Lecce", "lecce"),
        8564: ("Milan", "milan"),
        9875: ("Napoli", "napoli"),
        10167: ("Parma", "parma"),
        6479: ("Pisa", "pisa"),
        8686: ("Roma", "roma"),
        7943: ("Sassuolo", "sassuolo"),
        9804: ("Torino", "torino"),
        8600: ("Udinese", "udinese"),
    },
    "Bundesliga": {
        8722: ("1. FC Köln", "1-fc-koln"),
        8406: ("Augsburg", "augsburg"),
        8178: ("Bayer Leverkusen", "bayer-leverkusen"),
        9823: ("Bayern München", "fc-bayern-munchen"),
        9789: ("Borussia Dortmund", "borussia-dortmund"),
        9788: ("Borussia Mönchengladbach", "borussia-monchengladbach"),
        9810: ("Eintracht Frankfurt", "eintracht-frankfurt"),
        94937: ("FC Heidenheim", "fc-heidenheim"),
        8358: ("Freiburg", "freiburg"),
        9790: ("Hamburger SV", "hamburger-sv"),
        8226: ("Hoffenheim", "hoffenheim"),
        9905: ("Mainz 05", "mainz-05"),
        178475: ("RB Leipzig", "rb-leipzig"),
        8152: ("St. Pauli", "st-pauli"),
        8149: ("Union Berlin", "union-berlin"),
        10269: ("VfB Stuttgart", "vfb-stuttgart"),
        8697: ("Werder Bremen", "werder-bremen"),
        8721: ("Wolfsburg", "wolfsburg"),
    },
    "Ligue 1": {
        8121: ("Angers", "angers"),
        8583: ("Auxerre", "auxerre"),
        8521: ("Brest", "brest"),
        9746: ("Le Havre", "le-havre"),
        8588: ("Lens", "lens"),
        8639: ("Lille", "lille"),
        8689: ("Lorient", "lorient"),
        9748: ("Lyon", "lyon"),
        8592: ("Marseille", "marseille"),
        8550: ("Metz", "metz"),
        9829: ("Monaco", "monaco"),
        9830: ("Nantes", "nantes"),
        9831: ("Nice", "nice"),
        6379: ("Paris FC", "paris-fc"),
        9847: ("Paris Saint-Germain", "paris-saint-germain"),
        9851: ("Rennes", "rennes"),
        9848: ("Strasbourg", "strasbourg"),
        9941: ("Toulouse", "toulouse"),
    },
}

# ── Position mapping ──────────────────────────────────────────────────────────
# FotMob role.key values → canonical position_group used in DB.
# Derived from observed squad data (verified 2026-04-01).
FOTMOB_ROLE_TO_POSITION = {
    "keeper_long": "GK",
    "defender_long": "DEF",
    "midfielder_long": "MID",
    "attacker_long": "FWD",
    # Fallback variants seen in some squads
    "keeper": "GK",
    "defender": "DEF",
    "midfielder": "MID",
    "attacker": "FWD",
    "forward": "FWD",
}

# ── Team name map ─────────────────────────────────────────────────────────────
# Maps FotMob team name → DB team_name for cases where they differ.
# Only include entries that actually differ; exact matches need no entry.
# DB names are the canonical SofaScore names inserted by SofaScoreETL.
TEAM_NAME_MAP = {
    # Premier League
    "Wolverhampton Wanderers": "Wolverhampton",
    "AFC Bournemouth": "Bournemouth",

    # La Liga
    "Atletico Madrid": "Atlético Madrid",
    "Deportivo Alaves": "Deportivo Alavés",
    "Girona": "Girona FC",
    "Levante": "Levante UD",
    # Real Oviedo not in DB (relegated / not in any SofaScore season)

    # Serie A — names match DB exactly (Inter, Milan, Roma, etc.)

    # Bundesliga — most teams not in DB yet (no SofaScore data for Bundesliga)
    # Map what we can for future compatibility
    "Bayer Leverkusen": "Bayer 04 Leverkusen",
    "Bayern München": "FC Bayern München",
    "Wolfsburg": "VfL Wolfsburg",
    "Hoffenheim": "TSG Hoffenheim",

    # Ligue 1 — most teams not in DB yet (no SofaScore data for Ligue 1)
    # Map what we can
    "Lyon": "Olympique Lyonnais",
    "Marseille": "Olympique de Marseille",
    "Monaco": "AS Monaco",
    "Lens": "RC Lens",
    "Paris Saint-Germain": "Paris Saint-Germain",  # exact match
    "Lille": "Lille",  # exact match
}

# Rate limit between requests (seconds)
RATE_LIMIT_SECONDS = 3.0
