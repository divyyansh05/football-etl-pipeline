# API-Football Data Collection Guide

## Quick Start

### 1. Get Your Free API Key

1. Go to https://rapidapi.com/api-sports/api/api-football
2. Sign up for free account
3. Subscribe to the **FREE tier** (100 requests/day)
4. Copy your API key

### 2. Set Your API Key

**Option A: Environment Variable (Recommended)**
```bash
export API_FOOTBALL_KEY="your_api_key_here"
```

**Option B: Enter When Prompted**
The script will ask for your key if not set.

### 3. Collect Data

```bash
python collect_premier_league.py
```

This will:
- Fetch Premier League 2024-25 standings
- Collect all matches/fixtures
- Get top scorers and player stats
- Load everything into the database

**Expected API Calls**: ~10-15 requests (well within free tier limit)

## What Data is Collected

### Teams (20 teams)
- Team names
- Stadiums
- League standings

### Team Season Stats
- Matches played, wins, draws, losses
- Goals for/against
- Points and league position

### Matches (~200+ fixtures)
- Match dates and venues
- Scores
- Match status (completed/scheduled)
- Referees

### Player Stats (Top scorers)
- Goals and assists
- Shots and shots on target
- Minutes played
- Cards

## After Collection

### View in UI
```bash
python server/app.py
```

Then open: http://localhost:5001

### Query Database
```bash
docker exec football_etl_db psql -U postgres -d football_data
```

```sql
-- View standings
SELECT t.team_name, tss.points, tss.league_position
FROM team_season_stats tss
JOIN teams t ON tss.team_id = t.team_id
JOIN seasons s ON tss.season_id = s.season_id
WHERE s.season_name = '2024-25'
ORDER BY tss.league_position;

-- View top scorers
SELECT p.player_name, t.team_name, pss.goals, pss.assists
FROM player_season_stats pss
JOIN players p ON pss.player_id = p.player_id
JOIN teams t ON pss.team_id = t.team_id
JOIN seasons s ON pss.season_id = s.season_id
WHERE s.season_name = '2024-25'
ORDER BY pss.goals DESC
LIMIT 10;
```

## API-Football Free Tier Limits

- **100 requests/day**
- **1 request/second** rate limit
- All major leagues included
- Real-time data

## Troubleshooting

### "Invalid API Key"
- Check your API key is correct
- Make sure you've subscribed to the API on RapidAPI

### "Rate Limit Exceeded"
- Wait 24 hours for limit reset
- Or upgrade to paid tier

### "No Data Returned"
- Check league ID is correct (39 for Premier League)
- Verify season year (2024 for 2024-25)

## Alternative: Other Leagues

To collect other leagues, modify the script:

```python
# La Liga
stats = etl.process_league_season('la-liga', 2024)

# Serie A
stats = etl.process_league_season('serie-a', 2024)

# Bundesliga
stats = etl.process_league_season('bundesliga', 2024)

# Ligue 1
stats = etl.process_league_season('ligue-1', 2024)
```

## Benefits Over Web Scraping

✅ **Legal and reliable**
✅ **No 403 errors or blocks**
✅ **Structured, clean data**
✅ **Real-time updates**
✅ **Free tier available**
✅ **Well-documented API**
