import sys
import os
import re
import sqlite3
from pathlib import Path
from datetime import datetime, timedelta

# Add project root to path
sys.path.append(str(Path(__file__).parent.parent))

from flask import Flask, render_template, jsonify, request
from database.connection import get_db

app = Flask(__name__)

# Absolute project root so file paths work regardless of CWD
PROJECT_ROOT = Path(__file__).parent.parent

@app.route('/')
def index():
    """Render the dashboard home page."""
    return render_template('index.html', active_page='dashboard')

@app.route('/api/stats')
def get_stats():
    """Get overall database statistics."""
    db = get_db()

    # Convert Row objects to dicts for serialization
    sources_data = db.execute_query("SELECT source_name, reliability_score FROM data_sources", fetch=True)
    sources_list = [{'name': row[0], 'score': row[1]} for row in sources_data]

    stats = {
        'leagues': db.execute_query("SELECT COUNT(*) FROM leagues", fetch=True)[0][0],
        'teams': db.execute_query("SELECT COUNT(*) FROM teams", fetch=True)[0][0],
        'players': db.execute_query("SELECT COUNT(*) FROM players", fetch=True)[0][0],
        'matches': db.execute_query("SELECT COUNT(*) FROM matches", fetch=True)[0][0],
        'player_season_stats': db.execute_query("SELECT COUNT(*) FROM player_season_stats", fetch=True)[0][0],
        'team_season_stats': db.execute_query("SELECT COUNT(*) FROM team_season_stats", fetch=True)[0][0],
        'sources': sources_list
    }
    return jsonify(stats)


@app.route('/api/stats/detailed')
def get_detailed_stats():
    """Get detailed database statistics with league and season breakdown."""
    db = get_db()

    # League breakdown with data coverage
    league_stats = db.execute_query("""
        SELECT
            l.league_id,
            l.league_name,
            l.country,
            COUNT(DISTINCT t.team_id) as teams,
            COUNT(DISTINCT m.match_id) as matches,
            COUNT(DISTINCT pss.player_id) as players_with_stats,
            COUNT(DISTINCT pss.player_season_stat_id) as stat_records
        FROM leagues l
        LEFT JOIN teams t ON l.league_id = t.league_id
        LEFT JOIN matches m ON l.league_id = m.league_id
        LEFT JOIN player_season_stats pss ON l.league_id = pss.league_id
        GROUP BY l.league_id, l.league_name, l.country
        ORDER BY matches DESC
    """, fetch=True)

    leagues = [{
        'id': row[0],
        'name': row[1],
        'country': row[2],
        'teams': row[3],
        'matches': row[4],
        'players_with_stats': row[5],
        'stat_records': row[6],
        'data_completeness': min(100, int((row[5] / max(row[3] * 25, 1)) * 100)) if row[3] else 0
    } for row in league_stats]

    # Season breakdown
    season_stats = db.execute_query("""
        SELECT
            s.season_id,
            s.season_name,
            COUNT(DISTINCT m.match_id) as matches,
            COUNT(DISTINCT pss.player_id) as players_with_stats,
            COUNT(DISTINCT pss.player_season_stat_id) as stat_records
        FROM seasons s
        LEFT JOIN matches m ON s.season_id = m.season_id
        LEFT JOIN player_season_stats pss ON s.season_id = pss.season_id
        GROUP BY s.season_id, s.season_name
        ORDER BY s.season_name DESC
    """, fetch=True)

    seasons = [{
        'id': row[0],
        'name': row[1],
        'matches': row[2],
        'players_with_stats': row[3],
        'stat_records': row[4]
    } for row in season_stats]

    # Top scorers across all data
    top_scorers = db.execute_query("""
        SELECT
            p.player_id,
            p.player_name,
            p.position,
            t.team_name,
            l.league_name,
            SUM(pss.goals) as total_goals,
            SUM(pss.assists) as total_assists,
            SUM(pss.minutes) as total_minutes,
            s.season_name
        FROM player_season_stats pss
        JOIN players p ON pss.player_id = p.player_id
        JOIN teams t ON pss.team_id = t.team_id
        JOIN leagues l ON pss.league_id = l.league_id
        JOIN seasons s ON pss.season_id = s.season_id
        WHERE pss.goals > 0
        GROUP BY p.player_id, p.player_name, p.position, t.team_name, l.league_name, s.season_name
        ORDER BY total_goals DESC
        LIMIT 15
    """, fetch=True)

    scorers = [{
        'id': row[0],
        'name': row[1],
        'position': row[2],
        'team': row[3],
        'league': row[4],
        'goals': row[5],
        'assists': row[6],
        'minutes': row[7],
        'season': row[8]
    } for row in top_scorers]

    # Top assisters
    top_assisters = db.execute_query("""
        SELECT
            p.player_id,
            p.player_name,
            p.position,
            t.team_name,
            l.league_name,
            SUM(pss.assists) as total_assists,
            SUM(pss.goals) as total_goals,
            SUM(pss.minutes) as total_minutes,
            s.season_name
        FROM player_season_stats pss
        JOIN players p ON pss.player_id = p.player_id
        JOIN teams t ON pss.team_id = t.team_id
        JOIN leagues l ON pss.league_id = l.league_id
        JOIN seasons s ON pss.season_id = s.season_id
        WHERE pss.assists > 0
        GROUP BY p.player_id, p.player_name, p.position, t.team_name, l.league_name, s.season_name
        ORDER BY total_assists DESC
        LIMIT 15
    """, fetch=True)

    assisters = [{
        'id': row[0],
        'name': row[1],
        'position': row[2],
        'team': row[3],
        'league': row[4],
        'assists': row[5],
        'goals': row[6],
        'minutes': row[7],
        'season': row[8]
    } for row in top_assisters]

    # Recent data updates (matches by month)
    monthly_matches = db.execute_query("""
        SELECT
            to_char(match_date, 'YYYY-MM') as month,
            COUNT(*) as matches
        FROM matches
        WHERE match_date IS NOT NULL
        GROUP BY to_char(match_date, 'YYYY-MM')
        ORDER BY month DESC
        LIMIT 12
    """, fetch=True)

    monthly_data = [{
        'month': row[0],
        'matches': row[1]
    } for row in monthly_matches]

    return jsonify({
        'leagues': leagues,
        'seasons': seasons,
        'top_scorers': scorers,
        'top_assisters': assisters,
        'monthly_matches': monthly_data
    })


@app.route('/api/players/top')
def get_top_players():
    """Get top players with comprehensive stats."""
    db = get_db()
    league = request.args.get('league', '')
    season = request.args.get('season', '')
    sort_by = request.args.get('sort', 'goals')
    limit = min(int(request.args.get('limit', 50)), 100)

    # Build query with filters
    where_clauses = []
    params = {}

    if league:
        where_clauses.append("l.league_name = :league")
        params['league'] = league
    if season:
        where_clauses.append("s.season_name = :season")
        params['season'] = season

    where_sql = "WHERE " + " AND ".join(where_clauses) if where_clauses else ""

    # Determine sort column
    sort_columns = {
        'goals': 'pss.goals DESC',
        'assists': 'pss.assists DESC',
        'minutes': 'pss.minutes DESC',
        'xg': 'pss.xg DESC NULLS LAST',
        'goals_per_90': '(pss.goals::float / NULLIF(pss.minutes, 0) * 90) DESC NULLS LAST'
    }
    order_by = sort_columns.get(sort_by, 'pss.goals DESC')

    query = f"""
        SELECT
            p.player_id,
            p.player_name,
            p.position,
            p.nationality,
            t.team_name,
            l.league_name,
            s.season_name,
            pss.matches_played,
            pss.starts,
            pss.minutes,
            pss.goals,
            pss.assists,
            pss.xg,
            pss.xag,
            pss.shots,
            pss.key_passes,
            ROUND((pss.goals::numeric / NULLIF(pss.minutes, 0) * 90)::numeric, 2) as goals_per_90,
            ROUND((pss.assists::numeric / NULLIF(pss.minutes, 0) * 90)::numeric, 2) as assists_per_90
        FROM player_season_stats pss
        JOIN players p ON pss.player_id = p.player_id
        JOIN teams t ON pss.team_id = t.team_id
        JOIN leagues l ON pss.league_id = l.league_id
        JOIN seasons s ON pss.season_id = s.season_id
        {where_sql}
        ORDER BY {order_by}
        LIMIT :limit
    """
    params['limit'] = limit

    players = db.execute_query(query, params, fetch=True)

    return jsonify([{
        'id': row[0],
        'name': row[1],
        'position': row[2],
        'nationality': row[3],
        'team': row[4],
        'league': row[5],
        'season': row[6],
        'matches': row[7],
        'starts': row[8],
        'minutes': row[9],
        'goals': row[10],
        'assists': row[11],
        'xg': float(row[12]) if row[12] else None,
        'xag': float(row[13]) if row[13] else None,
        'shots': row[14],
        'key_passes': row[15],
        'goals_per_90': float(row[16]) if row[16] else None,
        'assists_per_90': float(row[17]) if row[17] else None
    } for row in players])


@app.route('/api/leagues')
def get_leagues():
    """Get all leagues with summary stats."""
    db = get_db()

    leagues = db.execute_query("""
        SELECT
            l.league_id,
            l.league_name,
            l.country,
            COUNT(DISTINCT t.team_id) as teams,
            COUNT(DISTINCT m.match_id) as matches
        FROM leagues l
        LEFT JOIN teams t ON l.league_id = t.league_id
        LEFT JOIN matches m ON l.league_id = m.league_id
        GROUP BY l.league_id, l.league_name, l.country
        ORDER BY l.league_name
    """, fetch=True)

    return jsonify([{
        'id': row[0],
        'name': row[1],
        'country': row[2],
        'teams': row[3],
        'matches': row[4]
    } for row in leagues])


@app.route('/api/seasons')
def get_seasons():
    """Get all seasons."""
    db = get_db()

    seasons = db.execute_query("""
        SELECT season_id, season_name
        FROM seasons
        ORDER BY season_name DESC
    """, fetch=True)

    return jsonify([{
        'id': row[0],
        'name': row[1]
    } for row in seasons])


@app.route('/api/health')
def get_data_health():
    """Get data health and completeness indicators."""
    db = get_db()
    health_issues = []
    health_metrics = {}

    # Check for players without stats
    players_without_stats = db.execute_query("""
        SELECT COUNT(*) FROM players p
        WHERE NOT EXISTS (
            SELECT 1 FROM player_season_stats pss WHERE pss.player_id = p.player_id
        )
    """, fetch=True)[0][0]
    health_metrics['players_without_stats'] = players_without_stats
    if players_without_stats > 0:
        health_issues.append({
            'level': 'warning',
            'message': f'{players_without_stats} players have no season stats'
        })

    # Check for teams without players
    teams_without_players = db.execute_query("""
        SELECT COUNT(*) FROM teams t
        WHERE NOT EXISTS (
            SELECT 1 FROM player_season_stats pss WHERE pss.team_id = t.team_id
        )
    """, fetch=True)[0][0]
    health_metrics['teams_without_players'] = teams_without_players
    if teams_without_players > 0:
        health_issues.append({
            'level': 'info',
            'message': f'{teams_without_players} teams have no player stats'
        })

    # Check for matches without scores
    matches_without_scores = db.execute_query("""
        SELECT COUNT(*) FROM matches
        WHERE home_score IS NULL OR away_score IS NULL
    """, fetch=True)[0][0]
    health_metrics['matches_without_scores'] = matches_without_scores
    if matches_without_scores > 0:
        health_issues.append({
            'level': 'info',
            'message': f'{matches_without_scores} matches have no scores (possibly upcoming)'
        })

    # Check last data update
    last_match = db.execute_query("""
        SELECT MAX(match_date) FROM matches
    """, fetch=True)[0][0]
    health_metrics['latest_match_date'] = str(last_match) if last_match else None

    last_stat_update = db.execute_query("""
        SELECT MAX(updated_at) FROM player_season_stats
    """, fetch=True)[0][0]
    health_metrics['last_stat_update'] = str(last_stat_update) if last_stat_update else None

    # Data coverage by league
    league_coverage = db.execute_query("""
        SELECT
            l.league_name,
            COUNT(DISTINCT t.team_id) as teams,
            COUNT(DISTINCT pss.player_id) as players_with_stats,
            COUNT(DISTINCT pss.player_season_stat_id) as stat_records,
            ROUND(COUNT(DISTINCT pss.player_id)::numeric / NULLIF(COUNT(DISTINCT t.team_id), 0) / 25 * 100, 1) as coverage_pct
        FROM leagues l
        LEFT JOIN teams t ON l.league_id = t.league_id
        LEFT JOIN player_season_stats pss ON l.league_id = pss.league_id
        GROUP BY l.league_id, l.league_name
        ORDER BY l.league_name
    """, fetch=True)

    coverage = [{
        'league': row[0],
        'teams': row[1],
        'players': row[2],
        'records': row[3],
        'coverage': float(row[4]) if row[4] else 0
    } for row in league_coverage]

    # Overall health score (0-100)
    total_players = db.execute_query("SELECT COUNT(*) FROM players", fetch=True)[0][0]
    total_teams = db.execute_query("SELECT COUNT(*) FROM teams", fetch=True)[0][0]

    score = 100
    if total_players > 0:
        score -= min(30, (players_without_stats / total_players) * 50)
    if total_teams > 0:
        score -= min(20, (teams_without_players / total_teams) * 30)

    return jsonify({
        'score': max(0, round(score)),
        'issues': health_issues,
        'metrics': health_metrics,
        'coverage': coverage
    })

@app.route('/api/matches')
def get_recent_matches():
    """Get 50 most recent matches."""
    db = get_db()
    query = """
        SELECT 
            m.match_date, 
            l.league_name,
            ht.team_name as home_team,
            at.team_name as away_team,
            m.home_score,
            m.away_score,
            m.venue
        FROM matches m
        JOIN leagues l ON m.league_id = l.league_id
        JOIN teams ht ON m.home_team_id = ht.team_id
        JOIN teams at ON m.away_team_id = at.team_id
        ORDER BY m.match_date DESC
        LIMIT 50
    """
    matches = db.execute_query(query, fetch=True)
    
    # Format for JSON
    data = []
    for m in matches:
        data.append({
            'date': str(m[0]),
            'league': m[1],
            'home_team': m[2],
            'away_team': m[3],
            'score': f"{m[4]} - {m[5]}" if m[4] is not None else "vs",
            'venue': m[6]
        })
        
    return jsonify(data)

# --- Routes for Teams ---

@app.route('/teams')
def teams_list():
    """Render the teams list page."""
    return render_template('teams.html', active_page='teams')

@app.route('/teams/<int:team_id>')
def team_detail(team_id):
    """Render the team detail page."""
    db = get_db()
    # verify team exists
    res = db.execute_query("SELECT team_name FROM teams WHERE team_id = :id", {'id': team_id}, fetch=True)
    if not res:
        return "Team not found", 404
    return render_template('team_detail.html', active_page='teams', team_id=team_id, team_name=res[0][0])

@app.route('/api/teams')
def get_teams():
    """Get all teams with league info, optional search."""
    db = get_db()
    search = request.args.get('q', '').lower()
    
    query = """
        SELECT t.team_id, t.team_name, l.league_name, t.stadium, t.founded_year
        FROM teams t
        JOIN leagues l ON t.league_id = l.league_id
    """
    params = {}
    if search:
        query += " WHERE LOWER(t.team_name) LIKE :search"
        params['search'] = f"%{search}%"
        
    query += " ORDER BY t.team_name ASC"
    
    teams = db.execute_query(query, params, fetch=True)
    data = [{
        'id': t[0],
        'name': t[1],
        'league': t[2],
        'stadium': t[3],
        'founded': t[4]
    } for t in teams]
    return jsonify(data)

@app.route('/api/teams/<int:team_id>')
def get_team_details(team_id):
    """Get detailed stats for a team."""
    db = get_db()
    
    # Basic Info
    q_info = """
        SELECT t.team_name, l.league_name, t.stadium, t.founded_year
        FROM teams t
        JOIN leagues l ON t.league_id = l.league_id
        WHERE t.team_id = :id
    """
    info = db.execute_query(q_info, {'id': team_id}, fetch=True)
    if not info:
        return jsonify({'error': 'Team not found'}), 404
        
    team_data = {
        'name': info[0][0],
        'league': info[0][1],
        'stadium': info[0][2],
        'founded': info[0][3]
    }
    
    # Recent Matches
    q_matches = """
        SELECT m.match_date, ht.team_name, at.team_name, m.home_score, m.away_score, m.match_id
        FROM matches m
        JOIN teams ht ON m.home_team_id = ht.team_id
        JOIN teams at ON m.away_team_id = at.team_id
        WHERE m.home_team_id = :id OR m.away_team_id = :id
        ORDER BY m.match_date DESC
        LIMIT 10
    """
    matches = db.execute_query(q_matches, {'id': team_id}, fetch=True)
    team_data['recent_matches'] = [{
        'date': str(m[0]),
        'home_team': m[1],
        'away_team': m[2],
        'score': f"{m[3]} - {m[4]}",
        'id': m[5]
    } for m in matches]
    
    # Squad with Stats Summary (using player_season_stats)
    q_squad = """
        SELECT DISTINCT p.player_id, p.player_name, p.position, p.nationality,
               SUM(pss.matches_played) as apps,
               SUM(pss.goals) as goals,
               SUM(pss.assists) as assists,
               SUM(pss.minutes) as minutes
        FROM players p
        JOIN player_season_stats pss ON p.player_id = pss.player_id
        WHERE pss.team_id = :id
        GROUP BY p.player_id, p.player_name, p.position, p.nationality
        ORDER BY goals DESC, apps DESC
    """
    players = db.execute_query(q_squad, {'id': team_id}, fetch=True)

    team_data['squad'] = [{
        'id': p[0],
        'name': p[1],
        'position': p[2],
        'nationality': p[3],
        'apps': p[4] or 0,
        'goals': p[5] or 0,
        'assists': p[6] or 0,
        'minutes': p[7] or 0
    } for p in players]

    return jsonify(team_data)

# --- Routes for Players ---

@app.route('/players')
def players_list():
    return render_template('players.html', active_page='players')

@app.route('/players/<int:player_id>')
def player_detail(player_id):
    db = get_db()
    res = db.execute_query("SELECT player_name FROM players WHERE player_id = :id", {'id': player_id}, fetch=True)
    if not res:
        return "Player not found", 404
    return render_template('player_detail.html', active_page='players', player_id=player_id, player_name=res[0][0])

@app.route('/api/players')
def get_players():
    db = get_db()
    search = request.args.get('q', '').lower()
    league = request.args.get('league', '')
    position = request.args.get('position', '')

    # Enhanced query with stats summary
    query = """
        SELECT
            p.player_id,
            p.player_name,
            p.position,
            p.nationality,
            t.team_name,
            l.league_name,
            SUM(pss.goals) as total_goals,
            SUM(pss.assists) as total_assists,
            SUM(pss.minutes) as total_minutes,
            COUNT(DISTINCT pss.season_id) as seasons
        FROM players p
        LEFT JOIN player_season_stats pss ON p.player_id = pss.player_id
        LEFT JOIN teams t ON pss.team_id = t.team_id
        LEFT JOIN leagues l ON pss.league_id = l.league_id
        WHERE 1=1
    """
    params = {}

    if search:
        query += " AND LOWER(p.player_name) LIKE :search"
        params['search'] = f"%{search}%"
    if league:
        query += " AND l.league_name = :league"
        params['league'] = league
    if position:
        query += " AND p.position = :position"
        params['position'] = position

    query += """
        GROUP BY p.player_id, p.player_name, p.position, p.nationality, t.team_name, l.league_name
        ORDER BY total_goals DESC NULLS LAST, p.player_name ASC
        LIMIT 200
    """

    players = db.execute_query(query, params, fetch=True)
    data = [{
        'id': p[0],
        'name': p[1],
        'position': p[2],
        'nationality': p[3],
        'team': p[4],
        'league': p[5],
        'goals': p[6] or 0,
        'assists': p[7] or 0,
        'minutes': p[8] or 0,
        'seasons': p[9] or 0
    } for p in players]
    return jsonify(data)

@app.route('/api/players/<int:player_id>')
def get_player_details(player_id):
    db = get_db()

    # Basic Info
    q_info = "SELECT player_name, position, nationality, date_of_birth FROM players WHERE player_id = :id"
    info = db.execute_query(q_info, {'id': player_id}, fetch=True)
    if not info:
        return jsonify({'error': 'Player not found'}), 404

    player_data = {
        'name': info[0][0],
        'position': info[0][1],
        'nationality': info[0][2],
        'dob': str(info[0][3]) if info[0][3] else None
    }

    # Season stats (primary data source)
    q_seasons = """
        SELECT
            s.season_name,
            t.team_name,
            l.league_name,
            pss.matches_played,
            pss.starts,
            pss.minutes,
            pss.goals,
            pss.assists,
            pss.xg,
            pss.xag,
            pss.shots,
            pss.key_passes,
            pss.passes_completed,
            pss.tackles,
            pss.interceptions,
            pss.dribbles_completed,
            pss.yellow_cards,
            pss.red_cards
        FROM player_season_stats pss
        JOIN seasons s ON pss.season_id = s.season_id
        JOIN teams t ON pss.team_id = t.team_id
        JOIN leagues l ON pss.league_id = l.league_id
        WHERE pss.player_id = :id
        ORDER BY s.season_name DESC
    """
    seasons = db.execute_query(q_seasons, {'id': player_id}, fetch=True)

    player_data['season_stats'] = [{
        'season': row[0],
        'team': row[1],
        'league': row[2],
        'matches': row[3] or 0,
        'starts': row[4] or 0,
        'minutes': row[5] or 0,
        'goals': row[6] or 0,
        'assists': row[7] or 0,
        'xg': float(row[8]) if row[8] else 0.0,
        'xag': float(row[9]) if row[9] else 0.0,
        'shots': row[10] or 0,
        'key_passes': row[11] or 0,
        'passes': row[12] or 0,
        'tackles': row[13] or 0,
        'interceptions': row[14] or 0,
        'dribbles': row[15] or 0,
        'yellow_cards': row[16] or 0,
        'red_cards': row[17] or 0
    } for row in seasons]

    # Calculate career totals
    totals = {
        'matches': sum(s['matches'] for s in player_data['season_stats']),
        'goals': sum(s['goals'] for s in player_data['season_stats']),
        'assists': sum(s['assists'] for s in player_data['season_stats']),
        'minutes': sum(s['minutes'] for s in player_data['season_stats']),
        'xg': sum(s['xg'] for s in player_data['season_stats']),
        'xag': sum(s['xag'] for s in player_data['season_stats'])
    }
    player_data['career_totals'] = totals

    return jsonify(player_data)

# --- Routes for Matches ---

@app.route('/matches')
def matches_list():
    return render_template('matches.html', active_page='matches')

@app.route('/matches/<int:match_id>')
def match_detail(match_id):
    db = get_db()
    # Check existence
    res = db.execute_query("SELECT match_id FROM matches WHERE match_id = :id", {'id': match_id}, fetch=True)
    if not res:
        return "Match not found", 404
    return render_template('match_detail.html', active_page='matches', match_id=match_id)

@app.route('/api/matches/<int:match_id>')
def get_match_full_details(match_id):
    db = get_db()
    
    # Header Info
    q_header = """
        SELECT m.match_date, l.league_name, ht.team_name, at.team_name, 
               m.home_score, m.away_score, m.venue, ht.team_id, at.team_id
        FROM matches m
        JOIN leagues l ON m.league_id = l.league_id
        JOIN teams ht ON m.home_team_id = ht.team_id
        JOIN teams at ON m.away_team_id = at.team_id
        WHERE m.match_id = :id
    """
    header = db.execute_query(q_header, {'id': match_id}, fetch=True)
    if not header:
        return jsonify({'error': 'Match not found'}), 404
        
    h = header[0]
    data = {
        'date': str(h[0]),
        'league': h[1],
        'home_team': {'name': h[2], 'id': h[7]},
        'away_team': {'name': h[3], 'id': h[8]},
        'score': f"{h[4]} - {h[5]}",
        'venue': h[6]
    }
    
    # Lineups / Players
    # We get player stats for this match, sorted by team
    q_stats = """
        SELECT p.player_name, p.player_id, t.team_id, pms.minutes_played, pms.goals, pms.assists
        FROM player_match_stats pms
        JOIN players p ON pms.player_id = p.player_id
        JOIN teams t ON pms.team_id = t.team_id
        WHERE pms.match_id = :id
        ORDER BY t.team_name, pms.started DESC, pms.minutes_played DESC
    """
    stats = db.execute_query(q_stats, {'id': match_id}, fetch=True)
    
    home_players = []
    away_players = []
    
    for s in stats:
        p_obj = {
            'name': s[0], 'id': s[1], 
            'mins': s[3], 'goals': s[4], 'assists': s[5]
        }
        if s[2] == data['home_team']['id']:
            home_players.append(p_obj)
        else:
            away_players.append(p_obj)
            
    data['home_lineup'] = home_players
    data['away_lineup'] = away_players
    
    return jsonify(data)

# ─────────────────────────────────────────────────────────────────────────────
# FEATURE 1 — Pipeline Monitor
# ─────────────────────────────────────────────────────────────────────────────

KNOWN_JOBS = {
    'fotmob_daily':          {'schedule': 'Daily 05:00 UTC',      'source': 'FotMob'},
    'fotmob_weekly_deep':    {'schedule': 'Sunday 02:00 UTC',     'source': 'FotMob'},
    'api_football_daily':    {'schedule': 'Daily 06:00 UTC',      'source': 'API-Football'},
    'understat_monday':      {'schedule': 'Monday 08:00 UTC',     'source': 'Understat'},
    'understat_thursday':    {'schedule': 'Thursday 08:00 UTC',   'source': 'Understat'},
    'understat_weekly_full': {'schedule': 'Sunday 04:00 UTC',     'source': 'Understat'},
    'priority_standings':    {'schedule': 'Daily 12:00 UTC',      'source': 'API-Football'},
    'current_season_update': {'schedule': 'Daily 18:00 UTC',      'source': 'API-Football'},
}


@app.route('/pipeline')
def pipeline():
    return render_template('pipeline.html', active_page='pipeline')


@app.route('/api/pipeline/jobs')
def get_pipeline_jobs():
    """Read APScheduler job metadata from SQLite store."""
    db_path = PROJECT_ROOT / 'data' / 'scheduler_jobs.db'
    db_next_run = {}
    scheduler_running = False

    if db_path.exists():
        try:
            conn = sqlite3.connect(str(db_path))
            cur = conn.cursor()
            cur.execute("SELECT id, next_run_time FROM apscheduler_jobs")
            for row in cur.fetchall():
                db_next_run[row[0]] = row[1]
            conn.close()
            scheduler_running = len(db_next_run) > 0
        except Exception:
            pass

    jobs = []
    for job_id, info in KNOWN_JOBS.items():
        next_run_ts = db_next_run.get(job_id)
        next_run = None
        if next_run_ts:
            try:
                next_run = datetime.fromtimestamp(float(next_run_ts)).strftime('%Y-%m-%d %H:%M')
            except Exception:
                pass
        jobs.append({
            'id': job_id,
            'schedule': info['schedule'],
            'source': info['source'],
            'next_run': next_run,
            'is_scheduled': job_id in db_next_run,
        })

    return jsonify({'jobs': jobs, 'scheduler_running': scheduler_running})


@app.route('/api/pipeline/runs')
def get_pipeline_runs():
    """Parse scheduler.log for recent job execution entries."""
    log_paths = [
        PROJECT_ROOT / 'logs' / 'scheduler.log',
        PROJECT_ROOT / 'scheduler.log',
    ]
    log_path = next((p for p in log_paths if p.exists()), None)

    if log_path is None:
        return jsonify({'runs': [], 'note': 'scheduler.log not found'})

    runs = []
    try:
        with open(str(log_path), 'r', errors='replace') as f:
            lines = f.readlines()

        recent = lines[-200:] if len(lines) > 200 else lines
        log_re = re.compile(
            r'^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}),\d+ - ([\w.]+) - (\w+) - (.+)$'
        )
        job_name_re = re.compile(r'"([^"(]+)')

        for line in recent:
            m = log_re.match(line.strip())
            if not m:
                continue
            ts, logger_name, level, msg = m.groups()

            if 'apscheduler' in logger_name and (
                'Running job' in msg or
                'executed successfully' in msg or
                'raised an exception' in msg
            ):
                jm = job_name_re.search(msg)
                job_name = jm.group(1).strip() if jm else 'Unknown'

                if 'executed successfully' in msg:
                    status = 'success'
                elif 'raised an exception' in msg:
                    status = 'failed'
                else:
                    status = 'started'

                runs.append({
                    'timestamp': ts,
                    'job': job_name,
                    'status': status,
                    'message': msg[:150],
                })

    except Exception as e:
        return jsonify({'runs': [], 'error': str(e)})

    runs = runs[-50:][::-1]
    return jsonify({'runs': runs})


@app.route('/api/pipeline/latest-data')
def get_pipeline_latest_data():
    """When was each league/season last updated, plus last matchday per league."""
    db = get_db()

    freshness = db.execute_query("""
        SELECT l.league_name, s.season_name,
               MAX(pss.updated_at) as last_updated,
               COUNT(pss.player_season_stat_id) as record_count
        FROM player_season_stats pss
        JOIN leagues l ON pss.league_id = l.league_id
        JOIN seasons s ON pss.season_id = s.season_id
        GROUP BY l.league_name, s.season_name
        ORDER BY l.league_name, s.season_name DESC
    """, fetch=True)

    last_matchday = db.execute_query("""
        SELECT l.league_name, MAX(m.match_date) as last_match
        FROM matches m
        JOIN leagues l ON m.league_id = l.league_id
        GROUP BY l.league_name
        ORDER BY l.league_name
    """, fetch=True)

    return jsonify({
        'freshness': [{
            'league': row[0],
            'season': row[1],
            'last_updated': str(row[2]) if row[2] else None,
            'record_count': row[3],
        } for row in freshness],
        'last_matchday': [{
            'league': row[0],
            'last_match': str(row[1]) if row[1] else None,
        } for row in last_matchday],
    })


# ─────────────────────────────────────────────────────────────────────────────
# FEATURE 2 + 3 — Data Coverage Report + Duplicate Detection
# ─────────────────────────────────────────────────────────────────────────────

@app.route('/coverage')
def coverage():
    return render_template('coverage.html', active_page='coverage')


@app.route('/api/coverage')
def get_coverage():
    db = get_db()

    player_coverage = db.execute_query("""
        SELECT
            l.league_name,
            s.season_name,
            COUNT(*) as total_records,
            COUNT(CASE WHEN pss.minutes > 0 THEN 1 END) as with_minutes,
            COUNT(CASE WHEN pss.xg > 0 THEN 1 END) as with_xg,
            COUNT(CASE WHEN pss.goals IS NOT NULL THEN 1 END) as with_goals,
            COUNT(CASE WHEN pss.shots > 0 THEN 1 END) as with_shots
        FROM player_season_stats pss
        JOIN leagues l ON pss.league_id = l.league_id
        JOIN seasons s ON pss.season_id = s.season_id
        GROUP BY l.league_name, s.season_name
        ORDER BY l.league_name, s.season_name DESC
    """, fetch=True)

    team_coverage = db.execute_query("""
        SELECT
            l.league_name,
            s.season_name,
            COUNT(*) as total_records,
            COUNT(CASE WHEN tss.matches_played > 0 THEN 1 END) as with_matches
        FROM team_season_stats tss
        JOIN leagues l ON tss.league_id = l.league_id
        JOIN seasons s ON tss.season_id = s.season_id
        GROUP BY l.league_name, s.season_name
        ORDER BY l.league_name, s.season_name DESC
    """, fetch=True)

    missing = db.execute_query("""
        SELECT
            l.league_name,
            COUNT(CASE WHEN pss.minutes = 0 OR pss.minutes IS NULL THEN 1 END) as no_minutes,
            COUNT(CASE WHEN pss.xg IS NULL THEN 1 END) as no_xg,
            COUNT(CASE WHEN pss.goals IS NULL THEN 1 END) as no_goals,
            COUNT(*) as total
        FROM player_season_stats pss
        JOIN leagues l ON pss.league_id = l.league_id
        GROUP BY l.league_name
        ORDER BY no_minutes DESC
    """, fetch=True)

    return jsonify({
        'player_coverage': [{
            'league': row[0],
            'season': row[1],
            'total': row[2],
            'with_minutes': row[3],
            'with_xg': row[4],
            'with_goals': row[5],
            'with_shots': row[6],
            'coverage_pct': round(row[3] / row[2] * 100, 1) if row[2] > 0 else 0,
        } for row in player_coverage],
        'team_coverage': [{
            'league': row[0],
            'season': row[1],
            'total': row[2],
            'with_matches': row[3],
            'coverage_pct': round(row[3] / row[2] * 100, 1) if row[2] > 0 else 0,
        } for row in team_coverage],
        'missing': [{
            'league': row[0],
            'no_minutes': row[1],
            'no_xg': row[2],
            'no_goals': row[3],
            'total': row[4],
        } for row in missing],
    })


@app.route('/api/coverage/duplicates')
def get_coverage_duplicates():
    db = get_db()

    rows = db.execute_query("""
        SELECT p.player_name, s.season_name, l.league_name,
               COUNT(*) as record_count,
               COUNT(DISTINCT pss.team_id) as team_count
        FROM player_season_stats pss
        JOIN players p ON pss.player_id = p.player_id
        JOIN seasons s ON pss.season_id = s.season_id
        JOIN leagues l ON pss.league_id = l.league_id
        GROUP BY p.player_name, s.season_name, l.league_name
        HAVING COUNT(*) > 1
        ORDER BY COUNT(*) DESC
        LIMIT 100
    """, fetch=True)

    duplicates = [{
        'player': row[0],
        'season': row[1],
        'league': row[2],
        'record_count': row[3],
        'team_count': row[4],
        'type': 'Transfer Split' if row[4] > 1 else 'True Duplicate',
    } for row in rows]

    transfer_splits = sum(1 for d in duplicates if d['type'] == 'Transfer Split')

    return jsonify({
        'duplicates': duplicates,
        'total': len(duplicates),
        'transfer_splits': transfer_splits,
        'true_duplicates': len(duplicates) - transfer_splits,
    })


# ─────────────────────────────────────────────────────────────────────────────
# FEATURE 4 — Team seasons endpoint
# ─────────────────────────────────────────────────────────────────────────────

@app.route('/api/teams/<int:team_id>/seasons')
def get_team_seasons(team_id):
    db = get_db()

    rows = db.execute_query("""
        SELECT s.season_name, s.season_id,
               COUNT(pss.player_season_stat_id) as player_count,
               tss.matches_played, tss.wins, tss.losses, tss.draws,
               tss.goals_for, tss.goals_against
        FROM player_season_stats pss
        JOIN seasons s ON pss.season_id = s.season_id
        LEFT JOIN team_season_stats tss
               ON tss.team_id = pss.team_id AND tss.season_id = pss.season_id
        WHERE pss.team_id = :team_id
        GROUP BY s.season_name, s.season_id, tss.matches_played,
                 tss.wins, tss.losses, tss.draws, tss.goals_for, tss.goals_against
        ORDER BY s.season_name DESC
    """, {'team_id': team_id}, fetch=True)

    return jsonify([{
        'season': row[0],
        'season_id': row[1],
        'player_count': row[2] or 0,
        'matches_played': row[3] or 0,
        'wins': row[4] or 0,
        'losses': row[5] or 0,
        'draws': row[6] or 0,
        'goals_for': row[7] or 0,
        'goals_against': row[8] or 0,
        'points': ((row[4] or 0) * 3 + (row[6] or 0)) if row[3] else None,
    } for row in rows])


# ─────────────────────────────────────────────────────────────────────────────
# FEATURE 5 — Enhanced player full-profile endpoint
# ─────────────────────────────────────────────────────────────────────────────

@app.route('/api/players/<int:player_id>/full-profile')
def get_player_full_profile(player_id):
    db = get_db()

    rows = db.execute_query("""
        SELECT pss.season_id,
               s.season_name, l.league_name, t.team_name, t.team_id,
               p.player_name, p.position, p.nationality, p.date_of_birth,
               pss.matches_played, pss.starts, pss.minutes,
               pss.goals, pss.assists, pss.shots, pss.shots_on_target,
               pss.xg, pss.npxg, pss.xa, pss.xg_chain,
               pss.yellow_cards, pss.red_cards,
               pss.progressive_carries, pss.progressive_passes,
               pss.key_passes, pss.passes_completed,
               pss.tackles, pss.interceptions, pss.dribbles_completed
        FROM player_season_stats pss
        JOIN players p ON pss.player_id = p.player_id
        JOIN seasons s ON pss.season_id = s.season_id
        JOIN leagues l ON pss.league_id = l.league_id
        JOIN teams t ON pss.team_id = t.team_id
        WHERE pss.player_id = :player_id
        ORDER BY s.season_name DESC, pss.minutes DESC
    """, {'player_id': player_id}, fetch=True)

    if not rows:
        return jsonify({'error': 'Player not found'}), 404

    # Find which season_ids have multiple teams (transfer splits)
    season_teams: dict = {}
    for row in rows:
        sid = row[0]
        tid = row[4]
        season_teams.setdefault(sid, set()).add(tid)

    def per90(val, mins):
        if not val or not mins or mins == 0:
            return None
        return round(float(val) / (mins / 90), 2)

    player_info = None
    seasons = []

    for row in rows:
        if player_info is None:
            player_info = {
                'name': row[5],
                'position': row[6],
                'nationality': row[7],
                'dob': str(row[8]) if row[8] else None,
            }

        season_id = row[0]
        mins = row[11] or 0

        seasons.append({
            'season': row[1],
            'league': row[2],
            'team': row[3],
            'team_id': row[4],
            'is_transfer_split': len(season_teams.get(season_id, set())) > 1,
            'matches': row[9] or 0,
            'starts': row[10] or 0,
            'minutes': mins,
            'goals': row[12] or 0,
            'assists': row[13] or 0,
            'shots': row[14] or 0,
            'shots_on_target': row[15] or 0,
            'xg': float(row[16]) if row[16] else 0.0,
            'npxg': float(row[17]) if row[17] else 0.0,
            'xa': float(row[18]) if row[18] else 0.0,
            'xg_chain': float(row[19]) if row[19] else 0.0,
            'yellow_cards': row[20] or 0,
            'red_cards': row[21] or 0,
            'progressive_carries': row[22] or 0,
            'progressive_passes': row[23] or 0,
            'key_passes': row[24] or 0,
            'passes_completed': row[25] or 0,
            'tackles': row[26] or 0,
            'interceptions': row[27] or 0,
            'dribbles': row[28] or 0,
            'goals_per_90': per90(row[12], mins),
            'xg_per_90': per90(row[16], mins),
            'xa_per_90': per90(row[18], mins),
        })

    career = {
        'matches': sum(s['matches'] for s in seasons),
        'goals': sum(s['goals'] for s in seasons),
        'assists': sum(s['assists'] for s in seasons),
        'minutes': sum(s['minutes'] for s in seasons),
        'xg': round(sum(s['xg'] for s in seasons), 2),
        'xa': round(sum(s['xa'] for s in seasons), 2),
        'shots': sum(s['shots'] for s in seasons),
        'yellow_cards': sum(s['yellow_cards'] for s in seasons),
        'red_cards': sum(s['red_cards'] for s in seasons),
    }

    return jsonify({'player': player_info, 'seasons': seasons, 'career': career})


# ─────────────────────────────────────────────────────────────────────────────
# FEATURE 6 — Global Search
# ─────────────────────────────────────────────────────────────────────────────

@app.route('/api/search')
def global_search():
    q = request.args.get('q', '').strip()
    if len(q) < 3:
        return jsonify([])

    db = get_db()
    term = f'%{q.lower()}%'

    players = db.execute_query("""
        SELECT 'player' as type, player_id, player_name, position
        FROM players
        WHERE LOWER(player_name) LIKE :q
        LIMIT 5
    """, {'q': term}, fetch=True)

    teams = db.execute_query("""
        SELECT 'team' as type, t.team_id, t.team_name, l.league_name
        FROM teams t
        LEFT JOIN leagues l ON t.league_id = l.league_id
        WHERE LOWER(t.team_name) LIKE :q
        LIMIT 5
    """, {'q': term}, fetch=True)

    results = []
    for row in players:
        results.append({
            'type': row[0], 'id': row[1], 'name': row[2],
            'subtitle': row[3] or '', 'url': f'/players/{row[1]}',
        })
    for row in teams:
        results.append({
            'type': row[0], 'id': row[1], 'name': row[2],
            'subtitle': row[3] or '', 'url': f'/teams/{row[1]}',
        })

    return jsonify(results)


# ─────────────────────────────────────────────────────────────────────────────
# FEATURE 7 — Season Comparison
# ─────────────────────────────────────────────────────────────────────────────

@app.route('/api/stats/season-comparison')
def get_season_comparison():
    db = get_db()

    rows = db.execute_query("""
        SELECT
            s.season_name,
            COUNT(DISTINCT pss.player_id) as total_players,
            COUNT(DISTINCT CASE WHEN pss.xg > 0 THEN pss.player_id END) as players_with_xg,
            ROUND(AVG(pss.xg)::numeric, 2) as avg_xg
        FROM player_season_stats pss
        JOIN seasons s ON pss.season_id = s.season_id
        GROUP BY s.season_name
        ORDER BY s.season_name DESC
    """, fetch=True)

    # Top scorer per season (first group per season after ordering by goals desc)
    scorer_rows = db.execute_query("""
        SELECT s.season_name, p.player_name, SUM(pss.goals) as total_goals
        FROM player_season_stats pss
        JOIN players p ON pss.player_id = p.player_id
        JOIN seasons s ON pss.season_id = s.season_id
        WHERE pss.goals > 0
        GROUP BY s.season_name, pss.player_id, p.player_name
        ORDER BY s.season_name, total_goals DESC
    """, fetch=True)

    top_scorers: dict = {}
    for row in scorer_rows:
        season = row[0]
        if season not in top_scorers:
            top_scorers[season] = {'name': row[1], 'goals': int(row[2]) if row[2] else 0}

    seasons = []
    for row in rows:
        sn = row[0]
        scorer = top_scorers.get(sn, {})
        seasons.append({
            'season': sn,
            'total_players': row[1],
            'players_with_xg': row[2],
            'avg_xg': float(row[3]) if row[3] else 0.0,
            'top_scorer': scorer.get('name', 'N/A'),
            'top_scorer_goals': scorer.get('goals', 0),
        })

    return jsonify(seasons)


if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5001)
