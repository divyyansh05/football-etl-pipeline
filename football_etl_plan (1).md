# Football Data Ingestion & ETL Pipeline
## Complete Implementation Plan for LLM Code Editors

---

## 🎯 PROJECT OVERVIEW

**Purpose**: Build a production-grade, standalone ETL pipeline that collects, validates, transforms, and stores football (soccer) data from multiple seasons and leagues into a normalized PostgreSQL database.

**Key Principle**: This is a DATA PROVIDER, not an analytics application. It runs independently, scheduled via CLI/cron, and populates a database that other applications can consume.

---

## 📋 IMPLEMENTATION PHASES

### **PHASE 1: Project Initialization & Structure**
### **PHASE 2: Database Layer**
### **PHASE 3: Core Utilities & Configuration**
### **PHASE 4: Scraper Modules**
### **PHASE 5: Parser Layer**
### **PHASE 6: Validation Layer**
### **PHASE 7: ETL Pipeline**
### **PHASE 8: CLI Interface**
### **PHASE 9: Scheduling & Automation**
### **PHASE 10: Testing & Documentation**

---

## PHASE 1: PROJECT INITIALIZATION & STRUCTURE

### Step 1.1: Create Root Directory and Virtual Environment

```bash
# Commands to execute
mkdir football_data_pipeline
cd football_data_pipeline
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

### Step 1.2: Create Complete Directory Structure

```bash
mkdir -p scrapers/fbref
mkdir -p scrapers/soccerway
mkdir -p scrapers/understat
mkdir -p scrapers/transfermarkt
mkdir -p scrapers/common
mkdir -p parsers
mkdir -p validators
mkdir -p etl
mkdir -p database
mkdir -p config
mkdir -p scheduler
mkdir -p logs
mkdir -p tests/unit
mkdir -p tests/integration
mkdir -p data/raw
mkdir -p data/processed
mkdir -p data/cache
```

### Step 1.3: Create Initial Files

Create empty `__init__.py` files in all Python package directories:
```bash
touch scrapers/__init__.py
touch scrapers/fbref/__init__.py
touch scrapers/soccerway/__init__.py
touch scrapers/understat/__init__.py
touch scrapers/transfermarkt/__init__.py
touch scrapers/common/__init__.py
touch parsers/__init__.py
touch validators/__init__.py
touch etl/__init__.py
touch database/__init__.py
touch scheduler/__init__.py
touch tests/__init__.py
touch tests/unit/__init__.py
touch tests/integration/__init__.py
```

### Step 1.4: Create requirements.txt

**File**: `requirements.txt`

```txt
# Web Scraping
requests==2.31.0
beautifulsoup4==4.12.2
lxml==4.9.3
selenium==4.15.2
webdriver-manager==4.0.1

# Data Processing
pandas==2.1.3
numpy==1.26.2

# Database
psycopg2-binary==2.9.9
sqlalchemy==2.0.23

# Configuration
pyyaml==6.0.1
python-dotenv==1.0.0

# Scheduling
schedule==1.2.0
APScheduler==3.10.4

# CLI
click==8.1.7
colorama==0.4.6
rich==13.7.0

# Logging
python-json-logger==2.0.7

# Data Validation
jsonschema==4.20.0
cerberus==1.3.5

# Rate Limiting
ratelimit==2.2.1

# Testing
pytest==7.4.3
pytest-cov==4.1.0
pytest-mock==3.12.0
faker==20.1.0

# Code Quality
black==23.11.0
flake8==6.1.0
mypy==1.7.1
```

### Step 1.5: Create .gitignore

**File**: `.gitignore`

```
# Virtual Environment
venv/
env/
.env

# Python
__pycache__/
*.py[cod]
*$py.class
*.so
.Python

# Database
*.db
*.sqlite3

# Data
data/raw/*
data/processed/*
data/cache/*
!data/raw/.gitkeep
!data/processed/.gitkeep
!data/cache/.gitkeep

# Logs
logs/*.log
logs/*.json

# IDE
.vscode/
.idea/
*.swp
*.swo
.DS_Store

# Testing
.pytest_cache/
.coverage
htmlcov/

# Secrets
secrets/
*.key
*.pem
config/secrets.yaml
```

---

## PHASE 2: DATABASE LAYER

### Step 2.1: Design Complete Database Schema

**File**: `database/schema.sql`

```sql
-- Football Data Pipeline Database Schema
-- PostgreSQL 13+

-- Enable required extensions
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- Drop tables if they exist (for clean rebuilds)
DROP TABLE IF EXISTS player_match_stats CASCADE;
DROP TABLE IF EXISTS team_match_stats CASCADE;
DROP TABLE IF EXISTS matches CASCADE;
DROP TABLE IF EXISTS player_season_stats CASCADE;
DROP TABLE IF EXISTS team_season_stats CASCADE;
DROP TABLE IF EXISTS players CASCADE;
DROP TABLE IF EXISTS teams CASCADE;
DROP TABLE IF EXISTS seasons CASCADE;
DROP TABLE IF EXISTS leagues CASCADE;
DROP TABLE IF EXISTS data_sources CASCADE;
DROP TABLE IF EXISTS scrape_metadata CASCADE;

-- ============================================
-- REFERENCE TABLES
-- ============================================

-- Data Sources: Track which source provided each piece of data
CREATE TABLE data_sources (
    source_id SERIAL PRIMARY KEY,
    source_name VARCHAR(50) UNIQUE NOT NULL,
    base_url VARCHAR(255),
    reliability_score INTEGER DEFAULT 100,
    last_successful_scrape TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Leagues
CREATE TABLE leagues (
    league_id SERIAL PRIMARY KEY,
    league_name VARCHAR(100) NOT NULL,
    country VARCHAR(50) NOT NULL,
    tier INTEGER DEFAULT 1,
    fbref_id VARCHAR(50),
    understat_name VARCHAR(50),
    transfermarkt_id VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(league_name, country)
);

-- Seasons
CREATE TABLE seasons (
    season_id SERIAL PRIMARY KEY,
    season_name VARCHAR(20) NOT NULL UNIQUE,  -- e.g., "2023-24"
    start_year INTEGER NOT NULL,
    end_year INTEGER NOT NULL,
    is_current BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ============================================
-- ENTITY TABLES
-- ============================================

-- Teams
CREATE TABLE teams (
    team_id SERIAL PRIMARY KEY,
    team_name VARCHAR(100) NOT NULL,
    team_short_name VARCHAR(50),
    league_id INTEGER REFERENCES leagues(league_id),
    fbref_id VARCHAR(50),
    understat_id VARCHAR(50),
    transfermarkt_id VARCHAR(50),
    founded_year INTEGER,
    stadium VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(team_name, league_id)
);

-- Create index on team lookups
CREATE INDEX idx_teams_league ON teams(league_id);
CREATE INDEX idx_teams_name ON teams(team_name);

-- Players
CREATE TABLE players (
    player_id SERIAL PRIMARY KEY,
    player_name VARCHAR(100) NOT NULL,
    date_of_birth DATE,
    nationality VARCHAR(50),
    position VARCHAR(20),
    preferred_foot VARCHAR(10),
    height_cm INTEGER,
    fbref_id VARCHAR(50),
    understat_id VARCHAR(50),
    transfermarkt_id VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes on player lookups
CREATE INDEX idx_players_name ON players(player_name);
CREATE INDEX idx_players_position ON players(position);

-- ============================================
-- MATCH TABLES
-- ============================================

-- Matches
CREATE TABLE matches (
    match_id SERIAL PRIMARY KEY,
    league_id INTEGER REFERENCES leagues(league_id),
    season_id INTEGER REFERENCES seasons(season_id),
    match_date DATE NOT NULL,
    matchday INTEGER,
    home_team_id INTEGER REFERENCES teams(team_id),
    away_team_id INTEGER REFERENCES teams(team_id),
    home_score INTEGER,
    away_score INTEGER,
    home_xg DECIMAL(5,2),
    away_xg DECIMAL(5,2),
    attendance INTEGER,
    referee VARCHAR(100),
    venue VARCHAR(100),
    match_status VARCHAR(20) DEFAULT 'completed',  -- completed, postponed, cancelled
    data_source_id INTEGER REFERENCES data_sources(source_id),
    scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(league_id, season_id, home_team_id, away_team_id, match_date)
);

-- Create indexes on match queries
CREATE INDEX idx_matches_league_season ON matches(league_id, season_id);
CREATE INDEX idx_matches_date ON matches(match_date);
CREATE INDEX idx_matches_teams ON matches(home_team_id, away_team_id);

-- Team Match Stats
CREATE TABLE team_match_stats (
    team_match_stat_id SERIAL PRIMARY KEY,
    match_id INTEGER REFERENCES matches(match_id) ON DELETE CASCADE,
    team_id INTEGER REFERENCES teams(team_id),
    is_home BOOLEAN NOT NULL,
    goals INTEGER DEFAULT 0,
    shots INTEGER,
    shots_on_target INTEGER,
    possession DECIMAL(5,2),
    passes_completed INTEGER,
    passes_attempted INTEGER,
    pass_accuracy DECIMAL(5,2),
    fouls_committed INTEGER,
    yellow_cards INTEGER DEFAULT 0,
    red_cards INTEGER DEFAULT 0,
    corners INTEGER,
    offsides INTEGER,
    xg DECIMAL(5,2),
    data_source_id INTEGER REFERENCES data_sources(source_id),
    scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(match_id, team_id)
);

CREATE INDEX idx_team_match_stats_match ON team_match_stats(match_id);
CREATE INDEX idx_team_match_stats_team ON team_match_stats(team_id);

-- Player Match Stats
CREATE TABLE player_match_stats (
    player_match_stat_id SERIAL PRIMARY KEY,
    match_id INTEGER REFERENCES matches(match_id) ON DELETE CASCADE,
    player_id INTEGER REFERENCES players(player_id),
    team_id INTEGER REFERENCES teams(team_id),
    minutes_played INTEGER DEFAULT 0,
    started BOOLEAN DEFAULT FALSE,
    position_played VARCHAR(20),
    goals INTEGER DEFAULT 0,
    assists INTEGER DEFAULT 0,
    shots INTEGER DEFAULT 0,
    shots_on_target INTEGER DEFAULT 0,
    xg DECIMAL(5,3),
    xa DECIMAL(5,3),  -- Expected assists
    key_passes INTEGER DEFAULT 0,
    passes_completed INTEGER DEFAULT 0,
    passes_attempted INTEGER DEFAULT 0,
    progressive_passes INTEGER DEFAULT 0,
    tackles INTEGER DEFAULT 0,
    interceptions INTEGER DEFAULT 0,
    blocks INTEGER DEFAULT 0,
    clearances INTEGER DEFAULT 0,
    aerials_won INTEGER DEFAULT 0,
    aerials_lost INTEGER DEFAULT 0,
    dribbles_completed INTEGER DEFAULT 0,
    dribbles_attempted INTEGER DEFAULT 0,
    fouls_committed INTEGER DEFAULT 0,
    fouls_won INTEGER DEFAULT 0,
    yellow_cards INTEGER DEFAULT 0,
    red_cards INTEGER DEFAULT 0,
    touches INTEGER DEFAULT 0,
    data_source_id INTEGER REFERENCES data_sources(source_id),
    scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(match_id, player_id)
);

CREATE INDEX idx_player_match_stats_match ON player_match_stats(match_id);
CREATE INDEX idx_player_match_stats_player ON player_match_stats(player_id);
CREATE INDEX idx_player_match_stats_team ON player_match_stats(team_id);

-- ============================================
-- AGGREGATED SEASON STATS
-- ============================================

-- Team Season Stats
CREATE TABLE team_season_stats (
    team_season_stat_id SERIAL PRIMARY KEY,
    team_id INTEGER REFERENCES teams(team_id),
    season_id INTEGER REFERENCES seasons(season_id),
    league_id INTEGER REFERENCES leagues(league_id),
    matches_played INTEGER DEFAULT 0,
    wins INTEGER DEFAULT 0,
    draws INTEGER DEFAULT 0,
    losses INTEGER DEFAULT 0,
    goals_for INTEGER DEFAULT 0,
    goals_against INTEGER DEFAULT 0,
    goal_difference INTEGER DEFAULT 0,
    points INTEGER DEFAULT 0,
    xg_for DECIMAL(6,2),
    xg_against DECIMAL(6,2),
    shots INTEGER,
    shots_on_target INTEGER,
    possession_avg DECIMAL(5,2),
    pass_accuracy_avg DECIMAL(5,2),
    clean_sheets INTEGER DEFAULT 0,
    yellow_cards INTEGER DEFAULT 0,
    red_cards INTEGER DEFAULT 0,
    final_position INTEGER,
    data_source_id INTEGER REFERENCES data_sources(source_id),
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(team_id, season_id, league_id)
);

CREATE INDEX idx_team_season_stats_team_season ON team_season_stats(team_id, season_id);
CREATE INDEX idx_team_season_stats_league_season ON team_season_stats(league_id, season_id);

-- Player Season Stats (Vector-Ready for ML)
CREATE TABLE player_season_stats (
    player_season_stat_id SERIAL PRIMARY KEY,
    player_id INTEGER REFERENCES players(player_id),
    team_id INTEGER REFERENCES teams(team_id),
    season_id INTEGER REFERENCES seasons(season_id),
    league_id INTEGER REFERENCES leagues(league_id),
    
    -- Appearance Data
    matches_played INTEGER DEFAULT 0,
    starts INTEGER DEFAULT 0,
    minutes_played INTEGER DEFAULT 0,
    
    -- Attacking Metrics
    goals INTEGER DEFAULT 0,
    assists INTEGER DEFAULT 0,
    xg DECIMAL(6,3),
    xa DECIMAL(6,3),
    shots INTEGER DEFAULT 0,
    shots_on_target INTEGER DEFAULT 0,
    shot_accuracy DECIMAL(5,2),
    key_passes INTEGER DEFAULT 0,
    dribbles_completed INTEGER DEFAULT 0,
    dribbles_attempted INTEGER DEFAULT 0,
    dribble_success_rate DECIMAL(5,2),
    
    -- Passing Metrics
    passes_completed INTEGER DEFAULT 0,
    passes_attempted INTEGER DEFAULT 0,
    pass_accuracy DECIMAL(5,2),
    progressive_passes INTEGER DEFAULT 0,
    
    -- Defensive Metrics
    tackles INTEGER DEFAULT 0,
    interceptions INTEGER DEFAULT 0,
    blocks INTEGER DEFAULT 0,
    clearances INTEGER DEFAULT 0,
    aerials_won INTEGER DEFAULT 0,
    aerials_lost INTEGER DEFAULT 0,
    
    -- Discipline
    fouls_committed INTEGER DEFAULT 0,
    fouls_won INTEGER DEFAULT 0,
    yellow_cards INTEGER DEFAULT 0,
    red_cards INTEGER DEFAULT 0,
    
    -- PER-90 METRICS (for fair comparison)
    goals_per90 DECIMAL(5,3),
    assists_per90 DECIMAL(5,3),
    xg_per90 DECIMAL(5,3),
    xa_per90 DECIMAL(5,3),
    shots_per90 DECIMAL(5,2),
    key_passes_per90 DECIMAL(5,2),
    tackles_per90 DECIMAL(5,2),
    interceptions_per90 DECIMAL(5,2),
    progressive_passes_per90 DECIMAL(5,2),
    
    -- Metadata
    primary_position VARCHAR(20),
    data_source_id INTEGER REFERENCES data_sources(source_id),
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    UNIQUE(player_id, team_id, season_id, league_id)
);

CREATE INDEX idx_player_season_stats_player_season ON player_season_stats(player_id, season_id);
CREATE INDEX idx_player_season_stats_team_season ON player_season_stats(team_id, season_id);
CREATE INDEX idx_player_season_stats_league_season ON player_season_stats(league_id, season_id);
CREATE INDEX idx_player_season_stats_position ON player_season_stats(primary_position);

-- ============================================
-- METADATA & AUDIT TABLES
-- ============================================

-- Scrape Metadata: Track all scraping operations
CREATE TABLE scrape_metadata (
    scrape_id SERIAL PRIMARY KEY,
    scrape_type VARCHAR(50) NOT NULL,  -- 'match', 'team', 'player', 'league'
    data_source_id INTEGER REFERENCES data_sources(source_id),
    season_id INTEGER REFERENCES seasons(season_id),
    league_id INTEGER REFERENCES leagues(league_id),
    scrape_status VARCHAR(20) NOT NULL,  -- 'success', 'partial', 'failed'
    records_scraped INTEGER DEFAULT 0,
    records_inserted INTEGER DEFAULT 0,
    records_updated INTEGER DEFAULT 0,
    error_message TEXT,
    started_at TIMESTAMP NOT NULL,
    completed_at TIMESTAMP,
    duration_seconds INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_scrape_metadata_source_season ON scrape_metadata(data_source_id, season_id);
CREATE INDEX idx_scrape_metadata_status ON scrape_metadata(scrape_status);
CREATE INDEX idx_scrape_metadata_started ON scrape_metadata(started_at);

-- ============================================
-- INITIAL DATA SEEDS
-- ============================================

-- Insert data sources
INSERT INTO data_sources (source_name, base_url, reliability_score) VALUES
    ('fbref', 'https://fbref.com', 95),
    ('understat', 'https://understat.com', 90),
    ('soccerway', 'https://int.soccerway.com', 85),
    ('transfermarkt', 'https://www.transfermarkt.com', 80);

-- Insert top 5 European leagues
INSERT INTO leagues (league_name, country, tier) VALUES
    ('Premier League', 'England', 1),
    ('La Liga', 'Spain', 1),
    ('Serie A', 'Italy', 1),
    ('Bundesliga', 'Germany', 1),
    ('Ligue 1', 'France', 1);

-- Insert seasons
INSERT INTO seasons (season_name, start_year, end_year, is_current) VALUES
    ('2020-21', 2020, 2021, FALSE),
    ('2021-22', 2021, 2022, FALSE),
    ('2022-23', 2022, 2023, FALSE),
    ('2023-24', 2023, 2024, FALSE),
    ('2024-25', 2024, 2025, TRUE);

-- ============================================
-- VIEWS FOR ANALYTICS
-- ============================================

-- View: Current season player stats with rankings
CREATE OR REPLACE VIEW current_season_player_rankings AS
SELECT 
    p.player_name,
    t.team_name,
    l.league_name,
    pss.*,
    RANK() OVER (PARTITION BY pss.league_id, pss.primary_position ORDER BY pss.goals DESC) as goals_rank,
    RANK() OVER (PARTITION BY pss.league_id, pss.primary_position ORDER BY pss.assists DESC) as assists_rank,
    RANK() OVER (PARTITION BY pss.league_id, pss.primary_position ORDER BY pss.xg DESC) as xg_rank
FROM player_season_stats pss
JOIN players p ON pss.player_id = p.player_id
JOIN teams t ON pss.team_id = t.team_id
JOIN leagues l ON pss.league_id = l.league_id
JOIN seasons s ON pss.season_id = s.season_id
WHERE s.is_current = TRUE
  AND pss.minutes_played >= 900;  -- At least 10 full matches

-- View: Team form (last 5 matches)
CREATE OR REPLACE VIEW team_recent_form AS
WITH recent_matches AS (
    SELECT 
        m.match_id,
        m.match_date,
        CASE 
            WHEN m.home_team_id = t.team_id THEN 'home'
            ELSE 'away'
        END as venue,
        t.team_id,
        t.team_name,
        CASE 
            WHEN m.home_team_id = t.team_id THEN m.home_score
            ELSE m.away_score
        END as goals_for,
        CASE 
            WHEN m.home_team_id = t.team_id THEN m.away_score
            ELSE m.home_score
        END as goals_against,
        ROW_NUMBER() OVER (PARTITION BY t.team_id ORDER BY m.match_date DESC) as match_number
    FROM matches m
    CROSS JOIN teams t
    WHERE (m.home_team_id = t.team_id OR m.away_team_id = t.team_id)
      AND m.match_status = 'completed'
)
SELECT 
    team_id,
    team_name,
    COUNT(*) as matches_played,
    SUM(CASE WHEN goals_for > goals_against THEN 1 ELSE 0 END) as wins,
    SUM(CASE WHEN goals_for = goals_against THEN 1 ELSE 0 END) as draws,
    SUM(CASE WHEN goals_for < goals_against THEN 1 ELSE 0 END) as losses,
    SUM(goals_for) as goals_for,
    SUM(goals_against) as goals_against
FROM recent_matches
WHERE match_number <= 5
GROUP BY team_id, team_name;

-- ============================================
-- FUNCTIONS & TRIGGERS
-- ============================================

-- Function: Update team_season_stats when matches are inserted/updated
CREATE OR REPLACE FUNCTION update_team_season_stats()
RETURNS TRIGGER AS $$
BEGIN
    -- Update home team stats
    INSERT INTO team_season_stats (
        team_id, season_id, league_id, matches_played, 
        wins, draws, losses, goals_for, goals_against, 
        goal_difference, points
    )
    SELECT 
        NEW.home_team_id,
        NEW.season_id,
        NEW.league_id,
        1,
        CASE WHEN NEW.home_score > NEW.away_score THEN 1 ELSE 0 END,
        CASE WHEN NEW.home_score = NEW.away_score THEN 1 ELSE 0 END,
        CASE WHEN NEW.home_score < NEW.away_score THEN 1 ELSE 0 END,
        NEW.home_score,
        NEW.away_score,
        NEW.home_score - NEW.away_score,
        CASE 
            WHEN NEW.home_score > NEW.away_score THEN 3
            WHEN NEW.home_score = NEW.away_score THEN 1
            ELSE 0
        END
    ON CONFLICT (team_id, season_id, league_id) 
    DO UPDATE SET
        matches_played = team_season_stats.matches_played + 1,
        wins = team_season_stats.wins + EXCLUDED.wins,
        draws = team_season_stats.draws + EXCLUDED.draws,
        losses = team_season_stats.losses + EXCLUDED.losses,
        goals_for = team_season_stats.goals_for + EXCLUDED.goals_for,
        goals_against = team_season_stats.goals_against + EXCLUDED.goals_against,
        goal_difference = team_season_stats.goal_difference + EXCLUDED.goal_difference,
        points = team_season_stats.points + EXCLUDED.points,
        last_updated = CURRENT_TIMESTAMP;
    
    -- Update away team stats (similar logic)
    -- ... (similar INSERT ON CONFLICT for away team)
    
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Trigger: Auto-update team season stats on match insert
CREATE TRIGGER trigger_update_team_season_stats
    AFTER INSERT OR UPDATE ON matches
    FOR EACH ROW
    WHEN (NEW.match_status = 'completed')
    EXECUTE FUNCTION update_team_season_stats();

-- ============================================
-- GRANTS (for application user)
-- ============================================

-- CREATE USER football_pipeline_user WITH PASSWORD 'your_secure_password';
-- GRANT SELECT, INSERT, UPDATE ON ALL TABLES IN SCHEMA public TO football_pipeline_user;
-- GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO football_pipeline_user;
```

### Step 2.2: Create Database Connection Module

**File**: `database/connection.py`

```python
"""
Database connection management with connection pooling and retry logic.
"""

import os
import logging
from typing import Optional, Dict, Any
from contextlib import contextmanager
from sqlalchemy import create_engine, text, pool
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.exc import SQLAlchemyError, OperationalError
import time

logger = logging.getLogger(__name__)


class DatabaseConnection:
    """
    Manages PostgreSQL database connections with pooling and automatic retry.
    """
    
    def __init__(
        self,
        host: Optional[str] = None,
        port: Optional[int] = None,
        database: Optional[str] = None,
        user: Optional[str] = None,
        password: Optional[str] = None,
        pool_size: int = 5,
        max_overflow: int = 10,
        pool_timeout: int = 30
    ):
        """
        Initialize database connection.
        
        Args:
            host: Database host (defaults to env var DB_HOST)
            port: Database port (defaults to env var DB_PORT or 5432)
            database: Database name (defaults to env var DB_NAME)
            user: Database user (defaults to env var DB_USER)
            password: Database password (defaults to env var DB_PASSWORD)
            pool_size: Number of connections to keep in pool
            max_overflow: Max connections beyond pool_size
            pool_timeout: Seconds to wait for connection from pool
        """
        self.host = host or os.getenv('DB_HOST', 'localhost')
        self.port = port or int(os.getenv('DB_PORT', '5432'))
        self.database = database or os.getenv('DB_NAME', 'football_data')
        self.user = user or os.getenv('DB_USER', 'postgres')
        self.password = password or os.getenv('DB_PASSWORD', '')
        
        self.connection_string = (
            f"postgresql://{self.user}:{self.password}@"
            f"{self.host}:{self.port}/{self.database}"
        )
        
        # Create engine with connection pooling
        self.engine = create_engine(
            self.connection_string,
            poolclass=pool.QueuePool,
            pool_size=pool_size,
            max_overflow=max_overflow,
            pool_timeout=pool_timeout,
            pool_pre_ping=True,  # Verify connections before using
            echo=False  # Set to True for SQL logging
        )
        
        # Create session factory
        self.SessionLocal = sessionmaker(
            autocommit=False,
            autoflush=False,
            bind=self.engine
        )
        
        logger.info(f"Database connection initialized: {self.host}:{self.port}/{self.database}")
    
    @contextmanager
    def get_session(self) -> Session:
        """
        Context manager for database sessions.
        
        Yields:
            SQLAlchemy session object
            
        Example:
            with db.get_session() as session:
                result = session.execute(text("SELECT * FROM leagues"))
        """
        session = self.SessionLocal()
        try:
            yield session
            session.commit()
        except Exception as e:
            session.rollback()
            logger.error(f"Session error: {e}")
            raise
        finally:
            session.close()
    
    def execute_query(
        self,
        query: str,
        params: Optional[Dict[str, Any]] = None,
        fetch: bool = True,
        max_retries: int = 3
    ):
        """
        Execute a SQL query with automatic retry on failure.
        
        Args:
            query: SQL query string
            params: Query parameters
            fetch: Whether to fetch and return results
            max_retries: Maximum retry attempts
            
        Returns:
            Query results if fetch=True, else None
        """
        params = params or {}
        
        for attempt in range(max_retries):
            try:
                with self.get_session() as session:
                    result = session.execute(text(query), params)
                    
                    if fetch:
                        return result.fetchall()
                    return None
                    
            except OperationalError as e:
                if attempt < max_retries - 1:
                    wait_time = 2 ** attempt  # Exponential backoff
                    logger.warning(
                        f"Query failed (attempt {attempt + 1}/{max_retries}). "
                        f"Retrying in {wait_time}s... Error: {e}"
                    )
                    time.sleep(wait_time)
                else:
                    logger.error(f"Query failed after {max_retries} attempts: {e}")
                    raise
            except SQLAlchemyError as e:
                logger.error(f"Database error: {e}")
                raise
    
    def test_connection(self) -> bool:
        """
        Test database connectivity.
        
        Returns:
            True if connection successful, False otherwise
        """
        try:
            result = self.execute_query("SELECT 1 as test")
            return result[0][0] == 1
        except Exception as e:
            logger.error(f"Connection test failed: {e}")
            return False
    
    def initialize_schema(self, schema_file: str = "database/schema.sql"):
        """
        Initialize database schema from SQL file.
        
        Args:
            schema_file: Path to schema SQL file
        """
        try:
            with open(schema_file, 'r') as f:
                schema_sql = f.read()
            
            # Split by semicolons and execute each statement
            statements = [s.strip() for s in schema_sql.split(';') if s.strip()]
            
            with self.get_session() as session:
                for statement in statements:
                    if statement:
                        session.execute(text(statement))
                        
            logger.info(f"Database schema initialized from {schema_file}")
            
        except FileNotFoundError:
            logger.error(f"Schema file not found: {schema_file}")
            raise
        except Exception as e:
            logger.error(f"Failed to initialize schema: {e}")
            raise
    
    def close(self):
        """Close all database connections."""
        self.engine.dispose()
        logger.info("Database connections closed")


# Global database instance (initialized once)
_db_instance: Optional[DatabaseConnection] = None


def get_db() -> DatabaseConnection:
    """
    Get or create global database instance.
    
    Returns:
        DatabaseConnection instance
    """
    global _db_instance
    if _db_instance is None:
        _db_instance = DatabaseConnection()
    return _db_instance


def init_database():
    """Initialize database schema (run once during setup)."""
    db = get_db()
    db.initialize_schema()
```

---

## PHASE 3: CORE UTILITIES & CONFIGURATION

### Step 3.1: Create Logging Configuration

**File**: `config/logging_config.py`

```python
"""
Centralized logging configuration for the entire pipeline.
"""

import logging
import sys
from pathlib import Path
from logging.handlers import RotatingFileHandler
from pythonjsonlogger import jsonlogger


def setup_logging(
    log_level: str = "INFO",
    log_dir: str = "logs",
    log_to_console: bool = True,
    log_to_file: bool = True
):
    """
    Configure application-wide logging.
    
    Args:
        log_level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        log_dir: Directory for log files
        log_to_console: Enable console logging
        log_to_file: Enable file logging
    """
    # Create logs directory
    Path(log_dir).mkdir(parents=True, exist_ok=True)
    
    # Root logger
    logger = logging.getLogger()
    logger.setLevel(getattr(logging, log_level.upper()))
    
    # Remove existing handlers
    logger.handlers = []
    
    # Console handler
    if log_to_console:
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(logging.INFO)
        console_formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        console_handler.setFormatter(console_formatter)
        logger.addHandler(console_handler)
    
    # File handler (rotating)
    if log_to_file:
        file_handler = RotatingFileHandler(
            f"{log_dir}/pipeline.log",
            maxBytes=10 * 1024 * 1024,  # 10 MB
            backupCount=5
        )
        file_handler.setLevel(logging.DEBUG)
        file_formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(funcName)s:%(lineno)d - %(message)s'
        )
        file_handler.setFormatter(file_formatter)
        logger.addHandler(file_handler)
    
    # JSON handler for structured logging
    json_handler = RotatingFileHandler(
        f"{log_dir}/pipeline.json",
        maxBytes=10 * 1024 * 1024,
        backupCount=5
    )
    json_handler.setLevel(logging.INFO)
    json_formatter = jsonlogger.JsonFormatter(
        '%(asctime)s %(name)s %(levelname)s %(message)s'
    )
    json_handler.setFormatter(json_formatter)
    logger.addHandler(json_handler)
    
    # Suppress noisy third-party loggers
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("selenium").setLevel(logging.WARNING)
    logging.getLogger("requests").setLevel(logging.WARNING)
    
    logging.info(f"Logging configured: level={log_level}, console={log_to_console}, file={log_to_file}")
```

### Step 3.2: Create Configuration Files

**File**: `config/sources.yaml`

```yaml
# Data source configurations

sources:
  fbref:
    base_url: "https://fbref.com"
    rate_limit:
      requests_per_minute: 20
      delay_between_requests: 3
    timeout: 30
    retry_attempts: 3
    user_agent: "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
    
  understat:
    base_url: "https://understat.com"
    rate_limit:
      requests_per_minute: 30
      delay_between_requests: 2
    timeout: 30
    retry_attempts: 3
    requires_selenium: false
    
  soccerway:
    base_url: "https://int.soccerway.com"
    rate_limit:
      requests_per_minute: 15
      delay_between_requests: 4
    timeout: 30
    retry_attempts: 3
    requires_selenium: false
    
  transfermarkt:
    base_url: "https://www.transfermarkt.com"
    rate_limit:
      requests_per_minute: 10
      delay_between_requests: 6
    timeout: 30
    retry_attempts: 3
    requires_selenium: false
    headers:
      User-Agent: "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"

# Priority order for data sources (in case of conflicts)
source_priority:
  - fbref
  - understat
  - soccerway
  - transfermarkt
```

**File**: `config/seasons.yaml`

```yaml
# Season configurations

seasons:
  - name: "2020-21"
    start_year: 2020
    end_year: 2021
    is_current: false
    scrape_priority: low
    
  - name: "2021-22"
    start_year: 2021
    end_year: 2022
    is_current: false
    scrape_priority: low
    
  - name: "2022-23"
    start_year: 2022
    end_year: 2023
    is_current: false
    scrape_priority: medium
    
  - name: "2023-24"
    start_year: 2023
    end_year: 2024
    is_current: false
    scrape_priority: high
    
  - name: "2024-25"
    start_year: 2024
    end_year: 2025
    is_current: true
    scrape_priority: critical

# Auto-detect current season based on date
auto_detect_current_season: true
```

**File**: `config/leagues.yaml`

```yaml
# League configurations

leagues:
  premier_league:
    name: "Premier League"
    country: "England"
    tier: 1
    num_teams: 20
    fbref_id: "9"
    understat_name: "EPL"
    transfermarkt_id: "GB1"
    scrape_enabled: true
    
  la_liga:
    name: "La Liga"
    country: "Spain"
    tier: 1
    num_teams: 20
    fbref_id: "12"
    understat_name: "La_liga"
    transfermarkt_id: "ES1"
    scrape_enabled: true
    
  serie_a:
    name: "Serie A"
    country: "Italy"
    tier: 1
    num_teams: 20
    fbref_id: "11"
    understat_name: "Serie_A"
    transfermarkt_id: "IT1"
    scrape_enabled: true
    
  bundesliga:
    name: "Bundesliga"
    country: "Germany"
    tier: 1
    num_teams: 18
    fbref_id: "20"
    understat_name: "Bundesliga"
    transfermarkt_id: "L1"
    scrape_enabled: true
    
  ligue_1:
    name: "Ligue 1"
    country: "France"
    tier: 1
    num_teams: 18
    fbref_id: "13"
    understat_name: "Ligue_1"
    transfermarkt_id: "FR1"
    scrape_enabled: true
```

**File**: `config/settings.yaml`

```yaml
# Global pipeline settings

pipeline:
  name: "Football Data Pipeline"
  version: "1.0.0"
  environment: "development"  # development, staging, production

database:
  host: "localhost"
  port: 5432
  name: "football_data"
  pool_size: 5
  max_overflow: 10

scraping:
  max_workers: 3  # Parallel scraping threads
  request_timeout: 30
  max_retries: 3
  backoff_factor: 2
  cache_enabled: true
  cache_ttl_hours: 24
  save_raw_html: true
  raw_data_dir: "data/raw"

data_processing:
  validate_on_load: true
  normalize_names: true
  handle_missing_values: "skip"  # skip, impute, error
  min_minutes_for_per90: 450  # Minimum minutes played for per-90 stats
  
scheduling:
  weekly_update:
    enabled: true
    day_of_week: "monday"
    time: "03:00"
    timezone: "UTC"
    
  matchday_update:
    enabled: true
    check_interval_hours: 6
    auto_detect_matchdays: true

logging:
  level: "INFO"
  console: true
  file: true
  json: true
  log_dir: "logs"

monitoring:
  enable_alerts: false
  alert_email: ""
  slack_webhook: ""
```

### Step 3.3: Create Configuration Loader

**File**: `config/config_loader.py`

```python
"""
Configuration loader for YAML config files.
"""

import yaml
from pathlib import Path
from typing import Dict, Any
import logging

logger = logging.getLogger(__name__)


class ConfigLoader:
    """Load and manage YAML configuration files."""
    
    def __init__(self, config_dir: str = "config"):
        """
        Initialize configuration loader.
        
        Args:
            config_dir: Directory containing config files
        """
        self.config_dir = Path(config_dir)
        self._configs: Dict[str, Any] = {}
        
    def load(self, config_name: str) -> Dict[str, Any]:
        """
        Load a configuration file.
        
        Args:
            config_name: Name of config file (without .yaml extension)
            
        Returns:
            Configuration dictionary
        """
        if config_name in self._configs:
            return self._configs[config_name]
        
        config_path = self.config_dir / f"{config_name}.yaml"
        
        try:
            with open(config_path, 'r') as f:
                config = yaml.safe_load(f)
            
            self._configs[config_name] = config
            logger.info(f"Loaded configuration: {config_name}")
            return config
            
        except FileNotFoundError:
            logger.error(f"Configuration file not found: {config_path}")
            raise
        except yaml.YAMLError as e:
            logger.error(f"Error parsing YAML file {config_path}: {e}")
            raise
    
    def get(self, config_name: str, *keys, default=None) -> Any:
        """
        Get a specific configuration value.
        
        Args:
            config_name: Name of config file
            *keys: Nested keys to access
            default: Default value if key not found
            
        Returns:
            Configuration value
            
        Example:
            config.get('settings', 'database', 'host')
        """
        config = self.load(config_name)
        
        value = config
        for key in keys:
            if isinstance(value, dict) and key in value:
                value = value[key]
            else:
                return default
        
        return value
    
    def reload(self, config_name: str):
        """Reload a configuration file."""
        if config_name in self._configs:
            del self._configs[config_name]
        return self.load(config_name)


# Global config instance
_config = ConfigLoader()


def get_config() -> ConfigLoader:
    """Get global config loader instance."""
    return _config
```

---

*[Due to length constraints, I'll continue with the next phases in a structured summary format]*

## PHASE 4-10 SUMMARY

### Phase 4: Scraper Modules
Create specialized scrapers for each data source:
- `scrapers/common/base_scraper.py` - Abstract base class with rate limiting
- `scrapers/fbref/` - FBref-specific scrapers (matches, players, teams)
- `scrapers/understat/` - Understat xG data scraper
- Each scraper implements retry logic, caching, and error handling

### Phase 5: Parser Layer
Build parsers to extract structured data:
- `parsers/match_parser.py` - Extract match data from HTML
- `parsers/player_parser.py` - Parse player statistics
- `parsers/team_parser.py` - Parse team data
- Handle inconsistent formats across sources

### Phase 6: Validation Layer
Implement data quality checks:
- `validators/schema_validation.py` - JSON schema validation
- `validators/consistency_checks.py` - Cross-field validation
- Ensure data integrity before database insertion

### Phase 7: ETL Pipeline
Core data transformation logic:
- `etl/extract.py` - Coordinate scraping operations
- `etl/transform.py` - Clean, normalize, compute per-90 metrics
- `etl/load.py` - Upsert data into PostgreSQL
- `etl/pipeline.py` - Orchestrate full ETL workflow

### Phase 8: CLI Interface
Command-line interface:
- `cli.py` - Click-based CLI with subcommands
- Commands: scrape, update, backfill, validate, status

### Phase 9: Scheduling
Automated execution:
- `scheduler/weekly_update.py` - Weekly league updates
- `scheduler/matchday_update.py` - Post-match updates
- Integration with cron/APScheduler

### Phase 10: Testing & Documentation
- Unit tests for each module
- Integration tests for full pipeline
- Comprehensive README with setup instructions

---

## EXECUTION CHECKLIST FOR LLM CODE EDITOR

### Initial Setup
```bash
□ Create project directory structure
□ Initialize git repository
□ Create virtual environment
□ Install dependencies from requirements.txt
□ Create .env file with database credentials
```

### Database Setup
```bash
□ Install PostgreSQL (if not installed)
□ Create database: createdb football_data
□ Run schema.sql to initialize tables
□ Test connection: python -c "from database.connection import get_db; print(get_db().test_connection())"
```

### Development Order
```
□ Phase 1: Project structure ✓
□ Phase 2: Database layer ✓
□ Phase 3: Core utilities ✓
□ Phase 4: Base scraper + 1 source (FBref)
□ Phase 5: Parsers for FBref data
□ Phase 6: Basic validation
□ Phase 7: ETL pipeline (extract → transform → load)
□ Phase 8: CLI interface
□ Test end-to-end with one season/league
□ Phase 4-5 (cont): Add remaining scrapers
□ Phase 9: Scheduling
□ Phase 10: Testing & docs
```

### Testing Strategy
```bash
□ Unit tests for each scraper
□ Parser tests with sample HTML fixtures
□ Validation tests with good/bad data
□ ETL integration test with test database
□ Full pipeline test: scrape → load → verify
```

---

## KEY DESIGN DECISIONS

1. **Separation of Concerns**: Scrapers don't know about database, parsers don't know about scrapers
2. **Idempotency**: All operations use UPSERT logic - safe to re-run
3. **Auditability**: Every scrape logged in `scrape_metadata` table
4. **Rate Limiting**: Respect website ToS, use exponential backoff
5. **Caching**: Save raw HTML to avoid re-scraping during development
6. **Per-90 Metrics**: Calculated during transformation for fair player comparison
7. **Source Priority**: FBref preferred, others used to fill gaps
8. **Modular**: Easy to add new leagues, sources, or seasons

---

## NEXT STEPS FOR IMPLEMENTATION

Ask your LLM code editor to:

1. **Start with Phase 1-3** (foundation layers)
2. **Implement one complete vertical slice**: FBref scraper → parser → ETL → database for Premier League 2023-24
3. **Test thoroughly** before expanding horizontally
4. **Add remaining sources** iteratively
5. **Implement scheduling** once core pipeline is stable

**Critical**: Test each phase before moving to the next. A solid foundation prevents cascading errors.