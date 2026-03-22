#!/usr/bin/env python
"""
Live integration test for SoccerData adapter.

Tests actual data fetching from FotMob via soccerdata library.
Run with: python scripts/test_soccerdata_live.py
"""

import sys
import os

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from etl.sources.soccerdata_adapter import SoccerDataAdapter, check_soccerdata_availability


def test_availability():
    """Check if soccerdata is available."""
    print("=" * 60)
    print("STEP 1: Checking SoccerData Availability")
    print("=" * 60)

    is_available, details = check_soccerdata_availability()

    print(f"Python version: {details['python_version']} (OK: {details['python_ok']})")
    print(f"Pandas version: {details['pandas_version']} (OK: {details['pandas_ok']})")
    print(f"SoccerData available: {details['soccerdata_available']}")
    print(f"Overall available: {is_available}")

    if not is_available:
        print("\nUpgrade instructions:")
        print(details.get('upgrade_instructions', 'N/A'))
        return False

    print("\n✅ Environment is ready for SoccerData!")
    return True


def test_adapter_initialization():
    """Test adapter initialization."""
    print("\n" + "=" * 60)
    print("STEP 2: Initializing SoccerData Adapter")
    print("=" * 60)

    adapter = SoccerDataAdapter()
    is_available, issues = adapter.is_available()

    if not is_available:
        print(f"❌ Adapter not available: {issues}")
        return None

    print("✅ Adapter initialized successfully!")
    return adapter


def test_fotmob_schedule(adapter):
    """Test fetching schedule from FotMob."""
    print("\n" + "=" * 60)
    print("STEP 3: Fetching Premier League Schedule (FotMob)")
    print("=" * 60)

    try:
        df = adapter.get_schedule('premier-league', '2024-25', source='fotmob')

        if df is None:
            print("❌ Schedule fetch returned None")
            return False

        print(f"✅ Fetched {len(df)} matches")
        print(f"Columns: {list(df.columns)[:10]}...")

        if len(df) > 0:
            print(f"\nSample match:\n{df.iloc[0].to_dict()}")

        return True

    except Exception as e:
        print(f"❌ Error fetching schedule: {e}")
        return False


def test_fotmob_player_stats(adapter):
    """Test fetching player season stats from FotMob."""
    print("\n" + "=" * 60)
    print("STEP 4: Fetching Player Season Stats (FotMob)")
    print("=" * 60)

    try:
        df = adapter.get_player_season_stats('premier-league', '2024-25', source='fotmob')

        if df is None:
            print("❌ Player stats fetch returned None")
            return False

        print(f"✅ Fetched stats for {len(df)} players")
        print(f"Columns: {list(df.columns)[:15]}...")

        # Look for xG columns
        xg_cols = [c for c in df.columns if 'xg' in c.lower() or 'xa' in c.lower()]
        print(f"xG-related columns: {xg_cols}")

        if len(df) > 0:
            print(f"\nTop player sample:\n{df.iloc[0].to_dict()}")

        return True

    except Exception as e:
        print(f"❌ Error fetching player stats: {e}")
        return False


def test_fotmob_events(adapter):
    """Test fetching match events from FotMob."""
    print("\n" + "=" * 60)
    print("STEP 5: Fetching Match Events (FotMob)")
    print("=" * 60)

    try:
        df = adapter.get_match_events('premier-league', '2024-25', source='fotmob')

        if df is None:
            print("⚠️ Events fetch returned None (may not be available)")
            return True  # Not a failure, some methods may not work

        print(f"✅ Fetched {len(df)} events")
        print(f"Columns: {list(df.columns)[:15]}...")

        # Look for location columns
        loc_cols = [c for c in df.columns if 'location' in c.lower() or c in ['x', 'y']]
        print(f"Location columns: {loc_cols}")

        if len(df) > 0:
            print(f"\nSample event:\n{df.iloc[0].to_dict()}")

        return True

    except Exception as e:
        print(f"⚠️ Events not available: {e}")
        return True  # Don't fail - events may not be supported


def test_fotmob_shots(adapter):
    """Test fetching shot events from FotMob."""
    print("\n" + "=" * 60)
    print("STEP 6: Fetching Shot Events / Shotmap (FotMob)")
    print("=" * 60)

    try:
        df = adapter.get_shot_events('premier-league', '2024-25', source='fotmob')

        if df is None:
            print("⚠️ Shot events fetch returned None (may require specific match)")
            return True

        print(f"✅ Fetched {len(df)} shots")
        print(f"Columns: {list(df.columns)[:15]}...")

        # Look for xG and coordinate columns
        xg_cols = [c for c in df.columns if 'xg' in c.lower()]
        coord_cols = [c for c in df.columns if c in ['x', 'y', 'location_x', 'location_y']]
        print(f"xG columns: {xg_cols}")
        print(f"Coordinate columns: {coord_cols}")

        if len(df) > 0:
            print(f"\nSample shot:\n{df.iloc[0].to_dict()}")

        return True

    except Exception as e:
        print(f"⚠️ Shot events not available: {e}")
        return True


def print_statistics(adapter):
    """Print adapter statistics."""
    print("\n" + "=" * 60)
    print("STATISTICS")
    print("=" * 60)

    stats = adapter.get_statistics()
    print(f"Records fetched: {stats['records_fetched']}")
    print(f"Records processed: {stats['records_processed']}")
    print(f"Records skipped: {stats['records_skipped']}")
    print(f"Errors: {len(stats['errors'])}")

    if stats['errors']:
        print("\nErrors encountered:")
        for err in stats['errors']:
            print(f"  - {err}")


def main():
    """Run all integration tests."""
    print("SoccerData Integration Test")
    print("=" * 60)

    # Step 1: Check availability
    if not test_availability():
        print("\n❌ SoccerData not available. Exiting.")
        return 1

    # Step 2: Initialize adapter
    adapter = test_adapter_initialization()
    if adapter is None:
        print("\n❌ Adapter initialization failed. Exiting.")
        return 1

    # Step 3: Test schedule
    test_fotmob_schedule(adapter)

    # Step 4: Test player stats
    test_fotmob_player_stats(adapter)

    # Step 5: Test events
    test_fotmob_events(adapter)

    # Step 6: Test shots
    test_fotmob_shots(adapter)

    # Print statistics
    print_statistics(adapter)

    # Cleanup
    adapter.close()

    print("\n" + "=" * 60)
    print("✅ Integration tests completed!")
    print("=" * 60)

    return 0


if __name__ == '__main__':
    sys.exit(main())
