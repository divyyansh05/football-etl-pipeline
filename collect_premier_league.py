"""
Collect Premier League 2024-25 data using API-Football.

This script uses API-Football to get real data for the Premier League 2024-25 season.
You'll need an API key from https://rapidapi.com/api-sports/api/api-football

Free tier: 100 requests/day
"""

import sys
import os
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent))

from etl.api_football_etl import APIFootballETL
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn
from rich.table import Table

console = Console()


def collect_premier_league_data(api_key: str = None):
    """
    Collect Premier League 2024-25 data.
    
    Args:
        api_key: API-Football API key (get from RapidAPI)
    """
    console.print("\n[bold blue]Collecting Premier League 2024-25 Data[/bold blue]\n")
    
    if not api_key or api_key == "DEMO_KEY":
        console.print("[yellow]⚠️  Using DEMO_KEY - Get your free API key from:[/yellow]")
        console.print("[cyan]   https://rapidapi.com/api-sports/api/api-football[/cyan]\n")
        console.print("[yellow]   Free tier: 100 requests/day[/yellow]\n")
        
        # Ask user for API key
        api_key_input = input("Enter your API key (or press Enter to use demo): ").strip()
        if api_key_input:
            api_key = api_key_input
    
    try:
        etl = APIFootballETL(api_key=api_key)
        
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            console=console
        ) as progress:
            task = progress.add_task("Collecting data from API-Football...", total=None)
            
            # Process Premier League 2024-25
            stats = etl.process_league_season('premier-league', 2024)
            
            progress.update(task, completed=True)
        
        # Display results
        console.print("\n[bold green]✓ Data collection complete![/bold green]\n")
        
        table = Table(title="Collection Statistics", show_header=True)
        table.add_column("Metric", style="cyan")
        table.add_column("Count", justify="right", style="green")
        
        table.add_row("Teams", str(stats.get('teams', 0)))
        table.add_row("Team Season Stats", str(stats.get('team_season_stats', 0)))
        table.add_row("Matches", str(stats.get('matches', 0)))
        table.add_row("Player Season Stats", str(stats.get('player_season_stats', 0)))
        
        console.print(table)
        
        console.print("\n[bold green]✓ Premier League 2024-25 data loaded successfully![/bold green]")
        console.print("\n[cyan]Next step: Run the UI to view the data[/cyan]")
        console.print("[cyan]  python server/app.py[/cyan]\n")
        
        return True
        
    except Exception as e:
        console.print(f"\n[bold red]✗ Error: {e}[/bold red]\n")
        import traceback
        traceback.print_exc()
        return False


if __name__ == "__main__":
    # Check for API key in environment
    api_key = os.getenv('API_FOOTBALL_KEY', 'DEMO_KEY')
    
    success = collect_premier_league_data(api_key)
    
    if success:
        console.print("=" * 60)
        console.print("[bold green]SUCCESS![/bold green]")
        console.print("=" * 60)
    else:
        console.print("=" * 60)
        console.print("[bold red]FAILED[/bold red]")
        console.print("=" * 60)
        sys.exit(1)
