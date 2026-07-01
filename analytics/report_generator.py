"""
PDF Scouting Report Generator.
Uses mplsoccer + matplotlib to produce premium player scouting reports as PDFs.
Called by the /api/v1/players/{id}/report endpoint.
"""
import io
import logging
from datetime import datetime
from typing import Optional

import matplotlib
matplotlib.use('Agg')  # Non-interactive backend for server-side rendering
import matplotlib.pyplot as plt
import matplotlib.gridspec as gridspec
from matplotlib.backends.backend_pdf import PdfPages
import matplotlib.patches as mpatches
import numpy as np

from database.connection import query
from analytics.radar import compute_radar_data, RADAR_METRICS

logger = logging.getLogger(__name__)

# ── Design Tokens ──────────────────────────────────────────────────────────────
BG_DARK       = '#0D1117'
BG_CARD       = '#161B22'
ACCENT_GREEN  = '#00FFA3'
ACCENT_BLUE   = '#3B82F6'
ACCENT_ORANGE = '#F97316'
TEXT_PRIMARY  = '#F0F6FC'
TEXT_MUTED    = '#8B949E'
GRID_COLOR    = '#21262D'


def _fetch_player_profile(player_id: int) -> Optional[dict]:
    rows = query("""
        SELECT p.player_id, p.name, p.position_group, p.primary_position,
               p.nationality, p.date_of_birth, p.height_cm, p.preferred_foot,
               COUNT(pms.id) as matches,
               COALESCE(SUM(pms.goals), 0) as goals,
               COALESCE(SUM(pms.assists), 0) as assists,
               COALESCE(SUM(pms.minutes_played), 0) as minutes,
               ROUND(COALESCE(SUM(pms.xg), 0)::numeric, 2) as total_xg,
               ROUND(COALESCE(SUM(pms.xa), 0)::numeric, 2) as total_xa,
               pms.competition_name
        FROM players p
        JOIN player_match_stats pms ON p.player_id = pms.player_id
        WHERE p.player_id = %s
        GROUP BY p.player_id, pms.competition_name
        ORDER BY SUM(pms.minutes_played) DESC
        LIMIT 1
    """, (player_id,), as_dict=True)
    return rows[0] if rows else None


def _fetch_form(player_id: int, n: int = 20) -> list:
    return query("""
        SELECT match_label, competition_name, match_date,
               COALESCE(minutes_played, 0) as minutes_played,
               COALESCE(goals, 0) as goals,
               COALESCE(assists, 0) as assists,
               COALESCE(xg, 0) as xg,
               COALESCE(xa, 0) as xa,
               COALESCE(shots, 0) as shots,
               COALESCE(key_passes, 0) as key_passes,
               COALESCE(dribbles_successful, 0) as dribbles_successful,
               COALESCE(touches_in_box, 0) as touches_in_box
        FROM player_match_stats
        WHERE player_id = %s AND match_date IS NOT NULL
        ORDER BY match_date DESC
        LIMIT %s
    """, (player_id, n), as_dict=True)


def _fetch_season_stats(player_id: int) -> list:
    return query("""
        SELECT competition_name,
               COUNT(*)::int as matches,
               SUM(minutes_played)::int as minutes,
               COALESCE(SUM(goals), 0)::int as goals,
               COALESCE(SUM(assists), 0)::int as assists,
               ROUND(COALESCE(SUM(xg), 0)::numeric, 2)::float as xg,
               ROUND(COALESCE(SUM(xa), 0)::numeric, 2)::float as xa,
               ROUND((COALESCE(SUM(goals), 0)::numeric / NULLIF(SUM(minutes_played), 0) * 90), 2)::float as goals_p90,
               ROUND((COALESCE(SUM(xg), 0)::numeric / NULLIF(SUM(minutes_played), 0) * 90), 2)::float as xg_p90,
               ROUND((COALESCE(SUM(passes_accurate), 0)::numeric / NULLIF(SUM(passes), 0) * 100), 1)::float as pass_acc
        FROM player_match_stats
        WHERE player_id = %s
        GROUP BY competition_name
        ORDER BY minutes DESC
    """, (player_id,), as_dict=True)


def _draw_header(fig, profile: dict, ax_header):
    """Draw the premium header section with player name and key bio info."""
    ax_header.set_facecolor(BG_CARD)
    ax_header.set_xlim(0, 1)
    ax_header.set_ylim(0, 1)
    ax_header.axis('off')

    # Accent bar on the left
    ax_header.add_patch(mpatches.FancyBboxPatch(
        (0, 0), 0.004, 1, color=ACCENT_GREEN,
        boxstyle='square,pad=0'
    ))

    # Player name
    ax_header.text(0.02, 0.72, profile['name'],
                   fontsize=22, fontweight='bold',
                   color=TEXT_PRIMARY, transform=ax_header.transAxes, va='top')

    # Position badge
    pos_label = f"{profile.get('primary_position', '')}  ·  {profile.get('position_group', '')}"
    ax_header.text(0.02, 0.42, pos_label,
                   fontsize=10, color=ACCENT_GREEN,
                   transform=ax_header.transAxes, va='top')

    # Bio info row
    age_str = ''
    if profile.get('date_of_birth'):
        try:
            dob = profile['date_of_birth']
            age = (datetime.now().date() - dob).days // 365
            age_str = f"Age: {age}"
        except Exception:
            pass

    bio_parts = [
        profile.get('nationality', 'N/A'),
        age_str,
        f"{profile.get('height_cm', '?')} cm",
        f"Foot: {profile.get('preferred_foot', '?').capitalize() if profile.get('preferred_foot') else '?'}"
    ]
    bio_str = '   |   '.join([p for p in bio_parts if p])
    ax_header.text(0.02, 0.18, bio_str,
                   fontsize=9, color=TEXT_MUTED,
                   transform=ax_header.transAxes, va='top')

    # Key stats on the right
    stats_data = [
        ('Goals', str(profile.get('goals', 0))),
        ('Assists', str(profile.get('assists', 0))),
        ('xG', str(profile.get('total_xg', 0))),
        ('Minutes', f"{profile.get('minutes', 0):,}"),
        ('Matches', str(profile.get('matches', 0))),
    ]
    x_start = 0.6
    for i, (label, val) in enumerate(stats_data):
        x_pos = x_start + i * 0.08
        ax_header.text(x_pos, 0.72, val, fontsize=14, fontweight='bold',
                       color=ACCENT_GREEN, transform=ax_header.transAxes,
                       ha='center', va='top')
        ax_header.text(x_pos, 0.35, label, fontsize=7.5, color=TEXT_MUTED,
                       transform=ax_header.transAxes, ha='center', va='top')

    # Competition badge
    comp = profile.get('competition_name', '')
    if comp:
        ax_header.text(0.98, 0.18, comp, fontsize=8, color=TEXT_MUTED,
                       transform=ax_header.transAxes, ha='right', va='top',
                       style='italic')


def _draw_radar(ax, radar_data: dict, pos_group: str):
    """Draw a spider/radar chart using matplotlib."""
    ax.set_facecolor(BG_CARD)

    metrics = list(radar_data.keys())
    values = [radar_data[m] / 100.0 for m in metrics]  # Normalise 0-1

    if not metrics:
        ax.text(0.5, 0.5, 'Insufficient data\nfor radar chart',
                ha='center', va='center', color=TEXT_MUTED, fontsize=10,
                transform=ax.transAxes)
        ax.axis('off')
        return

    N = len(metrics)
    angles = np.linspace(0, 2 * np.pi, N, endpoint=False).tolist()
    values_plot = values + [values[0]]
    angles_plot = angles + [angles[0]]

    ax = plt.subplot(ax.get_subplotspec(), polar=True)
    ax.set_facecolor(BG_CARD)

    # Draw grid rings
    for r in [0.2, 0.4, 0.6, 0.8, 1.0]:
        ax.plot(angles_plot, [r] * (N + 1), color=GRID_COLOR, linewidth=0.5, zorder=0)

    # Spoke lines
    for angle in angles:
        ax.plot([angle, angle], [0, 1], color=GRID_COLOR, linewidth=0.5, zorder=0)

    # Filled polygon
    ax.fill(angles_plot, values_plot, alpha=0.25, color=ACCENT_GREEN)
    ax.plot(angles_plot, values_plot, color=ACCENT_GREEN, linewidth=2)

    # Dots at each vertex
    ax.scatter(angles, values, color=ACCENT_GREEN, s=40, zorder=5)

    # Labels
    ax.set_xticks(angles)
    ax.set_xticklabels(metrics, color=TEXT_PRIMARY, fontsize=8.5, fontweight='bold')
    ax.set_yticks([])
    ax.set_ylim(0, 1)
    ax.spines['polar'].set_visible(False)

    ax.set_title('Percentile Ranking vs Position Peers',
                 color=TEXT_PRIMARY, fontsize=10, fontweight='bold', pad=15)


def _draw_form_chart(ax, form_data: list):
    """Draw bar chart of goals + xG per match for the last N games."""
    ax.set_facecolor(BG_CARD)

    if not form_data:
        ax.text(0.5, 0.5, 'No recent match data available',
                ha='center', va='center', color=TEXT_MUTED, fontsize=10,
                transform=ax.transAxes)
        ax.axis('off')
        return

    # Reverse to chronological order
    form_reversed = list(reversed(form_data))
    n = len(form_reversed)
    x = np.arange(n)

    goals = [m['goals'] for m in form_reversed]
    xg    = [float(m['xg']) for m in form_reversed]
    labels = [str(m['match_date'])[-5:] if m['match_date'] else '' for m in form_reversed]

    bars = ax.bar(x, goals, color=ACCENT_GREEN, alpha=0.85, label='Goals', zorder=3, width=0.5)
    ax.plot(x, xg, color=ACCENT_ORANGE, linewidth=2, marker='o', markersize=4,
            label='xG', zorder=4)

    ax.fill_between(x, xg, alpha=0.1, color=ACCENT_ORANGE)

    ax.set_xticks(x)
    ax.set_xticklabels(labels, rotation=45, ha='right', color=TEXT_MUTED, fontsize=7)
    ax.tick_params(colors=TEXT_MUTED)
    ax.set_facecolor(BG_CARD)
    ax.set_title('Recent Form — Goals vs xG', color=TEXT_PRIMARY,
                 fontsize=10, fontweight='bold', pad=8)
    ax.spines[['top', 'right', 'left', 'bottom']].set_color(GRID_COLOR)
    ax.tick_params(axis='y', colors=TEXT_MUTED)
    ax.yaxis.set_tick_params(labelcolor=TEXT_MUTED)
    ax.set_ylabel('Goals / xG', color=TEXT_MUTED, fontsize=8)
    ax.legend(facecolor=BG_CARD, edgecolor=GRID_COLOR,
              labelcolor=TEXT_PRIMARY, fontsize=8, loc='upper right')
    ax.grid(axis='y', color=GRID_COLOR, linewidth=0.5, zorder=0)


def _draw_season_table(ax, season_stats: list):
    """Draw a clean stats table per competition."""
    ax.set_facecolor(BG_CARD)
    ax.axis('off')

    if not season_stats:
        ax.text(0.5, 0.5, 'No season data available',
                ha='center', va='center', color=TEXT_MUTED, fontsize=10,
                transform=ax.transAxes)
        return

    ax.set_title('Season Statistics by Competition',
                 color=TEXT_PRIMARY, fontsize=10, fontweight='bold', pad=8,
                 loc='left')

    headers = ['Competition', 'Matches', 'Minutes', 'Goals', 'Assists',
               'xG', 'xA', 'G/90', 'xG/90', 'Pass%']

    table_data = []
    for row in season_stats:
        table_data.append([
            row.get('competition_name', ''),
            str(row.get('matches', 0)),
            str(row.get('minutes', 0)),
            str(row.get('goals', 0)),
            str(row.get('assists', 0)),
            f"{row.get('xg', 0):.2f}",
            f"{row.get('xa', 0):.2f}",
            f"{row.get('goals_p90', 0):.2f}",
            f"{row.get('xg_p90', 0):.2f}",
            f"{row.get('pass_acc', 0):.1f}%",
        ])

    col_widths = [0.28, 0.07, 0.08, 0.06, 0.07, 0.06, 0.06, 0.07, 0.07, 0.07]

    tbl = ax.table(
        cellText=table_data,
        colLabels=headers,
        cellLoc='center',
        loc='upper center',
        colWidths=col_widths,
    )
    tbl.auto_set_font_size(False)
    tbl.set_fontsize(7.5)

    for (row_idx, col_idx), cell in tbl.get_celld().items():
        cell.set_facecolor(BG_DARK if row_idx % 2 == 0 else BG_CARD)
        cell.set_edgecolor(GRID_COLOR)
        if row_idx == 0:
            cell.set_facecolor('#1F2937')
            cell.set_text_props(color=ACCENT_GREEN, fontweight='bold')
        else:
            cell.set_text_props(color=TEXT_PRIMARY)


def generate_player_report(player_id: int) -> bytes:
    """
    Generate a full PDF scouting report for a player.
    Returns raw PDF bytes ready to be streamed as HTTP response.
    """
    profile = _fetch_player_profile(player_id)
    if not profile:
        raise ValueError(f"Player {player_id} not found or has no match data.")

    form_data   = _fetch_form(player_id, n=20)
    season_data = _fetch_season_stats(player_id)
    radar_data  = compute_radar_data(player_id, min_minutes=450)

    buf = io.BytesIO()

    with PdfPages(buf) as pdf:
        fig = plt.figure(figsize=(16, 11), facecolor=BG_DARK)

        gs = gridspec.GridSpec(
            3, 2,
            figure=fig,
            hspace=0.45,
            wspace=0.35,
            left=0.04, right=0.96,
            top=0.93, bottom=0.06,
            height_ratios=[0.16, 0.46, 0.38]
        )

        # ── Header ──────────────────────────────────────────────────────────
        ax_header = fig.add_subplot(gs[0, :])
        _draw_header(fig, profile, ax_header)

        # ── Radar Chart ─────────────────────────────────────────────────────
        ax_radar = fig.add_subplot(gs[1, 0], polar=True)
        _draw_radar(ax_radar, radar_data, profile.get('position_group', 'FWD'))

        # ── Form Chart ──────────────────────────────────────────────────────
        ax_form = fig.add_subplot(gs[1, 1])
        _draw_form_chart(ax_form, form_data)

        # ── Season Stats Table ───────────────────────────────────────────────
        ax_table = fig.add_subplot(gs[2, :])
        _draw_season_table(ax_table, season_data)

        # ── Footer ───────────────────────────────────────────────────────────
        fig.text(0.04, 0.01, f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M')}",
                 fontsize=7, color=TEXT_MUTED)
        fig.text(0.96, 0.01, "football-data-platform  ·  Data: Wyscout",
                 fontsize=7, color=TEXT_MUTED, ha='right')

        pdf.savefig(fig, facecolor=BG_DARK, dpi=150)
        plt.close(fig)

        # PDF metadata
        d = pdf.infodict()
        d['Title'] = f"Scout Report — {profile['name']}"
        d['Author'] = 'football-data-platform'
        d['Subject'] = 'Player Scouting Report'
        d['Creator'] = 'mplsoccer + matplotlib'

    buf.seek(0)
    return buf.read()
