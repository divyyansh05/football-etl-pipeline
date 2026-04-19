# Football Analytics Domain Knowledge

## Key Metrics

xG (Expected Goals)
  Probability a shot results in a goal.
  Good attacker: > 0.4/90. Elite: > 0.6/90.

xA (Expected Assists)
  Probability a pass leads to a shot that goes in.
  Good creator: > 0.15/90. Elite: > 0.25/90.

npxG (Non-Penalty Expected Goals)
  xG excluding penalties. Better for open play threat.

PPDA (Passes Per Defensive Action)
  Lower = more intense pressing.
  Elite pressing: < 7. Passive: > 12.

Progressive passes/carries
  Actions moving ball significantly toward opponent goal.
  Key modern scouting metric.

xg_save (Expected Goals Saved)
  How many goals a GK prevented above average.
  Core GK evaluation metric.

## Position Groups
  GK  → Goalkeeper
  DEF → Defender (CB, LB, RB, LWB, RWB)
  MID → Midfielder (CDM, CM, CAM, LM, RM)
  FWD → Forward (CF, ST, LW, RW, SS)

## Minimum Sample Threshold
  450 minutes = ~5 full matches minimum for statistical relevance.
  Players below this threshold: visible but not scored.

## Per-90 Normalisation
  Formula: (stat / minutes_played) * 90
  Apply to all counting stats.
  Rate stats (%, ratios): do not normalise.

## League Difficulty
  Not applied currently. All leagues treated equally.
  Future: apply difficulty multipliers for cross-league comparison.
