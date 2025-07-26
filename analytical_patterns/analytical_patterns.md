# Week 4: Applying Analytical Patterns

This assignment explores key SQL analytical patterns using window functions, `GROUPING SETS`, and state change tracking. The goal is to gain insight into player and team performance by writing efficient, readable queries on NBA game data.

## 📁 Folder Structure

- `analytical-patterns/`
  - `analytical-patterns.sql` — Main SQL script with all analytical queries
  - `tables/` — Folder containing input and output data files
    - `players.csv` — Input table: player status by season
    - `players_scd.csv` — Input table: player career history (SCD format)
    - `player_seasons.csv` — Input table: player-to-team-season mapping
    - `games.csv` — Input table: game metadata
    - `game_details.csv` — Input table: individual player stats per game
    - `teams.csv` — Input table: team metadata
    - `players_retirement_status_tracking.csv` — Output: player career state tracking


## 🧠 Assignment Overview

This assignment is split into three main parts:

### 1. **Player State Change Tracking**
Using data from `players`, track the seasonal status of each player:
- `New`: Player enters league for the first time
- `Retired`: Player becomes inactive
- `Continued Playing`: Active in consecutive seasons
- `Returned from Retirement`: Active again after break
- `Stayed Retired`: Inactive and stays out of the league

👉 Output: `players_retirement_status_tracking`

---

### 2. **Using GROUPING SETS for Efficient Aggregations**
Aggregate player and team performance across different dimensions from `game_details` and `games`:
- (player, team): Who scored the most points for a single team?
- (player, season): Who scored the most points in a season?
- (team): Which team won the most games?

👉 Intermediate Output: `grouped_tbl` CTE used for multiple queries

---

### 3. **Window Function Analysis**
Apply advanced window functions to answer:
- 🏆 *"What is the most games a team has won in a 90-game stretch?"*
- 🔥 *"What is the longest streak of LeBron James scoring over 10 points?"*

These use rolling windows and "gaps & islands" techniques for sequence analysis.

---

## 🛠 How to Use

1. Ensure the required input tables (`tables/*.csv`) are loaded into your SQL engine.
2. Open and run the queries in `analytical-patterns.sql` sequentially.
3. Review outputs for each analytical section or redirect results to new output tables.

---

## 📌 Notes

- All logic is written in standard SQL and should work with engines like PostgreSQL or SQLite.
- You can modify the seed year or extend the tracking years as needed.
- Filtering by specific players (e.g., LeBron James) is case-sensitive depending on the database.

---
