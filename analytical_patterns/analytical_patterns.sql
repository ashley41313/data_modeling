/*
================================================================================
Applying Analytical Patterns
--------------------------------------------------------------------------------
Input Tables:
  - players
  - players_scd
  - player_seasons
  - game_details
  - games
  - teams

Output Tables:
  - players_retirement_status_tracking (created in this script)
  - grouped_tbl (CTE used for GROUPING SETS queries)

Tasks Covered:
  1. Track player state changes across seasons
  2. Use GROUPING SETS to answer analytical questions
  3. Apply window functions to analyze win streaks 
================================================================================
*/

-- ============================================================================
-- 1. A query that does state change tracking for players
-- This captures New, Retired, Continued Playing, Returned from Retirement, and Stayed Retired statuses
-- ============================================================================

CREATE TABLE players_retirement_status_tracking (
    player_name TEXT,
    first_active_year INTEGER,          -- First year the player entered league
    last_active_year INTEGER,           -- Most recent year the player was active
    yearly_active_state TEXT,           -- State change classification
    years_active INTEGER[],             -- Array of all years active
    year INTEGER,                       -- The season being recorded
    PRIMARY KEY (player_name, year)
);


-- We start from 1996 as a seed year and iterate through 1997 onward

WITH yesterday AS (
    SELECT * FROM players_retirement_status_tracking
    WHERE year = 1996
),

    today AS (
        SELECT 
            player_name,
            current_season,
            is_active
        FROM players
        WHERE current_season = 1997
    )

INSERT INTO players_retirement_status_tracking

SELECT 
    COALESCE(t.player_name, y.player_name) AS player_name,
    COALESCE(y.first_active_year, t.current_season) AS first_active_year,
	CASE WHEN t.is_active = 'true' THEN t.current_season ELSE y.last_active_year END AS last_active_year,
	CASE 
        WHEN y.player_name IS NULL AND t.is_active = 'true' THEN 'New'
        WHEN t.current_season = y. last_active_year + 1 AND t.is_active = 'false'  THEN 'Retired'
        WHEN y.last_active_year = t.current_season - 1 AND t.is_active = 'true' THEN 'Continued Playing'
        WHEN y.last_active_year < t.current_season - 1 AND t.is_active = 'true' THEN 'Returned from Retirement'
        WHEN t.current_season > y. last_active_year + 1 AND t.is_active = 'false'  THEN 'Stayed Retired'
        ELSE 'Stale'
    END AS yearly_active_state,
	CASE 
        WHEN t.is_active = 'true' AND y.years_active IS NULL 
            THEN ARRAY[t.current_season]
		WHEN t.is_active = 'true' AND y.years_active IS NOT NULL 
			THEN y.years_active || ARRAY[t.current_season]
		WHEN t.is_active = 'false' AND y.years_active IS NOT NULL 
			THEN y.years_active
    END AS years_active,
    COALESCE(t.current_season, y.year + 1) AS year
FROM today t 
FULL OUTER JOIN yesterday y 
ON t.player_name = y.player_name


-- ============================================================================
-- 2. Queries using GROUPING SETS to efficiently aggregate game_details
-- Aggregates across (player, team), (player, season), and (team) levels
-- ============================================================================

-- derive all of the columns we need from the game_details, teams, and games tables

WITH combined_tbl AS (

	SELECT  g.game_id as game_id,
			gd.player_id as player_id, 
			gd.player_name as player_name,
			gd.team_id as team_id, 
			t.nickname as team_name,
			g.season as season, 
			gd.pts as pts,
			g.home_team_id,
			
			CASE 
				WHEN g.home_team_id = gd.team_id AND g.home_team_wins = 1
					THEN 1
				ELSE 0
				
			END AS player_wins,
			ROW_NUMBER() OVER(PARTITION BY g.game_id, gd.team_id ORDER BY gd.player_id) AS win_row_number
			
		FROM game_details gd
		LEFT JOIN games g ON gd.game_id = g.game_id
		LEFT JOIN teams t ON gd.team_id = t.team_id
), 

-- add another column that will only be specific to (game,team) level aggregation, will only
-- have 1 win per game per team after aggregating the column

	filtered AS (
		SELECT game_id, player_id, player_name, team_id, team_name, season, pts, player_wins,
			CASE
				WHEN win_row_number = 1 THEN player_wins
				ELSE 0
			END AS team_win
		FROM combined_tbl
	),
	
	grouped_tbl AS (

		SELECT
			player_id, 
			team_id, 
			season, 
			MAX(team_name) as team_name,
			MAX(player_name) as player_name,
			SUM(pts) as total_points,
			SUM(player_wins) as total_player_wins,
			SUM(team_win) as total_team_wins
			
		FROM filtered 
		GROUP BY GROUPING SETS (
			(player_id, team_id),
			(player_id, season),
			(team_id)
		)
)


-- ============================================================================
-- Query: Which team won the most games?
-- ============================================================================
SELECT 
	team_id, 
	team_name,
	total_team_wins
FROM grouped_tbl 
WHERE player_id IS NULL AND season IS NULL 
ORDER BY total_team_wins DESC
LIMIT 1

-- ============================================================================
-- Query: Who scored the most points playing for one team?
-- ============================================================================
SELECT 
	player_name, player_id, team_id, total_points
FROM grouped_tbl 
WHERE season IS NULL 
	and total_points IS NOT NULL
	and player_id IS NOT NULL
ORDER BY total_points DESC
LIMIT 1


-- ============================================================================
-- Query: Who scored the most points in one season?
-- ============================================================================
SELECT player_id, player_name
FROM grouped_tbl 
WHERE team_id IS NULL AND total_points IS NOT NULL
ORDER BY total_points DESC
LIMIT 1



/*
===============================================================================
Section: Window Function Queries on `game_details`
-------------------------------------------------------------------------------
This section addresses two analytical questions:

1. What is the most games a team has won in a 90-game stretch?

2. How many games in a row did LeBron James score over 10 points a game?

===============================================================================
*/

-- ============================================================================
-- 1. What is the most games a team has won in a 90-game stretch?
-- ============================================================================

with game_team_wins as (
	select 
		gd.game_id, gd.team_id, gd.team_city,
		max(g.game_date_est) as date, 
		case 
			when MAX(gd.team_id) = MAX(g.home_team_id)
				THEN MAX(g.home_team_wins)
			else 0
		END AS team_wins
	from game_details gd 
	left join games g 
	on gd.game_id = g.game_id
	group by gd.game_id, gd.team_id, gd.team_city
)

SELECT 
	team_id,
	SUM(team_wins) 
		OVER (PARTITION BY team_id ORDER BY date ROWS BETWEEN 89 PRECEDING AND CURRENT ROW) 
		as wins_in_last_90_days
from game_team_wins
order by wins_in_last_90_days desc
limit 1


-- ============================================================================
-- 2. How many games in a row did LeBron James score over 10 points a game?
-- ============================================================================

WITH game_points_status AS (
	SELECT 
		g.game_date_est AS date,
		gd.game_id, gd.player_name, gd.pts,
		CASE 
			WHEN gd.pts > 10 THEN 1
			ELSE 0
		END AS scored_over_10_pts
	FROM game_details gd 
	LEFT JOIN games g 
	ON gd.game_id = g.game_id
	WHERE player_name = 'LeBron James'
	AND pts IS NOT NULL
),

	grouped AS (
		select 
			date, player_name, scored_over_10_pts,
			SUM( case when scored_over_10_pts != 1 then 1 else 0 end) OVER (ORDER BY date) as group_num
		FROM game_points_status
	)

-- Count how many games are in each streak group where he scored >10 points

select group_num as winning_streak, count(*) as best_win_streak
from grouped
group by group_num
order by 2 desc 
limit 1