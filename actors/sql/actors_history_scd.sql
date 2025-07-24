-- Notes:
--   - This is a backfill query that can populate the entire actors_history_scd
--     table in a single query by analyzing all historical years.
--   - It uses window functions and grouping to detect changes and create
--     historical versioned records.


-- DDL for actors_history_scd table
-- This table models a Type 2 slowly changing dimension by capturing
-- each period where an actor's `quality_class` or `is_active` value remains unchanged.
-- New records are created only when those values change.
CREATE TABLE actors_history_scd ( 
	actorid TEXT,
	actor TEXT,
	quality_class quality_class, 		-- ENUM type defined previously
	is_active BOOLEAN,
	start_year INTEGER, 				-- Start of this status period
	end_year INTEGER, 					-- End of this status period
	current_year INTEGER, 				-- Current snapshot year (can be useful for context or filtering)
	PRIMARY KEY(actorid, start_year)
)

-- ===========================================
-- Backfill query to populate the SCD table
-- ===========================================
INSERT INTO actors_history_scd

-- Step 1: Add lagged values of key attributes to detect changes
WITH with_previous AS (
		SELECT 
			actor, 
			actorid,
			current_year,
			quality_class, 
			is_active,
			LAG(quality_class, 1) OVER (PARTITION BY actorid ORDER BY current_year) as previous_quality_class,
			LAG(is_active, 1) OVER (PARTITION BY actorid ORDER BY current_year) as previous_is_active
		FROM actors

    -- Filter to a known full data range to avoid incomplete edge cases
		WHERE current_year <= 2021
	),

-- Step 2: Flag rows where the quality_class or is_active value has changed
	with_indicators AS ( 
		SELECT *, 
				CASE 
					WHEN quality_class <> previous_quality_class THEN 1
					WHEN is_active <> previous_is_active THEN 1
					ELSE 0
				END AS change_indicator
		FROM with_previous
	),

-- Step 3: Assign a streak identifier that groups unchanged sequences together
-- This works like a "sessionization" by change in values
		with_streaks AS (
		SELECT *, 
				SUM(change_indicator) 
					OVER (PARTITION BY actor ORDER BY current_year) as streak_identifier 
		FROM with_indicators
	)

-- Final SELECT: Collapse each streak into a single SCD row
-- MIN and MAX give the start and end years of each unchanged period
SELECT 	actorid,
		MAX(actor) AS actor, -- actor name might be repeated, so just take one
		quality_class,
		is_active,
		MIN(current_year) as start_year,
		MAX(current_year) as end_year,

    	-- Hardcoded current_year; in production this could be passed as a variable
		2021 AS current_year
FROM with_streaks

-- Grouping by actor and streak captures each "version" of the dimension
GROUP BY actorid, streak_identifier, is_active, quality_class
ORDER BY actorid, streak_identifier
