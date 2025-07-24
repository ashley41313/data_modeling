/* 
============================================================
  Script: incremental_update_actors_history_scd.sql

  Purpose:
    - Incrementally update the actors_history_scd table with new data from the actors table
    - Combines last year's SCD snapshot with current year incoming data
    - Handles unchanged, changed, and new actor records using UNION ALL

  Input Tables:
    - actors_history_scd (SCD Type 2 dimension table of actor historical data)
    - actors (latest cumulative actor data per year)

  Output Tables:
    - actors_history_scd (updated with incremental changes for current year)

  Notes:
    - This is an incremental query that processes only the latest year
      of data, building on the previous year's SCD snapshot.
    - It is more efficient for yearly batch loads but depends on the
      accuracy of the previous snapshot.
    - Not suitable as a backfill; backfills require full history processing.

  Author: Ashley Eclert
  Created: 2025-07-24
============================================================
*/

-- Composite type to represent an SCD record structure
CREATE TYPE actors_scd_type AS (
					quality_class quality_class,
					is_active boolean,
					start_year INTEGER,
					end_year INTEGER
)


-- Get the last year's final SCD records (snapshot for 2020)
WITH last_year_scd AS (
		SELECT * FROM actors_history_scd
		WHERE current_year = 2020
		AND end_year = 2020
	),

-- Historical SCD records for years before 2020 that remain unchanged
	historical_scd AS (
		SELECT
			actor,
			actorid,
			quality_class,
			is_active,
			start_year,
			end_year
		FROM actors_history_scd
		WHERE current_year = 2020
		AND end_year < 2020
	),
	
-- New incoming data for the current year (2021) from the actors table
	this_year_data AS (
		SELECT * FROM actors
		WHERE current_year = 2021
	),

-- Actors whose quality_class and is_active did NOT change from last year
	unchanged_records AS (
		SELECT 
			ts.actorid,
			ts.actor,
			ts.quality_class,
			ts.is_active,
			ls.start_year,
			ts.current_year as end_year
		FROM this_year_data ts
		JOIN last_year_scd ls
		ON ls.actorid = ts.actorid
			WHERE ts.quality_class = ls.quality_class
			AND ts.is_active = ls.is_active
	),

-- Actors who have changed status or are completely new this year
	new_and_changed_records AS (
		SELECT 
			ts.actorid,
			ts.actor,
			ts.quality_class,
			ts.is_active,
			ls.start_year,
			ts.current_year as end_year
		FROM this_year_data ts
		LEFT JOIN last_year_scd ls
		ON ls.actorid = ts.actorid
		WHERE (ts.quality_class <> ls.quality_class
			OR ts.is_active <> ls.is_active)
			OR ls.actorid IS NULL
	),

-- Prepare two separate SCD records for changed actors:
-- one closing the previous period, one opening the new period
	changed_records AS (
		SELECT 
			ts.actorid,
			ts.actor,
			UNNEST(ARRAY[
				ROW(
					ls.quality_class, 
					ls.is_active,
					ls.start_year,
					ls.end_year
					
					)::actors_scd_type,
				ROW(
					ts.quality_class, 
					ts.is_active,
					ts.current_year,
					ts.current_year
					)::actors_scd_type
			]) as records
		FROM this_year_data ts
		LEFT JOIN last_year_scd ls
		ON ls.actorid = ts.actorid
		WHERE (ts.quality_class <> ls.quality_class
			OR ts.is_active <> ls.is_active)
			OR ls.actorid IS NULL
	),


-- Flatten the unnested changed records back into columns
	unnested_changed_records AS (

		SELECT 	actorid,
				actor,
				(records::actors_scd_type).quality_class,
				(records::actors_scd_type).is_active,
				(records::actors_scd_type).start_year,
				(records::actors_scd_type).end_year
		FROM changed_records
	),

-- New actors who did not exist in the last year's SCD snapshot
	new_records AS (
		SELECT 
			ts.actorid,
			ts.actor,
			ts.quality_class,
			ts.is_active,
			ts.current_year AS start_year,
			ts.current_year AS end_year
		FROM this_year_data ts
		LEFT JOIN last_year_scd ls
			ON ts.actorid = ls.actorid
		WHERE ls.actorid IS NULL
	)


-- Combine historical, unchanged, changed, and new records into the updated SCD table
SELECT * FROM historical_scd

UNION ALL 

SELECT * FROM unchanged_records

UNION ALL 

SELECT * FROM unnested_changed_records

UNION ALL 

SELECT * FROM new_records

-- Notes:
-- This incremental approach processes less data than full backfill queries,
-- but depends on the accuracy of the previous year's SCD snapshot.
-- It is optimized for yearly batch loads, but complicates backfilling multiple years.