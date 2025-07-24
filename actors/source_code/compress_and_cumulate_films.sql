/* 
============================================================
  Script: compress_and_cumulate_films.sql

  Purpose: 
    - Create the `actors` table to store cumulative film data
      per actor per year, from 1970 onward
    - Define custom types for film structure and quality class
    - Seed the table with data for the year 1970 using aggregation

  Input Tables:
    - actor_films  (columns include actorid, actor, film, filmid, votes, rating, year)

  Output Tables:
    - actors  (stores cumulative film data by actor and year)

  Author: Ashley Eckert
  Created: 2025-07-24
============================================================
*/

-- Define a custom ENUM type to classify actor quality
-- based on their average film ratings
CREATE TYPE quality_class AS ENUM ('star', 'good', 'average', 'bad');

-- Define a composite type to represent a film object
-- Each actor can have an array of these structs
CREATE TYPE films AS (
	film TEXT,
	film_id TEXT,
	votes INTEGER,
	rating REAL
);

-- Create the `actors` table to store compressed yearly data
-- Each row represents an actor's cumulative film history for a given year
CREATE TABLE actors (
	actor TEXT,
	actorid TEXT,
	films films[], -- Array of film structs
    quality_class quality_class, -- Based on average film rating
    is_active BOOLEAN, -- True if they released films this year
	current_year INTEGER, 
	PRIMARY KEY(actorid, current_year)
)

-- Seed the table for the initial year: 1970
-- "yesterday" CTE gets the prior year's state (1969), if any
WITH yesterday AS (
	SELECT * FROM actors 
	WHERE current_year = 1969
), 

-- "today" aggregates films per actor for the year 1970
-- Ensures a single row per actor by aggregating into film arrays
	today AS (
		SELECT 
			actor,
			actorid, 
			ARRAY_AGG(ROW(film, filmid, votes, rating)::films) as films,
			MAX(year) AS year,
			AVG(rating) AS avg_rating
		FROM actor_films
		WHERE year = 1970
		GROUP BY actorid, year
	)

-- Insert combined cumulative data into the `actors` table
-- Uses FULL OUTER JOIN to ensure we include:
-- - Actors active in 1969 but not 1970 (carry forward)
-- - New actors in 1970 (first entry)
INSERT INTO actors 

SELECT 
	COALESCE(t.actor, y.actor) AS actor,
	COALESCE(t.actorid, y.actorid) AS actorid,

    -- Merge previous films with current year's films if both exist
	CASE 
        WHEN y.films IS NULL THEN t.films
	    WHEN t.films IS NOT NULL THEN y.films || t.films
	    ELSE y.films
	END AS films,
    
    -- Compute updated quality class if new data exists
	CASE WHEN t.avg_rating IS NOT NULL THEN
			CASE WHEN 
					t.avg_rating > 8 THEN 'star'
				WHEN 
					t.avg_rating > 7 AND t.avg_rating <= 8 THEN 'good'
				WHEN 
					t.avg_rating > 6 AND t.avg_rating <= 7 THEN 'average'
				ELSE 'bad'
			END::quality_class
		ELSE y.quality_class
	END AS quality_class,

    -- Mark actor as active if they had any films this year
    CASE 
        WHEN t.films IS NOT NULL THEN True   
		ELSE False 
	END AS is_active,

    -- Use current year if available, otherwise increment from last year
	COALESCE(t.year, y.current_year+1) as current_year
FROM today t FULL OUTER JOIN yesterday y 
on t.actorid = y.actorid