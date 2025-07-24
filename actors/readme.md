# Actors Data ETL and History Tracking

## Overview

This project builds a structured pipeline for transforming raw actor film data into cumulative summaries and historical records using SQL. It compresses film data by actor/year and implements a Slowly Changing Dimension (SCD Type 2) model to track changes in actor quality and activity status over time.

---

## Folder Structure
actors/
├── sql/ # SQL scripts for creating and populating the tables
│ ├── compress_and_cumulate_films.sql
│ ├── actors_history_scd.sql
│ └── incremental_update_actors_history_scd.sql
├── tables/ # Final output tables created and populated by the SQL scripts
│ ├── actors.zip # actors table was compressed
│ ├── actors_history_scd
│ └── actors_scd_type (type definition used in SCD)
└── README.md

## Source Table: `actors`

The `actors` table stores raw film data for actors per film per year, including votes and ratings.

### Columns

| Column   | Type   | Description                          |
|----------|--------|------------------------------------|
| actor    | TEXT   | Actor's full name                   |
| actorid  | TEXT   | Unique actor identifier (e.g., nm0000001) |
| film     | TEXT   | Film title                        |
| year     | INTEGER| Release year of the film            |
| votes    | INTEGER| Number of votes for the film       |
| filmid   | TEXT   | Unique film identifier (e.g., tt0072308) |
| rating   | REAL   | Average user rating for the film   |

### Sample Data

| Actor          | Actor ID   | Film                          | Year | Votes  | Rating | Film ID    |
|----------------|------------|-------------------------------|------|--------|--------|------------|
| Fred Astaire   | nm0000001  | The Towering Inferno           | 1974 | 39888  | 7.0    | tt0072308  |
| Fred Astaire   | nm0000001  | The Amazing Dobermans          | 1976 | 369    | 5.3    | tt0074130  |
| Fred Astaire   | nm0000001  | The Purple Taxi                | 1977 | 533    | 6.6    | tt0076851  |
| Fred Astaire   | nm0000001  | Ghost Story                   | 1981 | 7731   | 6.3    | tt0082449  |
| Lauren Bacall  | nm0000002  | Murder on the Orient Express   | 1974 | 56620  | 7.3    | tt0071877  |
| Lauren Bacall  | nm0000002  | The Shootist                  | 1976 | 22409  | 7.6    | tt0075213  |
| Lauren Bacall  | nm0000002  | HealtH                       | 1980 | 693    | 5.7    | tt0079256  |
| Lauren Bacall  | nm0000002  | The Fan                      | 1981 | 2038   | 5.8    | tt0082362  |
| Lauren Bacall  | nm0000002  | Appointment with Death        | 1988 | 4058   | 6.2    | tt0094669  |
| Lauren Bacall  | nm0000002  | Mr. North                    | 1988 | 1297   | 5.9    | tt0095665  |
| Lauren Bacall  | nm0000002  | Innocent Victim              | 1989 | 103    | 5.9    | tt0099846  |
| Lauren Bacall  | nm0000002  | Misery                       | 1990 | 186886 | 7.8    | tt0100157  |
| Lauren Bacall  | nm0000002  | All I Want for Christmas      | 1991 | 4564   | 6.0    | tt0101301  |
| Lauren Bacall  | nm0000002  | Ready to Wear                | 1994 | 14444  | 5.2    | tt0110907  |
| Lauren Bacall  | nm0000002  | The Mirror Has Two Faces      | 1996 | 16564  | 6.7    | tt0117057  |
| Lauren Bacall  | nm0000002  | My Fellow Americans           | 1996 | 14178  | 6.5    | tt0117119  |
| Lauren Bacall  | nm0000002  | Le jour et la nuit            | 1997 | 1014   | 1.6    | tt0119418  |
| Lauren Bacall  | nm0000002  | A Star for Two                | 1991 | 227    | 5.5    | tt0166817  |
| Lauren Bacall  | nm0000002  | Diamonds                    | 1999 | 1493   | 5.5    | tt0167423  |
| Lauren Bacall  | nm0000002  | Presence of Mind             | 1999 | 540    | 5.5    | tt0211577  |

## SQL Scripts

### 1. `compress_and_cumulate_films.sql` (Backfill Script)

- **Purpose:**  
  Creates the `actors` table that stores cumulative film data per actor per year, compressing individual film records into arrays along with quality classification and activity status.  
  Seeds the table starting from the earliest year (1970).  

- **Input Table:** `actor_films` (raw film data per actor and year)  
- **Output Table:** `actors` (yearly cumulative film data per actor)

- **Notes:**  
  This script can be used as a backfill to build the cumulative actors dataset from scratch or to seed initial data.

---

### 2. `actors_history_scd.sql` (Backfill Script)

- **Purpose:**  
  Creates and populates the `actors_history_scd` table, implementing a Slowly Changing Dimension (Type 2) for tracking changes in actor quality and activity over time, including start and end years of each record version.  

- **Input Table:** `actors` (cumulative yearly data with quality and activity status)  
- **Output Table:** `actors_history_scd` (historical SCD Type 2 table)

- **Notes:**  
  This backfill query processes the entire history to generate a complete SCD record set.

---

### 3. `incremental_update_actors_history_scd.sql` (Incremental Update Script)

- **Purpose:**  
  Incrementally updates the `actors_history_scd` table by combining the previous year's snapshot with new actor data for the current year, efficiently handling unchanged, changed, and new actor records.  

- **Input Tables:**  
  - `actors_history_scd` (previous year's SCD data)  
  - `actors` (current year's cumulative data)  

- **Output Table:**  
  - `actors_history_scd` (updated with incremental changes for the current year)

- **Notes:**  
  This script is designed for efficient yearly incremental updates and depends on the accuracy of the prior snapshot. It is not suitable for full historical backfills.

---

## How to Use

1. Use `compress_and_cumulate_films.sql` to create and seed the `actors` cumulative data table from raw film data.  
2. Use `actors_history_scd.sql` to create and backfill the `actors_history_scd` SCD table for full historical data.  
3. Use `incremental_update_actors_history_scd.sql` to incrementally update the SCD table with new yearly data.

---

## Contact

For questions or support, contact Ashley Eckert at ashley41313@hotmail.com