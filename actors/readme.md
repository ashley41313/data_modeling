# Actors Data ETL and History Tracking

## Overview

This folder contains SQL scripts to create, populate, and maintain an **actors** dataset with cumulative film data and a historical slowly changing dimension (SCD) tracking changes in actor quality and activity status over time.

---

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

"Fred Astaire" "nm0000001" "The Towering Inferno" 1974 39888 7 "tt0072308"
"Fred Astaire" "nm0000001" "The Amazing Dobermans" 1976 369 5.3 "tt0074130"
"Lauren Bacall" "nm0000002" "Murder on the Orient Express" 1974 56620 7.3 "tt0071877"
"Brigitte Bardot" "nm0000003" "The Bear and the Doll" 1970 431 6.4 "tt0064779"

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