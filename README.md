# Spotify Airflow Pipline

**Tools & Tech Stack:** Python, Apache Airflow, AWS S3, Pandas, Spotify API, Docker

**TL;DR:** Daily ingestion of Spotify's top podcast episodes across various regions, orchestrated using Apache Airflow and stored in AWS S3 Unploded to Kaggle

**Daily Updated Kaggle Dataset:** [https://www.kaggle.com](https://www.kaggle.com/datasets/daniilmiheev/top-spotify-podcasts-daily-updated)

## Sources

- **Top Podcast Charts:** [https://podcastcharts.byspotify.com/](https://podcastcharts.byspotify.com/)
- **Spotify API Endpoint Used:** [Get an Episode](https://developer.spotify.com/documentation/web-api/reference/get-an-episode)

## Dataset Overview

Are you fascinated by the world of podcasts and eager to explore what's trending globally? This dataset offers daily snapshots of the top podcast episodes from Spotify across multiple countries. By analyzing this data, you can uncover insights such as:

- **Trending Podcasts:** Discover the most popular podcast episodes in different regions.
- **Cultural Preferences:** Compare podcast popularity to understand cultural influences on podcast consumption.
- **Trend Analysis:** Track how podcast rankings change over time to identify emerging trends.
- **Content Analysis:** Dive into episode metadata for sentiment analysis, topic modeling, or genre classification.

## Data Collection Process

The data is collected daily using Apache Airflow as the orchestrator. Here's how the pipeline works:

1. **Data Ingestion:** A Python script interacts directly with Spotify's API (without using any SDK) to fetch the top podcast episodes for specified regions.
2. **Data Storage:** The retrieved data is saved locally in Parquet format for efficient storage.
3. **Error Logging:** Any discrepancies or mismatches during data collection are logged and stored for transparency and debugging purposes.
4. **Data Upload:** The Parquet files and fail logs are uploaded to AWS S3 for centralized storage.
5. **Data Union:** Individual Parquet files are concatenated into a single CSV file to simplify data consumption.

## Dataset Details

- **Regions Covered:** Argentina (`ar`), Australia (`au`), Austria (`at`), Brazil (`br`), Canada (`ca`), Chile (`cl`), Colombia (`co`), France (`fr`), Germany (`de`), India (`in`), Indonesia (`id`), Ireland (`ie`), Italy (`it`), Japan (`jp`), Mexico (`mx`), New Zealand (`nz`), Philippines (`ph`), Poland (`pl`), Spain (`es`), Netherlands (`nl`), United Kingdom (`gb`), United States (`us`)
- **Data Formats:** Parquet files and a consolidated CSV file
- **Update Frequency:** Daily
- **Key Fields:**
  - `pipeline`: Name of the data pipeline
  - `function`: Function within the pipeline
  - `date`: Date of data collection
  - `region`: Country code
  - `episodeUri`: Unique identifier for the episode on Spotify
  - `id`: Episode ID
  - `episodeName`: Name of the episode
  - `name`: Name of the podcast
  - Additional metadata fields as available from the API
