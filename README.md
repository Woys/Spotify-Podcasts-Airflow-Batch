# Spotify Airflow Pipline

**Tools & Tech Stack:** Python, Apache Airflow, AWS S3, Pandas, Spotify API, Docker

**TL;DR:** Daily ingestion of Spotify's top podcast episodes across various regions, orchestrated using Apache Airflow and stored in AWS S3 Unploded to Kaggle

**Daily Updated Kaggle Dataset:** [https://www.kaggle.com](https://www.kaggle.com/datasets/daniilmiheev/top-spotify-podcasts-daily-updated)

## Sources

- **Top Podcast Charts:** [https://podcastcharts.byspotify.com/](https://podcastcharts.byspotify.com/)
- **Spotify API Endpoint Used:** [Get an Episode](https://developer.spotify.com/documentation/web-api/reference/get-an-episode)

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
