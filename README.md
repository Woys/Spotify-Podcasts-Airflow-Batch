# Airflow-Batch

**Tools & Tech Stack:** Python, Apache Airflow, AWS S3, Pandas, Docker

**TL;DR:** Reusable batch orchestration project built with Apache Airflow. The current implementation includes a Spotify ingestion example and can be adapted for other batch data sources.

**Daily Updated Kaggle Dataset:** [https://www.kaggle.com](https://www.kaggle.com/datasets/daniilmiheev/top-spotify-podcasts-daily-updated)

## Sources

- **Top Podcast Charts:** [https://podcastcharts.byspotify.com/](https://podcastcharts.byspotify.com/)
- **Spotify API Endpoint Used:** [Get an Episode](https://developer.spotify.com/documentation/web-api/reference/get-an-episode)

## Dataset Details

- **Regions Covered:** Argentina (`ar`), Australia (`au`), Austria (`at`), Brazil (`br`), Canada (`ca`), Chile (`cl`), Colombia (`co`), France (`fr`), Germany (`de`), India (`in`), Indonesia (`id`), Ireland (`ie`), Italy (`it`), Japan (`jp`), Mexico (`mx`), New Zealand (`nz`), Philippines (`ph`), Poland (`pl`), Spain (`es`), Netherlands (`nl`), United Kingdom (`gb`), United States (`us`)
- **Data Formats:** Parquet files and a consolidated CSV file
- **Update Frequency:** Daily
- **Key Fields:**
  - `date`: Date of data collection
  - `region`: Country code
  - `episodeUri`: Unique identifier for the episode on Spotify
  - `id`: Episode ID
  - `episodeName`: Name of the episode
  - `name`: Name of the podcast
  - Additional metadata fields as available from the API

## Monetizable Research Dataset Pipeline

This repository also includes a generic, non-Spotify pipeline for building a daily text dataset with `text-ingest`.

- DAG id: `text_daily_pipeline`
- Schedule: daily (`0 2 * * *`)
- Sources: OpenAlex + Crossref + Hacker News + Federal Register (+ NewsAPI when key is set)
- Output format: Parquet only
- Monetization channel: S3-only

### Output in S3

For each run date (`YYYY-MM-DD`) the DAG publishes:

- `s3://<SP_S3_BUCKET>/datasets/text_daily/dt=<YYYY-MM-DD>/dataset.parquet`
- `s3://<SP_S3_BUCKET>/datasets/text_daily/dt=<YYYY-MM-DD>/manifest.json`
- `s3://<SP_S3_BUCKET>/datasets/text_daily/latest.json`
- `s3://<SP_S3_BUCKET>/datasets/text_daily/merged/dataset.csv` (merged across all daily partitions, human-readable)

Canonical schema fields:
`ingested_at`, `source`, `source_record_id`, `doi`, `title`, `abstract`, `authors`, `publication_date`, `journal`, `topics`, `url`, `language`, `license`, `raw_payload_hash`.

### Required Airflow Variables

- `SP_S3_BUCKET` (required; `DATASET_S3_BUCKET` is also accepted as fallback)
- `TI_SEARCH_TERMS` (JSON array, default: `["data engineering", "machine learning", "airflow"]`)
- `TI_MAX_RECORDS_PER_SOURCE` (default: `200`)
- `TI_REQUEST_DELAY_SECONDS` (default: `1.0`)
- `TI_CROSSREF_EMAIL` (recommended for provider etiquette/policy)
- `TI_OPENALEX_API_KEY` (optional)
- `TI_NEWSAPI_KEY` (optional; enables NewsAPI source when set)
- `TI_NEWSAPI_LANGUAGE` (optional, default: `en`)
- `TI_HN_ITEM_TYPE` (optional, one of: `story`, `comment`, `all`; default: `story`)
- `TI_HN_USE_DATE_SORT` (optional boolean, default: `true`)
- `OPENROUTER_API_KEY` (required by `spotify_dbt_qa`)
- `TELEGRAM_APPRISE_URL` (recipient for generated Spotify dbt QA reports and failures)

S3 uploads use Airflow connection id `aws_conn`.

### Reusable email sender

`include.notification.send_email` sends HTML through an Airflow SMTP connection:

```python
from include.notification import send_email

send_email("<p>Pipeline completed</p>", "owner@example.com")
```

The default connection id is `smtp_default`, and the default subject is
`Airflow notification`. Configure the SMTP host, port, credentials, TLS/SSL,
and sender address with `AIRFLOW_CONN_SMTP_DEFAULT` in `.env`; do not put SMTP
credentials in DAG code. Both the
subject and connection id can be overridden with the `subject` and
`smtp_conn_id` keyword arguments.

The email helper remains available for DAGs that explicitly choose email. The
current `spotify_dbt_qa` DAG sends its redacted report through Telegram.

### Reusable Telegram sender

`include.notification.send_telegram` sends a Telegram message through Apprise:

```python
from include.notification import send_telegram

send_telegram("Pipeline completed", title="Spotify pipeline")
```

Store the complete Apprise Telegram URL in the `TELEGRAM_APPRISE_URL` Airflow
Variable in `.env`:

```dotenv
AIRFLOW_VAR_TELEGRAM_APPRISE_URL=tgram://BOT_TOKEN/CHAT_ID
```

Every DAG uses this variable for one Telegram notification when a DAG run fails
after retries are exhausted. When available, the message includes the failed
task, error, and Airflow log URL.

The URL can use Apprise's Telegram options, including multiple chat IDs and
topics. To use a differently named Airflow Variable, pass its name with the
`url_variable` keyword argument. Keep bot tokens out of DAG code and logs.

### Run

Trigger from the UI/API by DAG id `text_daily_pipeline`, or run tests locally:

```bash
make all
```
