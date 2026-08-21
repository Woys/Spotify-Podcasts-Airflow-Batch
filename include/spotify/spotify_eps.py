"""Spotify API helpers for Spotify DAGs."""

try:
    from airflow.sdk import Variable
except Exception:  # pragma: no cover - fallback for newer/partial Airflow layouts
    from airflow.sdk import Variable
import os
import base64
from requests import post, get, exceptions
import json
import pandas as pd
from datetime import date
import logging
from pydantic import BaseModel, ValidationError
from typing import List, Optional, Any

class PodcastChartItem(BaseModel):
    chartRankMove: str
    episodeUri: str
    showUri: str
    episodeName: str

class Show(BaseModel):
    name: str
    description: str
    publisher: str
    copyrights: List[Any]
    explicit: bool
    href: str
    html_description: str
    is_externally_hosted: bool
    languages: List[str]
    media_type: str
    total_episodes: int
    type: str
    uri: str

class Episode(BaseModel):
    id: str
    name: str
    description: str
    show: Show
    duration_ms: int
    explicit: bool
    is_externally_hosted: bool
    is_playable: bool
    language: str
    languages: List[str]
    release_date: str
    release_date_precision: str

class Episodes(BaseModel):
    episodes: List[Optional[Episode]]

class SpotifyAPI:
    request_timeout_seconds = 30

    def __init__(self):
        self.client_id = Variable.get("SP_CLIENT_ID")
        self.client_secret = Variable.get("SP_CLIENT_SECRET")
        self.token = self._get_token()
        self.podcastchart_data = None  # To store podcast chart data
        self.episode_data = {}  # To store episode data

    def _get_token(self) -> str:
        auth = f"{self.client_id}:{self.client_secret}"
        auth_bytes = auth.encode("utf-8")
        auth_base64 = str(base64.b64encode(auth_bytes), "utf-8")

        url = "https://accounts.spotify.com/api/token"
        headers = {
            "Authorization": "Basic " + auth_base64,
            "Content-Type": "application/x-www-form-urlencoded"
        }

        data = {"grant_type": "client_credentials"}
        result = post(
            url,
            headers=headers,
            data=data,
            timeout=self.request_timeout_seconds,
        )
        json_result = json.loads(result.content)
        return json_result["access_token"]

    def _get_auth_header(self) -> dict:
        return {"Authorization": "Bearer " + self.token}

    def _fetch_podcastchart(self, chart: str, region: str):
        if self.podcastchart_data is None:
            url = f"https://podcastcharts.byspotify.com/api/charts/{chart}"
            params = {"region": region}
            headers = {"Referer": "https://podcastcharts.byspotify.com/"}

            try:
                response = get(
                    url,
                    headers=headers,
                    params=params,
                    timeout=self.request_timeout_seconds,
                )
                response.raise_for_status()
                data = response.json()
                self.podcastchart_data = [PodcastChartItem(**item) for item in data]
            except (exceptions.RequestException, ValidationError) as e:
                raise SystemExit(e)
            logging.info(f"Fetched _fetch_podcastchart: {region}")

    def _fetch_episodes(self, episode_ids: str, region: str = 'us'):
        if episode_ids not in self.episode_data:
            url = "https://api.spotify.com/v1/episodes"
            query = f"?ids={episode_ids}&market={region}"
            url_query = url + query
            headers = self._get_auth_header()

            try:
                response = get(
                    url_query,
                    headers=headers,
                    timeout=self.request_timeout_seconds,
                )
                response.raise_for_status()
                data = response.json()
                self.episode_data[episode_ids] = Episodes(**data)
            except (exceptions.RequestException, ValidationError) as e:
                raise SystemExit(e)
            logging.info(f"Fetched _fetch_episodes: {url_query}")

    def get_transformed_podcastchart(self, chart: str = "top-episodes", region: str = "us") -> pd.DataFrame:
        self._fetch_podcastchart(chart, region)
        today = date.today()

        charts = []
        for i, item in enumerate(self.podcastchart_data):
            charts.append({
                "date": today,
                "rank": i + 1,
                "region": region,
                "chartRankMove": item.chartRankMove,
                "episodeUri": item.episodeUri[16:],
                "showUri": item.showUri[13:],
                "episodeName": item.episodeName
            })
        return pd.DataFrame(charts)

    def get_transformed_podcastcharts(self, chart: str = "top-episodes", regions: list = ["us"]) -> pd.DataFrame:
        df_result = pd.DataFrame()
        for region in regions:
            self.podcastchart_data = None 
            chart_df = self.get_transformed_podcastchart(chart, region)
            df_result = pd.concat([df_result, chart_df], ignore_index=True)
        return df_result

    def get_transformed_search_eps(self, **kwargs: str) -> pd.DataFrame:
        if "chart" in kwargs and "region" in kwargs:
            self._fetch_podcastchart(kwargs.get("chart"), kwargs.get("region"))
            episodeUris_list = [item.episodeUri[16:] for item in self.podcastchart_data]
        elif "chart_file" in kwargs:
            episodeUris_list = kwargs.get("episodeUris_list")
        else:
            raise ValueError(f"get_transformed_search_eps has no chart,region, or chart_file in kwargs: {kwargs}")
        
        episodes_data = []
        for i in range(0, len(episodeUris_list), 50):
            episodeUris_selected = episodeUris_list[i:i + 50]
            episodeUris = ",".join(episodeUris_selected)
            self._fetch_episodes(episodeUris,kwargs.get("region"))

            validated_episodes = self.episode_data[episodeUris]

            for episode in validated_episodes.episodes:
                if episode:
                    episodes_data.append({
                        "id": episode.id,
                        "name": episode.name,
                        "description": episode.description,
                        "show.name": episode.show.name,
                        "show.description": episode.show.description,
                        "show.publisher": episode.show.publisher,
                        "duration_ms": episode.duration_ms,
                        "explicit": episode.explicit,
                        "is_externally_hosted": episode.is_externally_hosted,
                        "is_playable": episode.is_playable,
                        "language": episode.language,
                        "languages": episode.languages,
                        "release_date": episode.release_date,
                        "release_date_precision": episode.release_date_precision,
                        "show.copyrights": episode.show.copyrights,
                        "show.explicit": episode.show.explicit,
                        "show.href": episode.show.href,
                        "show.html_description": episode.show.html_description,
                        "show.is_externally_hosted": episode.show.is_externally_hosted,
                        "show.languages": episode.show.languages,
                        "show.media_type": episode.show.media_type,
                        "show.total_episodes": episode.show.total_episodes,
                        "show.type": episode.show.type,
                        "show.uri": episode.show.uri
                    })

        return pd.DataFrame(episodes_data)
    
    def get_charts_eps_file(self, chart_file: str, regions: list = ["us"]) -> pd.DataFrame:
        df_result = pd.DataFrame()
        chart_df_all_regions = pd.read_parquet(chart_file)
        for region in regions:
            chart_df = chart_df_all_regions.loc[chart_df_all_regions['region'] == region]
            episodeUris_list = chart_df['episodeUri'].tolist()
            eps_df = self.get_transformed_search_eps(chart_file=chart_file, region=region, episodeUris_list=episodeUris_list)
            merged_df = pd.merge(chart_df, eps_df, left_on='episodeUri', right_on='id', how='left')

            missing_eps = merged_df[merged_df['name'].isna()]
            if not missing_eps.empty:
                missing_ids = missing_eps['episodeUri'].tolist()
                logging.warning(
                    f"Missing episode data for {len(missing_eps)} items in region '{region}'. "
                    f"Sample missing IDs: {missing_ids[:5]}"
                )

            valid_eps = merged_df[merged_df['name'].notna()]
            name_mismatch = valid_eps[
                valid_eps['episodeName'].str.strip().str.lower() != 
                valid_eps['name'].str.strip().str.lower()
            ]
            if not name_mismatch.empty:
                mismatched_ids = name_mismatch['episodeUri'].tolist()
                logging.warning(
                    f"Name mismatch found for {len(name_mismatch)} items in region '{region}'. "
                    f"Sample mismatched IDs: {mismatched_ids[:5]}"
                )


            df_result = pd.concat([df_result, merged_df], ignore_index=True)
        df_result = df_result.drop(columns=['id','name'])
        return df_result

    def get_charts_eps(self, chart: str = "top-episodes", regions: list = ["us"]) -> pd.DataFrame:
        df_result = pd.DataFrame()
        for region in regions:
            self.podcastchart_data = None 
            chart_df = self.get_transformed_podcastchart(chart=chart, region=region)
            eps_df = self.get_transformed_search_eps(chart=chart, region=region)
            
            merged_df = pd.merge(chart_df, eps_df, left_on='episodeUri', right_on='id', how='left')

            missing_eps = merged_df[merged_df['name'].isna()]
            if not missing_eps.empty:
                missing_ids = missing_eps['episodeUri'].tolist()
                logging.warning(
                    f"Missing episode data for {len(missing_eps)} items in region '{region}'. "
                    f"Sample missing IDs: {missing_ids[:5]}"
                )

            valid_eps = merged_df[merged_df['name'].notna()]
            name_mismatch = valid_eps[
                valid_eps['episodeName'].str.strip().str.lower() != 
                valid_eps['name'].str.strip().str.lower()
            ]
            if not name_mismatch.empty:
                mismatched_ids = name_mismatch['episodeUri'].tolist()
                logging.warning(
                    f"Name mismatch found for {len(name_mismatch)} items in region '{region}'. "
                    f"Sample mismatched IDs: {mismatched_ids[:5]}"
                )

            df_result = pd.concat([df_result, merged_df], ignore_index=True)
        
        df_result = df_result.drop(columns=['id','name'])
        return df_result
