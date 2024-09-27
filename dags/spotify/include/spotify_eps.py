from airflow.models import Variable
import os
import base64
from requests import post, get, exceptions
import json
import pandas as pd
from datetime import date
import logging

class SpotifyAPI:
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
        result = post(url, headers=headers, data=data)
        json_result = json.loads(result.content)
        return json_result["access_token"]

    def _get_auth_header(self) -> dict:
        return {"Authorization": "Bearer " + self.token}

    def _fetch_podcastchart(self, chart: str, region: str):
        if self.podcastchart_data is None:
            url = f"https://podcastcharts.byspotify.com/api/charts//{chart}"
            params = {"region": region}
            headers = {"Referer": "https://podcastcharts.byspotify.com/"}

            try:
                response = get(url, headers=headers, params=params)
                response.raise_for_status()
            except exceptions.RequestException as e:
                raise SystemExit(e)
            self.podcastchart_data = response.json()

    def _fetch_episodes(self, episode_ids: str, region: str = 'us'):
        if episode_ids not in self.episode_data:
            url = "https://api.spotify.com/v1/episodes"
            query = f"?ids={episode_ids}&market={region}"
            url_query = url + query
            headers = self._get_auth_header()

            try:
                response = get(url_query, headers=headers)
                response.raise_for_status()
            except exceptions.RequestException as e:
                raise SystemExit(e)
            self.episode_data[episode_ids] = response.json()

    def get_transformed_podcastchart(self, chart: str = "top_episodes", region: str = "us") -> pd.DataFrame:
        self._fetch_podcastchart(chart, region)
        today = date.today()

        columns = [
            "date", "rank", "region", "chartRankMove", "episodeUri", "showUri", "episodeName"
        ]
        df_result = pd.DataFrame(columns=columns)

        for i, item in enumerate(self.podcastchart_data):
            data = {
                "date": today,
                "rank": i + 1,
                "region": region,
                "chartRankMove": item["chartRankMove"],
                "episodeUri": item["episodeUri"][16:],
                "showUri": item["showUri"][13:],
                "episodeName": item["episodeName"]
            }
            df_result = pd.concat([df_result, pd.DataFrame([data])], ignore_index=True)

        return df_result

    def get_transformed_podcastcharts(self, chart: str = "top_episodes", regions: list = ["us"]) -> pd.DataFrame:
        df_result = pd.DataFrame()
        for region in regions:
            self.podcastchart_data = None 
            chart_df = self.get_transformed_podcastchart(chart, region)
            df_result = pd.concat([df_result, chart_df], ignore_index=True)
        return df_result

    def get_transformed_search_eps(self, chart: str = "top_episodes", region: str = "us") -> pd.DataFrame:
        self._fetch_podcastchart(chart, region)
        
        columns = [
            'id', 'name', 'description', 'show.name', 'show.description','show.publisher','duration_ms',
            'explicit', 'is_externally_hosted', 'is_playable', 'language',
            'languages', 'release_date', 'release_date_precision', 'show.copyrights', 'show.explicit',
            'show.href', 'show.html_description', 'show.is_externally_hosted',
            'show.languages', 'show.media_type', 'show.total_episodes', 'show.type', 'show.uri'
        ]

        episodeUris_list = [item["episodeUri"][16:] for item in self.podcastchart_data]
        df_result = pd.DataFrame(columns=columns)

        for i in range(0, len(episodeUris_list), 50):
            episodeUris_selected = episodeUris_list[i:i + 50]
            episodeUris = ",".join(episodeUris_selected)
            self._fetch_episodes(episodeUris)

            search_json = self.episode_data[episodeUris]

            if not search_json or 'episodes' not in search_json or not search_json['episodes']:
                logging.warning(f"No episodes found in batch starting at index {i}. Skipping this batch.")
                continue  # Skip to the next batch
            
            for episode in search_json['episodes']:
                if not episode or 'id' not in episode:
                    logging.warning(f"Missing data for an episode in batch starting at index {i}. Skipping this episode.")
                    continue

                data = {
                    "id": episode["id"],
                    "name": episode["name"],
                    "description": episode["description"],
                    "show.name": episode["show"]["name"],
                    "show.description": episode["show"]["description"],
                    "show.publisher": episode["show"]["publisher"],
                    "duration_ms": episode["duration_ms"],
                    "explicit": episode["explicit"],
                    "is_externally_hosted": episode["is_externally_hosted"],
                    "is_playable": episode["is_playable"],
                    "language": episode["language"],
                    "languages": episode["languages"],
                    "release_date": episode["release_date"],
                    "release_date_precision": episode["release_date_precision"],
                    "show.copyrights": episode["show"]["copyrights"],
                    "show.explicit": episode["show"]["explicit"],
                    "show.href": episode["show"]["href"],
                    "show.html_description": episode["show"]["html_description"],
                    "show.is_externally_hosted": episode["show"]["is_externally_hosted"],
                    "show.languages": episode["show"]["languages"],
                    "show.media_type": episode["show"]["media_type"],
                    "show.total_episodes": episode["show"]["total_episodes"],
                    "show.type": episode["show"]["type"],
                    "show.uri": episode["show"]["uri"]
                }
                df_result = pd.concat([df_result, pd.DataFrame([data])], ignore_index=True)

        return df_result
    
    def get_chart_eps(self, chart: str = "top_episodes", region: str = "us") -> pd.DataFrame:
        chart_df = self.get_transformed_podcastchart(chart, region)
        eps_df = self.get_transformed_search_eps(chart, region)
        
        df_result = pd.merge(chart_df, eps_df, left_on='episodeUri', right_on='id', how='left')

        name_mismatch = df_result[df_result['episodeName'] != df_result['name']]
        if not name_mismatch.empty:
                raise ValueError("Name mismatch found between chart data and episode data.")
        
        df_result = df_result.drop(columns=['id','name'])

        return df_result
    
    def get_charts_eps(self, chart: str = "top_episodes", regions: list = ["us"]) -> pd.DataFrame:
        df_result = pd.DataFrame()
        for region in regions:
            self.podcastchart_data = None 
            chart_df = self.get_transformed_podcastchart(chart, region)
            eps_df = self.get_transformed_search_eps(chart, region)
            
            merged_df = pd.merge(chart_df, eps_df, left_on='episodeUri', right_on='id', how='left')

            name_mismatch = merged_df[merged_df['episodeName'] != merged_df['name']]
            if not name_mismatch.empty:
                    raise ValueError("Name mismatch found between chart data and episode data.")

            df_result = pd.concat([df_result, merged_df], ignore_index=True)
        
        df_result = df_result.drop(columns=['id','name'])
        return df_result