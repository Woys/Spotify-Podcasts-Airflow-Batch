from __future__ import annotations
import logging

import base64
import json
from types import SimpleNamespace
from unittest.mock import Mock

import pandas as pd
import pytest


import include.spotify.spotify_eps as spotify_eps


def _episode_dict(episode_id: str, name: str) -> dict:
    return {
        "id": episode_id,
        "name": name,
        "description": f"description-{name}",
        "show": {
            "name": f"show-{name}",
            "description": "show description",
            "publisher": "publisher",
            "copyrights": [],
            "explicit": False,
            "href": "https://example.com/show",
            "html_description": "html",
            "is_externally_hosted": False,
            "languages": ["en"],
            "media_type": "audio",
            "total_episodes": 100,
            "type": "show",
            "uri": "spotify:show:1",
        },
        "duration_ms": 1000,
        "explicit": False,
        "is_externally_hosted": False,
        "is_playable": True,
        "language": "en",
        "languages": ["en"],
        "release_date": "2024-01-01",
        "release_date_precision": "day",
    }


@pytest.fixture
def api() -> spotify_eps.SpotifyAPI:
    instance = spotify_eps.SpotifyAPI.__new__(spotify_eps.SpotifyAPI)
    instance.client_id = "client-id"
    instance.client_secret = "client-secret"
    instance.token = "token-value"
    instance.podcastchart_data = None
    instance.episode_data = {}
    return instance


def test_init_reads_airflow_variables_and_token(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        spotify_eps.Variable,
        "get",
        lambda key: {"SP_CLIENT_ID": "cid", "SP_CLIENT_SECRET": "csecret"}[key],
    )
    monkeypatch.setattr(spotify_eps.SpotifyAPI, "_get_token", lambda self: "token-123")

    instance = spotify_eps.SpotifyAPI()

    assert instance.client_id == "cid"
    assert instance.client_secret == "csecret"
    assert instance.token == "token-123"
    assert instance.podcastchart_data is None
    assert instance.episode_data == {}


def test_get_token_calls_spotify_accounts_endpoint(monkeypatch: pytest.MonkeyPatch, api: spotify_eps.SpotifyAPI) -> None:
    captured: dict[str, object] = {}

    def fake_post(
        url: str, headers: dict, data: dict, timeout: int
    ) -> SimpleNamespace:
        captured["url"] = url
        captured["headers"] = headers
        captured["data"] = data
        captured["timeout"] = timeout
        return SimpleNamespace(content=json.dumps({"access_token": "abc"}).encode("utf-8"))

    monkeypatch.setattr(spotify_eps, "post", fake_post)

    token = api._get_token()

    assert token == "abc"
    assert captured["url"] == "https://accounts.spotify.com/api/token"
    assert captured["data"] == {"grant_type": "client_credentials"}
    assert captured["timeout"] == 30
    expected_basic = base64.b64encode(b"client-id:client-secret").decode("utf-8")
    assert captured["headers"]["Authorization"] == f"Basic {expected_basic}"


def test_get_auth_header_returns_bearer_token(api: spotify_eps.SpotifyAPI) -> None:
    assert api._get_auth_header() == {"Authorization": "Bearer token-value"}


def test_fetch_podcastchart_populates_cache(monkeypatch: pytest.MonkeyPatch, api: spotify_eps.SpotifyAPI) -> None:
    payload = [
        {
            "chartRankMove": "UP",
            "episodeUri": "spotify:episode:ep1",
            "showUri": "spotify:show:show1",
            "episodeName": "Episode 1",
        },
        {
            "chartRankMove": "DOWN",
            "episodeUri": "spotify:episode:ep2",
            "showUri": "spotify:show:show2",
            "episodeName": "Episode 2",
        },
    ]

    class Response:
        def raise_for_status(self) -> None:
            return None

        def json(self) -> list[dict]:
            return payload

    monkeypatch.setattr(spotify_eps, "get", lambda *args, **kwargs: Response())

    api._fetch_podcastchart("top-episodes", "us")

    assert api.podcastchart_data is not None
    assert len(api.podcastchart_data) == 2
    assert all(isinstance(item, spotify_eps.PodcastChartItem) for item in api.podcastchart_data)


def test_fetch_podcastchart_uses_existing_cache(monkeypatch: pytest.MonkeyPatch, api: spotify_eps.SpotifyAPI) -> None:
    api.podcastchart_data = [
        spotify_eps.PodcastChartItem(
            chartRankMove="SAME",
            episodeUri="spotify:episode:ep1",
            showUri="spotify:show:show1",
            episodeName="Episode 1",
        )
    ]
    get_mock = Mock(side_effect=AssertionError("network should not be called when cache exists"))
    monkeypatch.setattr(spotify_eps, "get", get_mock)

    api._fetch_podcastchart("top-episodes", "us")

    assert get_mock.call_count == 0


def test_fetch_podcastchart_raises_system_exit_on_request_error(monkeypatch: pytest.MonkeyPatch, api: spotify_eps.SpotifyAPI) -> None:
    def raise_request(*args, **kwargs):
        raise spotify_eps.exceptions.RequestException("boom")

    monkeypatch.setattr(spotify_eps, "get", raise_request)

    with pytest.raises(SystemExit):
        api._fetch_podcastchart("top-episodes", "us")


def test_fetch_podcastchart_raises_system_exit_on_validation_error(monkeypatch: pytest.MonkeyPatch, api: spotify_eps.SpotifyAPI) -> None:
    class Response:
        def raise_for_status(self) -> None:
            return None

        def json(self) -> list[dict]:
            return [{"invalid": "shape"}]

    monkeypatch.setattr(spotify_eps, "get", lambda *args, **kwargs: Response())

    with pytest.raises(SystemExit):
        api._fetch_podcastchart("top-episodes", "us")


def test_fetch_episodes_populates_episode_cache(monkeypatch: pytest.MonkeyPatch, api: spotify_eps.SpotifyAPI) -> None:
    class Response:
        def raise_for_status(self) -> None:
            return None

        def json(self) -> dict:
            return {"episodes": [_episode_dict("id1", "Episode 1")]}

    monkeypatch.setattr(spotify_eps, "get", lambda *args, **kwargs: Response())

    api._fetch_episodes("id1", "us")

    assert "id1" in api.episode_data
    assert isinstance(api.episode_data["id1"], spotify_eps.Episodes)


def test_fetch_episodes_uses_cache(monkeypatch: pytest.MonkeyPatch, api: spotify_eps.SpotifyAPI) -> None:
    api.episode_data["id1"] = spotify_eps.Episodes(episodes=[])
    get_mock = Mock(side_effect=AssertionError("network should not be called when cache exists"))
    monkeypatch.setattr(spotify_eps, "get", get_mock)

    api._fetch_episodes("id1", "us")

    assert get_mock.call_count == 0


def test_get_transformed_podcastchart_trims_uris(monkeypatch: pytest.MonkeyPatch, api: spotify_eps.SpotifyAPI) -> None:
    api.podcastchart_data = [
        spotify_eps.PodcastChartItem(
            chartRankMove="UP",
            episodeUri="spotify:episode:ep1",
            showUri="spotify:show:show1",
            episodeName="Episode 1",
        )
    ]
    monkeypatch.setattr(api, "_fetch_podcastchart", lambda chart, region: None)

    frame = api.get_transformed_podcastchart(region="us")

    assert frame.iloc[0]["region"] == "us"
    assert frame.iloc[0]["rank"] == 1
    assert frame.iloc[0]["episodeUri"] == "ep1"
    assert frame.iloc[0]["showUri"] == "show1"


def test_get_transformed_podcastcharts_combines_regions(monkeypatch: pytest.MonkeyPatch, api: spotify_eps.SpotifyAPI) -> None:
    def fake_get_transformed(chart: str, region: str) -> pd.DataFrame:
        return pd.DataFrame([{"region": region, "episodeUri": f"{region}-id"}])

    monkeypatch.setattr(api, "get_transformed_podcastchart", fake_get_transformed)

    frame = api.get_transformed_podcastcharts(regions=["us", "gb"])

    assert frame["region"].tolist() == ["us", "gb"]


def test_get_transformed_search_eps_requires_expected_kwargs(api: spotify_eps.SpotifyAPI) -> None:
    with pytest.raises(ValueError):
        api.get_transformed_search_eps()


def test_get_transformed_search_eps_chunks_episode_requests(monkeypatch: pytest.MonkeyPatch, api: spotify_eps.SpotifyAPI) -> None:
    episode_ids = [f"id{i}" for i in range(51)]

    def fake_fetch_episodes(ids: str, region: str) -> None:
        parsed_ids = ids.split(",")
        episodes = [None] + [spotify_eps.Episode(**_episode_dict(item, item)) for item in parsed_ids]
        api.episode_data[ids] = spotify_eps.Episodes(episodes=episodes)

    fetch_mock = Mock(side_effect=fake_fetch_episodes)
    monkeypatch.setattr(api, "_fetch_episodes", fetch_mock)

    frame = api.get_transformed_search_eps(
        chart_file="chart.parquet",
        region="us",
        episodeUris_list=episode_ids,
    )

    assert fetch_mock.call_count == 2
    assert len(frame) == 51
    assert set(["id", "name", "show.name"]).issubset(frame.columns)


def test_get_charts_eps_file_merges_and_drops_id_name(monkeypatch: pytest.MonkeyPatch, api: spotify_eps.SpotifyAPI) -> None:
    chart_df = pd.DataFrame(
        [
            {"region": "us", "episodeUri": "id1", "episodeName": "Episode 1"},
            {"region": "gb", "episodeUri": "id2", "episodeName": "Episode 2"},
        ]
    )

    monkeypatch.setattr(spotify_eps.pd, "read_parquet", lambda path: chart_df)

    def fake_search(**kwargs) -> pd.DataFrame:
        ids = kwargs["episodeUris_list"]
        return pd.DataFrame(
            {
                "id": ids,
                "name": ["Episode 1" if item == "id1" else "Episode 2" for item in ids],
                "description": ["d"] * len(ids),
            }
        )

    monkeypatch.setattr(api, "get_transformed_search_eps", fake_search)

    frame = api.get_charts_eps_file("chart.parquet", regions=["us", "gb"])

    assert len(frame) == 2
    assert "id" not in frame.columns
    assert "name" not in frame.columns


def test_get_charts_eps_file_logs_on_name_mismatch(monkeypatch: pytest.MonkeyPatch, api: spotify_eps.SpotifyAPI, caplog: pytest.LogCaptureFixture) -> None:
    chart_df = pd.DataFrame([{"region": "us", "episodeUri": "id1", "episodeName": "Expected"}])
    monkeypatch.setattr(spotify_eps.pd, "read_parquet", lambda path: chart_df)
    monkeypatch.setattr(
        api,
        "get_transformed_search_eps",
        lambda **kwargs: pd.DataFrame({"id": ["id1"], "name": ["Other"]}),
    )

    with caplog.at_level(logging.WARNING):
        api.get_charts_eps_file("chart.parquet", regions=["us"])
    
    assert "Name mismatch found" in caplog.text
    assert "id1" in caplog.text


def test_get_charts_eps_merges_regions(monkeypatch: pytest.MonkeyPatch, api: spotify_eps.SpotifyAPI) -> None:
    def fake_chart_df(chart: str, region: str) -> pd.DataFrame:
        return pd.DataFrame(
            [{"region": region, "episodeUri": f"{region}-id", "episodeName": f"{region}-name"}]
        )

    def fake_eps_df(**kwargs) -> pd.DataFrame:
        region = kwargs["region"]
        return pd.DataFrame(
            [
                {
                    "id": f"{region}-id",
                    "name": f"{region}-name",
                    "description": "desc",
                }
            ]
        )

    monkeypatch.setattr(api, "get_transformed_podcastchart", fake_chart_df)
    monkeypatch.setattr(api, "get_transformed_search_eps", fake_eps_df)

    frame = api.get_charts_eps(regions=["us", "gb"])

    assert len(frame) == 2
    assert "id" not in frame.columns
    assert "name" not in frame.columns


def test_get_charts_eps_logs_on_name_mismatch(monkeypatch: pytest.MonkeyPatch, api: spotify_eps.SpotifyAPI, caplog: pytest.LogCaptureFixture) -> None:
    monkeypatch.setattr(
        api,
        "get_transformed_podcastchart",
        lambda **kwargs: pd.DataFrame(
            [{"region": "us", "episodeUri": "id1", "episodeName": "Expected"}]
        ),
    )
    monkeypatch.setattr(
        api,
        "get_transformed_search_eps",
        lambda **kwargs: pd.DataFrame({"id": ["id1"], "name": ["Different"]}),
    )

    with caplog.at_level(logging.WARNING):
        api.get_charts_eps(regions=["us"])
    
    assert "Name mismatch found" in caplog.text
    assert "id1" in caplog.text
