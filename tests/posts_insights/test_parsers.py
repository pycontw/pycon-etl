from unittest.mock import Mock, patch

import pytest
from utils.posts_insights.facebook import FacebookPostsInsightsParser
from utils.posts_insights.instagram import InstagramPostsInsightsParser
from utils.posts_insights.linkedin import LinkedinPostsInsightsParser
from utils.posts_insights.twitter import TwitterPostsInsightsParser

PARSERS = [
    TwitterPostsInsightsParser,
    LinkedinPostsInsightsParser,
    InstagramPostsInsightsParser,
    FacebookPostsInsightsParser,
]


@pytest.fixture(autouse=True)
def block_external_clients():
    with (
        patch(
            "utils.posts_insights.base.bigquery.Client",
            side_effect=AssertionError("Real BigQuery disabled"),
        ),
        patch(
            "requests.sessions.Session.request",
            side_effect=AssertionError("Real HTTP disabled"),
        ),
    ):
        yield


@pytest.mark.parametrize("parser_class", PARSERS)
def test_table_destinations(parser_class):
    parser = parser_class()
    platform = {
        TwitterPostsInsightsParser: "twitter",
        LinkedinPostsInsightsParser: "linkedin",
        InstagramPostsInsightsParser: "ig",
        FacebookPostsInsightsParser: "fb",
    }[parser_class]
    assert parser.POST_TABLE_NAME == f"ods_pycontw_{platform}_posts"
    assert parser.INSIGHT_TABLE_NAME == f"ods_pycontw_{platform}_posts_insights"
    assert (
        f"`pycontw-225217.ods.{parser.POST_TABLE_NAME}`"
        in parser.CREATE_POSTS_TABLE_SQL
    )
    assert (
        f"`pycontw-225217.ods.{parser.INSIGHT_TABLE_NAME}`"
        in parser.CREATE_INSIGHTS_TABLE_SQL
    )


@pytest.mark.parametrize(
    ("parser_class", "post", "post_id", "favorite", "reply", "retweet"),
    [
        (
            TwitterPostsInsightsParser,
            {
                "tweet_id": "123",
                "timestamp": 100,
                "text": "hello",
                "favorite_count": 2,
                "reply_count": 3,
                "retweet_count": 4,
                "views": 5,
            },
            "123",
            2,
            3,
            4,
        ),
        (
            LinkedinPostsInsightsParser,
            {
                "urn": "456",
                "postedDateTimestamp": 100,
                "text": "hello",
                "likeCount": 2,
                "commentsCount": 3,
                "repostsCount": 4,
            },
            "456",
            2,
            3,
            4,
        ),
        (
            InstagramPostsInsightsParser,
            {
                "id": "789",
                "timestamp": 100,
                "caption": "hello",
                "like_count": 2,
                "comments_count": 3,
            },
            "789",
            2,
            3,
            "0",
        ),
        (
            FacebookPostsInsightsParser,
            {
                "id": "1_2",
                "created_time": "2026-10-25T00:00:00+0000",
                "message": "hello",
                "comments": {"summary": {"total_count": 3}},
                "reactions": {"summary": {"total_count": 2}},
                "shares": {"count": 4},
            },
            "1_2",
            2,
            3,
            4,
        ),
    ],
)
def test_process_records(parser_class, post, post_id, favorite, reply, retweet):
    parser = parser_class()
    row = parser._process_posts([post])[0]
    assert row["id"] == post_id
    assert row["message"] == "hello"
    insights = parser._process_posts_insights([post])[0]
    assert insights["post_id"] == post_id
    assert insights["query_time"] > 0
    fields = (
        ("reactions", "comments", "share")
        if parser_class == FacebookPostsInsightsParser
        else ("favorite", "reply", "retweet")
    )
    assert [insights[field] for field in fields] == [favorite, reply, retweet]


@pytest.mark.parametrize("parser_class", PARSERS)
def test_load_filters_invalid_ids_and_deduplicates_posts(parser_class):
    parser = parser_class()
    client = Mock()
    parser.__dict__["bq_client"] = client
    records = [
        {"id": value}
        for value in [None, "", " ", "0", 0, 123, " 123", "123", "123", "1_2"]
    ]
    parser._dump_posts_to_bigquery(records)
    assert client.load_table_from_json.call_args.args[0] == [
        {"id": "123"},
        {"id": "1_2"},
    ]
    assert client.load_table_from_json.call_args.args[1].endswith(
        parser.POST_TABLE_NAME
    )
    client.load_table_from_json.return_value.result.assert_called_once()
    client.reset_mock()
    parser._dump_posts_insights_to_bigquery(
        [
            {"post_id": "0"},
            {"post_id": "123", "query_time": 1},
            {"post_id": "123", "query_time": 2},
        ]
    )
    assert len(client.load_table_from_json.call_args.args[0]) == 2
    assert client.load_table_from_json.call_args.args[1].endswith(
        parser.INSIGHT_TABLE_NAME
    )


def test_empty_load_and_ddl_wait():
    parser = TwitterPostsInsightsParser()
    client = Mock()
    parser.__dict__["bq_client"] = client
    parser._dump_posts_to_bigquery([{"id": "0"}])
    client.load_table_from_json.assert_not_called()
    parser.create_tables_if_not_exists()
    assert client.query.return_value.result.call_count == 2
    client.query.return_value.result.side_effect = RuntimeError("DDL failed")
    with pytest.raises(RuntimeError, match="DDL failed"):
        parser.create_tables_if_not_exists()
    parser.CREATE_POSTS_TABLE_SQL = ""
    with pytest.raises(ValueError, match="Both"):
        parser.create_tables_if_not_exists()


def response(payload, status=200):
    return Mock(ok=status == 200, status_code=status, json=Mock(return_value=payload))


def test_instagram_pagination_and_optional_fields():
    parser = InstagramPostsInsightsParser()
    next_url = f"{parser.API_BASE_URL}/17841405043609765/media?after=cursor"
    with (
        patch("utils.posts_insights.instagram.Variable.get", return_value="fake-token"),
        patch(
            "utils.posts_insights.instagram.requests.get",
            side_effect=[
                response(
                    {
                        "data": [{"id": "1"}, {}, {"id": "0"}],
                        "paging": {"next": next_url},
                    }
                ),
                response({"id": "1", "timestamp": "2026-10-25T00:00:00+0000"}),
                response({"data": [{"id": "1"}, {"id": "2"}]}),
                response(
                    {
                        "id": "2",
                        "timestamp": "2026-10-24T00:00:00+0000",
                        "caption": "second",
                        "like_count": 10,
                    }
                ),
            ],
        ) as get,
    ):
        posts = parser._request_posts_data()
    assert [post["id"] for post in posts] == ["1", "2"]
    assert posts[0]["caption"] == "No Content"
    assert posts[0]["like_count"] is None
    assert posts[1]["like_count"] == 10
    assert get.call_args_list[0].kwargs["params"]["limit"] == "100"
    assert get.call_args_list[2].args[0] == next_url
    assert get.call_args_list[2].kwargs["params"] is None


@pytest.mark.parametrize(
    "detail", [{}, {"id": "0"}, {"id": "1"}, {"id": "1", "timestamp": "invalid"}]
)
def test_instagram_invalid_detail_is_skipped(detail, caplog):
    with (
        patch("utils.posts_insights.instagram.Variable.get", return_value="fake"),
        patch(
            "utils.posts_insights.instagram.requests.get",
            side_effect=[response({"data": [{"id": "1"}]}), response(detail)],
        ),
    ):
        assert InstagramPostsInsightsParser()._request_posts_data() == []
    assert "Skipping Instagram" in caplog.text


@pytest.mark.parametrize(
    "failed_response", [response({}, 400), response({"error": {"code": 190}})]
)
@pytest.mark.parametrize("on_detail", [False, True])
def test_instagram_api_errors_raise(failed_response, on_detail):
    responses = (
        [response({"data": [{"id": "1"}]}), failed_response]
        if on_detail
        else [failed_response]
    )
    with (
        patch("utils.posts_insights.instagram.Variable.get", return_value="fake"),
        patch("utils.posts_insights.instagram.requests.get", side_effect=responses),
        pytest.raises(RuntimeError, match="Instagram API"),
    ):
        InstagramPostsInsightsParser()._request_posts_data()


def test_instagram_empty_page(caplog):
    with (
        patch("utils.posts_insights.instagram.Variable.get", return_value="fake"),
        patch(
            "utils.posts_insights.instagram.requests.get",
            return_value=response({"data": []}),
        ),
        caplog.at_level("INFO"),
    ):
        assert InstagramPostsInsightsParser()._request_posts_data() == []
    assert "no usable posts" in caplog.text


def test_instagram_pagination_cycle():
    parser = InstagramPostsInsightsParser()
    url = f"{parser.API_BASE_URL}/17841405043609765/media"
    with (
        patch("utils.posts_insights.instagram.Variable.get", return_value="fake"),
        patch(
            "utils.posts_insights.instagram.requests.get",
            return_value=response({"data": [], "paging": {"next": url}}),
        ),
        pytest.raises(RuntimeError, match="pagination cycle"),
    ):
        parser._request_posts_data()


@pytest.mark.parametrize("parser_class", PARSERS)
def test_incremental_filter(parser_class):
    from datetime import datetime, timezone

    parser = parser_class()
    last_post = {"created_at": datetime(2026, 10, 24, tzinfo=timezone.utc)}
    field = (
        "postedDateTimestamp"
        if parser_class == LinkedinPostsInsightsParser
        else "timestamp"
    )
    if parser_class == FacebookPostsInsightsParser:
        posts = [
            {"created_time": value}
            for value in [
                "2026-10-23T00:00:00+0000",
                "2026-10-24T00:00:00+0000",
                "2026-10-25T00:00:00+0000",
            ]
        ]
    else:
        posts = [
            {field: last_post["created_at"].timestamp() + offset}
            for offset in [-1, 0, 1]
        ]
    assert parser._filter_new_posts(posts, last_post) == [posts[2]]


def test_save_uses_incremental_posts_and_all_insights():
    from datetime import datetime, timezone

    parser = TwitterPostsInsightsParser()
    client = Mock()
    parser.__dict__["bq_client"] = client
    client.query.return_value = [
        {"created_at": datetime.fromtimestamp(100, tz=timezone.utc)}
    ]
    old = {
        "tweet_id": "1",
        "timestamp": 99,
        "text": "old",
        "favorite_count": 2,
        "reply_count": 3,
        "retweet_count": 4,
        "views": 5,
    }
    new = {**old, "tweet_id": "2", "timestamp": 101, "text": "new"}
    with patch.object(parser, "_request_posts_data", return_value=[old, new, new]):
        parser.save_posts_and_insights()
    posts_load, insights_load = client.load_table_from_json.call_args_list
    assert [post["id"] for post in posts_load.args[0]] == ["2"]
    assert [row["post_id"] for row in insights_load.args[0]] == ["1", "2", "2"]
    assert "ods_pycontw_twitter_posts`" in client.query.call_args.args[0]
