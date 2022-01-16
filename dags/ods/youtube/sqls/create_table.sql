CREATE TABLE IF NOT EXISTS `{}.ods.ods_youtubeStatistics_videoId_datetime`
(
    created_at TIMESTAMP NOT NULL,
    videoId STRING NOT NULL,
    title STRING NOT NULL,
    viewCount INT64 NOT NULL,
    likeCount INT64 NOT NULL,
    dislikeCount INT64 NOT NULL,
    favoriteCount INT64 NOT NULL,
    commentCount INT64 NOT NULL
);

CREATE TABLE IF NOT EXISTS `{}.ods.ods_youtubeInfo_videoId_datetime`
(
    created_at TIMESTAMP NOT NULL,
    videoId STRING NOT NULL,
    title STRING NOT NULL,
    image_url STRING NOT NULL,
    subtitle STRING NOT NULL,
    time TIMESTAMP NOT NULL,
    url STRING NOT NULL
);

