CREATE TABLE IF NOT EXISTS `{}`
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
