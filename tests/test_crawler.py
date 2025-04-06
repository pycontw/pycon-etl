"""
test crawler
"""

from dags.ods.opening_crawler.udfs.crawlers import CakeResumeCrawler


def test_demo() -> None:
    if __debug__:
        if CakeResumeCrawler.crawl() != "i'm a CakeResume crawler!":
            raise AssertionError("CakeResumeCrawler Error!")
