"""
Crawler of openings
"""

from abc import ABC, abstractclassmethod


class BaseCrawler(ABC):
    """
    Abstract cralwer
    """

    @classmethod
    @abstractclassmethod
    def crawl(cls, **conf):
        pass


class CakeResumeCrawler(BaseCrawler):
    """
    Crawler of cakeresume
    """

    @classmethod
    def crawl(cls, **conf):
        print("i'm a CakeResume crawler!")
        return "i'm a CakeResume crawler!"
