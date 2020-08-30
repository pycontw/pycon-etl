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
    def crawl(cls):
        pass


class CakeResumeCrawler(BaseCrawler):
    """
    Crawler of cakeresume
    """

    @classmethod
    def crawl(cls):
        print("i'm a CakeResume crawler!")
        return "i'm a CakeResume crawler!"
