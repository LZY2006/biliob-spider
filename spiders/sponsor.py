import logging
from bson import ObjectId
from simpyder import Spider, FAKE_UA, SimpyderConfig
import datetime
import redis
from utils import get_url_from_redis
from db import redis_connect_string, db
from time import sleep
import requests


class BiliobSponsorSpider(Spider):

  def gen_url(self):
    pn_list = range(1, 11)
    url = 'https://azz.net/a/api?service=user_orders&type=receive&page={pn}'
    try:
      for pn in pn_list:
        yield url.format(pn=pn)
      while True:
        yield url.format(pn=1)
    except Exception as e:
      logging.exception(e)
      sleep(10)

  def parse(self, res):
    j = res.json()
    return j

  def save(self, item):
    for each in item['data']['list']:
      db.sponsor.update_one({'order_id': each['order_id']}, each, upsert=True)
    return item


if __name__ == "__main__":
  s = BiliobSponsorSpider("biliob-author-follow-spider")
  sc = SimpyderConfig()
  sc.PARSE_THREAD_NUMER = 1
  sc.DOWNLOAD_INTERVAL = 10
  sc.LOG_LEVEL = "INFO"
  sc.USER_AGENT = FAKE_UA
  sc.COOKIE = ''
  s.set_config(sc)
  s.run()
