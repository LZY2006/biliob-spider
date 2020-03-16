
import requests
from time import sleep
from db import redis_connect_string, db
from utils import get_url_from_redis
import redis
import datetime
from simpyder import Spider, FAKE_UA, SimpyderConfig
from bson import ObjectId
import logging


class BiliobAuthorFollowSpider(Spider):

  def gen_url(self):
    ps = 50
    pn_list = [1, 2, 3]
    url = 'https://api.bilibili.com/x/relation/followings?vmid={mid}&pn={pn}&ps={ps}'
    while True:
      try:
        authors = db.author.find({
            'follow': {
                '$exists': False
            }
        }, {'mid': 1})
        for each_author in authors:
          for pn in pn_list:
            yield url.format(mid=each_author['mid'], pn=pn, ps=ps)
      except Exception as e:
        logging.exception(e)
        sleep(10)

  def parse(self, res):
    j = res.json()
    item = {
        'mid': int(res.url.split('?')[1].split('&')[0].split('=')[1]),
        'follows': []
    }
    for each_member in j['data']['list']:
      item['follows'].append(each_member['mid'])
    return item

  def save(self, item):
    db.author.update_one({'mid': item['mid']}, {'$addToSet': {
        'follows': {'$each': item['follows']}
    }})
    return item


if __name__ == "__main__":
  s = BiliobAuthorFollowSpider("biliob-author-follow-spider")
  sc = SimpyderConfig()
  sc.PARSE_THREAD_NUMER = 4
  sc.DOWNLOAD_INTERVAL = 1
  sc.LOG_LEVEL = "INFO"
  sc.USER_AGENT = FAKE_UA
  s.set_config(sc)
  s.run()
