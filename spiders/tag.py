
import requests
from time import sleep
from db import redis_connect_string, db
from utils import get_url_from_redis
import redis
import datetime
from simpyder import Spider, FAKE_UA, SimpyderConfig
from bson import ObjectId
import logging


class BiliobAuthorSpider(Spider):
  def gen_url(self):
    while True:
      try:
        videos = db.video.find({
            'tag': {
                '$exists': False
            }
        }, {'aid': 1})
        for each_video in videos:
          yield 'https://www.bilibili.com/video/av{}'.format(each_video['aid'])
      except Exception as e:
        logging.exception(e)
      sleep(10)

  def parse(self, res):
    aid = str(
        res.url.lstrip(
            'https://www.bilibili.com/video/av').rsplit('?')[0])
    tagName = res.xpath("//li[@class='tag']/a/text()")
    item = {}
    item['aid'] = int(aid)
    item['tag_list'] = []
    if tagName != []:
      ITEM_NUMBER = len(tagName)
      for i in range(0, ITEM_NUMBER):
        item['tag_list'].append(tagName[i])
      return item

  def save(self, item):
    db.video.update_one({
        'aid': item['aid']
    }, {
        '$set': {
            'tag': item['tag_list']
        }
    }, True)
    return item


if __name__ == "__main__":
  s = BiliobAuthorSpider("biliob-author-spider")

  sc = SimpyderConfig()
  sc.PARSE_THREAD_NUMER = 8
  sc.LOG_LEVEL = "INFO"
  sc.USER_AGENT = FAKE_UA
  s.set_config(sc)
  s.run()
