import requests
from time import sleep
from db import redis_connect_string, db
from utils import get_url_from_redis
import redis
import datetime
from simpyder import Spider
from simpyder import FAKE_UA
from simpyder import SimpyderConfig
from bson import ObjectId


class BiliobAuthorSpider(Spider):
  def sentCallBack(self, object_id, coll):
    if object_id != None and object_id != 'null':
      coll.update_one({'_id': ObjectId(object_id)}, {
          '$set': {'isExecuted': True}})

  def gen_url(self):
    while True:
      url = get_url_from_redis("authorRedis:start_urls")
      yield url

  def parse(self, res):
    j = res.json()
    if 'data' in j and j['data'] == None:
      mid = int(res.url.split("=")[1].split('&')[0])
      saved_data = db['author'].find_one({'mid': mid})
      if saved_data == None or 'data' not in saved_data:
        db['author_interval'].remove({'mid': mid})
        db['author'].remove({'mid': mid})

      raise Exception
    name = j['data']['card']['name']
    mid = j['data']['card']['mid']
    sex = j['data']['card']['sex']
    face = j['data']['card']['face']
    fans = j['data']['card']['fans']
    attention = j['data']['card']['attention']
    level = j['data']['card']['level_info']['current_level']
    official = j['data']['card']['Official']['title']
    archive = j['data']['archive_count']
    article = j['data']['article_count']
    face = j['data']['card']['face']
    item = {}
    item['mid'] = int(mid)
    item['name'] = name
    item['face'] = face
    item['official'] = official
    item['sex'] = sex
    item['level'] = int(level)
    item['data'] = {
        'fans': int(fans),
        'attention': int(attention),
        'archive': int(archive),
        'article': int(article),
        'datetime': datetime.datetime.now()
    }
    item['c_fans'] = int(fans)
    item['c_attention'] = int(attention)
    item['c_archive'] = int(archive)
    item['c_article'] = int(article)

    url_list = res.url.split('&')
    if len(url_list) == 2:
      item['object_id'] = url_list[1]
    else:
      item['object_id'] = None
    view_data_res = s.get(
        "https://api.bilibili.com/x/space/upstat?mid={mid}".format(mid=mid))
    j = view_data_res.json()
    if j['data'] == None:
      mid = int(res.url.split("=")[1].split('&')[0])
      saved_data = db['author'].find_one({'mid': mid})
      if saved_data == None or 'data' not in saved_data:
        db['author'].remove({'mid': mid})
        db['author_interval'].remove({'mid': mid})
      raise Exception
    archive_view = j['data']['archive']['view']
    article_view = j['data']['article']['view']
    like = j['data']['likes']
    item['data']['archiveView'] = archive_view
    item['data']['articleView'] = article_view
    item['data']['like'] = like
    item['c_like'] = like
    now = datetime.datetime.now()
    c = coll.aggregate([
        {
            "$match": {
                "mid": item['mid']
            }
        }, {
            "$unwind": "$data"
        }, {
            "$match": {
                "data.datetime": {"$gt": now - datetime.timedelta(1.1)}
            }
        }, {
            "$sort": {"data.datetime": 1}
        }, {
            "$limit": 1
        }, {
            "$project": {
                "datetime": "$data.datetime",
                "like": "$data.like",
                "fans": "$data.fans",
                "archiveView": "$data.archiveView",
                "articleView": "$data.articleView"
            }
        }
    ])
    item['c_archive_view'] = int(archive_view)
    item['c_article_view'] = int(article_view)
    item['c_rate'] = 0
    for each in c:
      delta_seconds = now.timestamp() - each['datetime'].timestamp()
      delta_fans = item['data']['fans'] - each['fans']
      item['c_rate'] = int(delta_fans / delta_seconds * 86400)

    return item

  def save(self, item):
    coll.update_one({
        'mid': item['mid']
    }, {
        '$set': {
            'focus': True,
            'sex': item['sex'],
            'name': item['name'],
            'face': item['face'],
            'level': item['level'],
            'cFans': item['c_fans'],
            'cLike': item['c_like'],
            'cRate': item['c_rate'],
            'cArchive_view': item['c_archive_view'],
            'cArchive': item['c_archive'],
            'cArticle_view': item['c_article_view'],
            'cArticle': item['c_article'],
            'official': item['official'],
        },
        '$push': {
            'data': {
                '$each': [item['data']],
                '$position': 0
            }
        }
    }, True)
    if 'object_id' in item:
      self.sentCallBack(item['object_id'], db['user_record'])
    return(item)


if __name__ == "__main__":
  s = BiliobAuthorSpider("biliob-author-spider")

  sc = SimpyderConfig()
  sc.PARSE_THREAD_NUMER = 1
  sc.LOG_LEVEL = "INFO"
  sc.USER_AGENT = FAKE_UA
  sc.DOWNLOAD_INTERVAL = 0.15
  s.set_config(sc)

  coll = db['author']
  s.run()
