from db import db
from time import sleep
import logging
import datetime
author_filter = {'mid': 1}


class KeywordAdder():
  def __init__(self):
    super().__init__()
    self.db = db

  def update_keyword_by_author(self, author):
    mid = author['mid']
    keywords = set()
    videos = db.video.find({'mid': mid}, {'keyword': 1}
                           ).sort("cJannchie", -1).limit(10)
    for each_video in videos:
      if 'keyword' not in each_video:
        continue
      for each_keyword in each_video['keyword']:
        keywords.add(each_keyword)
    self.db.author.update_one({'mid': mid}, {
        '$addToSet': {
            'keyword': {'$each': list(keywords)}
        }
    })
    return keywords

  def get_author_gt(self, start):
    return self.db.author.find({'mid': {'$gt': start}}, author_filter, no_cursor_timeout=True).limit(100)

  def add_all(self):
    start = 0
    while True:
      cursor = self.get_author_gt(start)
      try:
        authors = list(cursor)
        if len(authors) == 0:
          break
        for each_author in authors:
          start = each_author['mid']
          self.update_keyword_by_author(each_author)
          print('[{}] {}'.format(datetime.datetime.now(), start))
      finally:
        cursor.close()


if __name__ == "__main__":
  ka = KeywordAdder()
  ka.add_all()
  pass
