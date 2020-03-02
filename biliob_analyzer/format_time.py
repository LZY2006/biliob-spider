import datetime
from db import db
for video in db.video.find({'datetime': {'$type': 'string'}}):
  date = datetime.datetime.strptime(
      video['datetime'], '%Y-%m-%dT%H:%M:%S.000Z'
  )
  db.video.update_one({'aid': video['aid']}, {'$set': {
      'datetime': date
  }})
