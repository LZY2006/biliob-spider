from db import db
import datetime
author_coll = db['author']
authors = author_coll.find({'data.2000': {'$exists': True}})
for each_author in authors:
  print(each_author['name'])
  mid = each_author['mid']
  data = sorted(each_author['data'],
                key=lambda x: x['datetime'], reverse=True)
  c_data = data[0]
  c_date = data[0]['datetime'].strftime('%Y-%m-%d %H')
  f_data = [c_data]
  for each_data in data:
    delta_day = (datetime.datetime.now() -
                 each_data['datetime']).days
    if delta_day > 7:
      n_date = each_data['datetime'].strftime('%Y-%m-%d %H')
      # 如果不是同一小时
      if n_date != c_date:
        f_data.append(each_data)
        c_date = n_date
        pass
      pass
    else:
      f_data.append(each_data)
  author_coll.update_one({'mid': mid}, {'$set': {'data': f_data}})
  pass
pass
