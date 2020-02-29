from db import db
weight = {'coin': 0.4,
          'favorite': 0.3,
          'danmaku': 0.4,
          'reply': 0.4,
          'view': 0.25,
          'like': 0.4,
          'share': 0.6}
sum_weight = sum(weight.values())
for video in db['video'].find():
  print(video)
  for each_data in video['data']:
    if 'jannchie' not in each_data:
      jannchie = 0
      temp_weight = 0
      for each_key in weight:
        if each_key in each_data and each_data[each_key] != None:
          jannchie += weight[each_key] * each_data[each_key]
          temp_weight += weight[each_key]
      if temp_weight < sum_weight:
        jannchie = jannchie / temp_weight * sum_weight
      each_data['jannchie'] = int(jannchie)
  db['video'].save(video)
  pass
