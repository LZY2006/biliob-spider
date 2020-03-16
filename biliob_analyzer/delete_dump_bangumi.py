from db import db
for each_bangumi in db.bangumi.find({}, {'sid': 1}):
  count = db.bangumi.count({'sid': each_bangumi['sid']})
  while count > 1:
    db.bangumi.delete_one({'sid': each_bangumi['sid']})
    print(each_bangumi['sid'])
    pass
