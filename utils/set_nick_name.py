from db import db
users = db['user'].find({}, {'name': 1})
for user in users:
    db['user'].update_one({'name': user['name']}, {
        '$set': {'nickName': user['name']}})
    pass
    print(user)
pass
