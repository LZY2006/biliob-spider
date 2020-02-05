from db import db


def get_subchannel_top_author(subChannel: str, limit: int):
    video_coll = db['video']
    author_coll = db['author']
    result = video_coll.aggregate(
        [{'$match': {'subChannel': subChannel}}, {'$group': {"_id": "$mid", "sum_view": {"$sum": "$cView"}, }},  {"$match": {'sum_view': {'$ne': 0}}}, {"$sort": {'sum_view': -1}}, {"$limit": limit}])
    result = list(map(lambda x: x['_id'], result))
    print("筛选数量：", len(result))
    return result


def get_Tag_top_author(tag: str, limit: int):
    video_coll = db['video']
    author_coll = db['author']
    result = video_coll.aggregate(
        [{'$match': {'tag': tag}}, {'$group': {"_id": "$mid", "sum_view": {"$sum": "$cView"}, }},  {"$match": {'sum_view': {'$ne': 0}}}, {"$sort": {'sum_view': -1}}, {"$limit": limit}])
    result = list(map(lambda x: x['_id'], result))
    print("筛选数量：", len(result))
    return result
