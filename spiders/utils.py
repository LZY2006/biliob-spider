
from db import redis_connect_string, redis_connection
import redis


def get_url_from_redis(key, redis_connection=redis_connection):
  while True:
    try:
      url = redis_connection.lpop(key)
    except Exception:
      redis_connection = redis.from_url(redis_connect_string)
      url = redis_connection.lpop(key)
    if url != None:
      return url
    else:
      sleep(1)
