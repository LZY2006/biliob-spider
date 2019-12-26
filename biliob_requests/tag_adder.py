import redis
import time
import logging
from db import redis_connect_string
import requests
from lxml import etree
from pymongo import MongoClient
from db import db
import threading
import queue
redis_connection = redis.from_url(redis_connect_string)
logging.basicConfig(level=logging.INFO,
                    format='[%(asctime)s] %(levelname)s @ %(name)s: %(message)s')
headers = {
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0 Win64 x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/69.0.3497.81 Safari/537.36'}
coll = db['video']


class ParseThread(threading.Thread):

    def __init__(self, queue, queueLock, get_response, parse, save):
        threading.Thread.__init__(self)
        self.name = 'ParseThread'
        self.queue = queue
        self.queueLock = queueLock
        self.get_response = get_response
        self.parse = parse
        self.save = save

    def get_key(self):
        self.queueLock.acquire()
        if not self.queue.empty():
            key = self.queue.get()
        else:
            key = None
        self.queueLock.release()
        return key

    def run(self):
        while True:
            try:
                key = self.get_key()
                if key == None:
                    time.sleep(1)
                    continue

                response = self.get_response(key)
                item = self.parse(response, key)
                self.save(item)
            except Exception as error:
                # 出现错误时打印错误日志
                logging.error("SAVE ERROR")
                print(error)


def funcname(self, parameter_list):
    raise NotImplementedError


class Spider():
    aid_queue = queue.Queue(10000)
    queueLock = threading.Lock()

    def get_key(self):
        raise NotImplementedError

    def get_response(self, key):
        raise NotImplementedError

    def parse(self, response, key=None):
        raise NotImplementedError

    def save(self, item):
        raise NotImplementedError

    def __init__(self):
        for i in range(8):
            ParseThread(self.aid_queue, self.queueLock,
                        self.get_response, self.parse, self.save).start()
        self.get_key()


class TagSpider(Spider):

    def get_key(self):
        while True:
            try:
                aid = redis_connection.lpop("tag_task")
            except Exception:
                redis_connection = redis.from_url(redis_connect_string)
                continue
            if aid != None:
                self.queueLock.acquire()
                self.aid_queue.put(aid.decode())
                self.queueLock.release()
            else:
                time.sleep(1)

    def get_response(self, key):
        url = 'https://www.bilibili.com/video/av{}'.format(key)
        response = requests.get(url, headers=headers)
        return response

    def parse(self, response, key):
        html = etree.HTML(response.text)
        item = {}
        item['aid'] = int(key)
        item['tag_list'] = html.xpath("//li[@class='tag']/a/text()")
        logging.info(item)
        return item

    def save(self, item):
        coll.update_one({
            'aid': item['aid']
        }, {
            '$set': {
                'tag': item['tag_list'],
            },
        }, True)


TagSpider()
