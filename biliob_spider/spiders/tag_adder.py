# coding=utf-8
import datetime
import json
import time

import redis
import scrapy
from pymongo import MongoClient
from scrapy.http import Request
from scrapy_redis.spiders import RedisSpider

from biliob_spider.items import TagListItem
from biliob_tracer.task import SpiderTask
from db import db


class TagAdderSpider(RedisSpider):
  name = "tagAdder"
  allowed_domains = ["bilibili.com"]

  start_urls = []

  custom_settings = {
      'ITEM_PIPELINES': {
          'biliob_spider.pipelines.TagAdderPipeline': 300
      },
  }

  def __init__(self):
    self.db = db

  def start_requests(self):
    for i in self.start_urls:
      yield Request(i, meta={
          'dont_redirect': True,
          'handle_httpstatus_list': [302]
      }, callback=self.parse)

  def parse(self, response):
    try:
      aid = str(
          response.url.lstrip(
              'https://www.bilibili.com/video/av').rstrip('/'))
      tagName = response.xpath("//li[@class='tag']/a/text()").extract()
      item = TagListItem()
      item['aid'] = int(aid)
      item['tag_list'] = []
      if tagName != []:
        ITEM_NUMBER = len(tagName)
        for i in range(0, ITEM_NUMBER):
          item['tag_list'].append(tagName[i])
      yield item
    except Exception as error:
      # 出现错误时打印错误日志
      print(error)
      item = TagListItem()
      item['aid'] = int(aid)
      item['tag_list'] = []
      yield item
