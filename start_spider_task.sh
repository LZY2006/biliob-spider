#!/bin/bash


simpyders=(author video)
for spider in ${simpyders[@]} 
do 
    ps -ef | grep $spider | grep -v grep | cut -c 9-15 | xargs kill -9 
    nohup python ./spiders/$spider.py -L INFO 1>log-$spider.out 2>&1 &
done 

spiders=(bangumiAndDonghua authorAutoAdd videoAutoAdd site online)

for var in ${spiders[@]} 
do 
    ps -ef | grep $var | grep -v grep | cut -c 9-15 | xargs kill -9 
    nohup scrapy crawl $var -L INFO 1>$var.log 2>&1 &
done 
ps -ef | grep tag_adder | grep -v grep | cut -c 9-15 | xargs kill -9 
nohup python ./biliob_requests/tag_adder.py 1>tag_adder.log 2>&1 &
ps -ef | grep DanmakuAggregate | grep -v grep | cut -c 9-15 | xargs kill -9 
cd danmaku_spider/ && nohup scrapy crawl DanmakuAggregate 1>log.log 2>&1 &
ps -ef |grep py

