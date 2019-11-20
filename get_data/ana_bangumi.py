from db import db
from scipy.interpolate import interp1d

import datetime


def date_range(start_date, end_date, step, f="%Y-%m-%d"):
    o = []
    c_date = start_date
    while c_date <= end_date:
        o.append(c_date)
        c_date += datetime.timedelta(days=step)
    if c_date != end_date:
        o.append(c_date)
    return o


start_date = datetime.datetime(2019, 1, 1)
end_date = datetime.datetime.now()
dr = date_range(start_date, end_date, 1)

docs = db['bangumi'].find()
f = open('D:/DataSource/bangumi.csv', 'w', encoding='utf-8-sig')
f.writelines('"{}","{}","{}"\n'.format('name', 'date', 'value'))
for each_doc in docs:
    value_list = list(map(lambda item: item['pts'], each_doc['data']))
    date_list = list(
        map(lambda item: item['datetime'].timestamp(), each_doc['data']))
    print(each_doc['title'])
    if len(value_list) <= 1:

        continue
    fun = interp1d(date_list, value_list)
    t_max = max(date_list)
    t_min = min(date_list)
    for each_date in dr:
        d = each_date.timestamp()
        if d >= t_min and d <= t_max:
            f.writelines('"{}","{}","{}"\n'.format(
                each_doc['title'], each_date, fun(d)))
            pass
    fun = interp1d(date_list, value_list)
