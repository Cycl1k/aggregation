from pymongo import MongoClient
from dotenv import dotenv_values
from dateutil.relativedelta import *
from bson.json_util import dumps
import datetime
import asyncio
import time
import json

config = dotenv_values('.env')

mongoSrv = MongoClient(config['MONGO_STR_SRV'])
mongoDB = mongoSrv[config['MONGO_STR_DB']]
mongoCol = mongoDB[config['MONGO_STR_COL']]

async def aggregation(start_D, end_D, num) -> list:
    
    agg_match = {
        "$match" : {"dt":{"$gte": start_D , "$lte":end_D}}
    }

    agg_group = {
        "$group": {
            "_id": {"$substr" : ["$dt", 0, num]},  
            "value": {"$sum": '$value'}
        }
    }

    agg_sort = {
        "$sort":({"_id": 1 })
    }

    pipeline = [
        agg_match,
        agg_group,
        agg_sort,
    ]
    cursor = mongoCol.aggregate(pipeline)
    return json.loads(dumps(cursor))

async def timing(type_D, db_list):
    match type_D:
        case 13:
            for x in db_list:
                x['_id'] += ":00:00"
            return db_list
        case 10 :
            for x in db_list:
                x['_id'] += "T00:00:00"
            return db_list
        case 7 :
            for x in db_list:
                x['_id'] += "-01T00:00:00"
            return db_list
        case _:
            return '))))'

async def typing(type_D):
    match type_D:
        case "hour":
            return 13
        case "day" :
            return  10
        case "month" :
            return  7
        case _:
            return 0

async def datelist(start_D, end_D, type_D) -> list:
    jsonn = []
    current_D = start_D
    if type_D == 13:
        while current_D<= end_D:
            jsonn.append(datetime.datetime.isoformat(current_D))
            current_D += datetime.timedelta(hours=1)
        return jsonn
    elif type_D == 10:
        while current_D <= end_D:
            jsonn.append(datetime.datetime.isoformat(current_D))
            current_D += datetime.timedelta(days=1)
        return jsonn
    elif type_D == 7:
        while current_D <= end_D:
            jsonn.append(datetime.datetime.isoformat(current_D))
            current_D += relativedelta(months=+1)
        return jsonn
    else:
        return ")))"
    
    
async def final(calen_l, db_l,type_D):
    count = 0
    f = {}
    max_count = len(db_l)
    for x in calen_l:
        if count<max_count and x == db_l[count]['_id']:
            print(x, db_l[count]['value'])
            f.update({x : db_l[count]['value']})
            count +=1
        else:
            print ( x, 0)
            f.update({x : 0})
    return { 
        "dataset" : list(f.values()),
        "labels"  : list(f.keys())
    }

async def main(test):
    dt_from = datetime.datetime.fromisoformat(test['dt_from'])
    dt_upto = datetime.datetime.fromisoformat(test['dt_upto'])
    num = await typing(test['group_type'])
    dbData = await aggregation(dt_from, dt_upto, num)
    dbData_plus = await timing(num, dbData)
    dictDate = await datelist(dt_from, dt_upto, num)
    return await final(dictDate, dbData_plus, num)

# dt = {"dt_from": "2022-10-01T00:00:00", "dt_upto": "2022-12-11T00:00:00", "group_type": "hour"}
# print(asyncio.run(main(dt)))