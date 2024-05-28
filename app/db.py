from pymongo import MongoClient
from dateutil.relativedelta import *
from bson.json_util import dumps
import datetime
import asyncio
import json
import os

mongoSrv = MongoClient(os.environ['MONGO_STR_SRV'])
mongoDB = mongoSrv[os.environ['MONGO_STR_DB']]
mongoCol = mongoDB[os.environ['MONGO_STR_COL']]


async def aggregation(start_D, end_D, timeAdd) -> list:
    

    agg_match = {
        "$match" : {"dt":{"$gte": start_D , "$lte":end_D}}
    }

    agg_group = {
        "$group": {
            "_id": {"$substr" : ["$dt", 0, 19 - len(timeAdd)]},  
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
    db_list = json.loads(dumps(cursor))

    for x in db_list:
        x['_id'] = f'{x['_id']}{timeAdd}'

    return db_list

async def datelist(start_D, end_D, type_D) -> list:
    jsonn = []
    current_D = start_D
    match type_D:
        case 'hour':
            while current_D<= end_D:
                jsonn.append(datetime.datetime.isoformat(current_D))
                current_D += datetime.timedelta(hours=1)
            return jsonn
        case 'day':
            while current_D<= end_D:
                jsonn.append(datetime.datetime.isoformat(current_D))
                current_D += datetime.timedelta(days=1)
            return jsonn
        case 'month':
            while current_D <= end_D:
                jsonn.append(datetime.datetime.isoformat(current_D))
                current_D += relativedelta(months=+1)
                relativedelta()
            return jsonn
    
async def final(calen_l, db_l) -> dict:
    count = 0
    f = {}
    max_count = len(db_l)
    for x in calen_l:
        if count<max_count and x == db_l[count]['_id']:
            f.update({x : db_l[count]['value']})
            count +=1
            
        else:
            f.update({x : 0})
    return { 
        "dataset" : list(f.values()),
        "labels"  : list(f.keys())
    }
    
async def maindb(qerrry):
    try:
        dt_from = datetime.datetime.fromisoformat(qerrry['dt_from'])
        dt_upto = datetime.datetime.fromisoformat(qerrry['dt_upto'])
        gr_type = qerrry['group_type']
        dt_type = {'hour': ':00:00', 'day': 'T00:00:00', 'month': '-01T00:00:00'}
        dbData, dictDate = await asyncio.gather(aggregation(dt_from, dt_upto, dt_type[gr_type]), datelist(dt_from, dt_upto, gr_type))
        return await final(dictDate, dbData)
    except:
        return 'bad'