#!/usr/bin/python

import sys
import logging
import config
import mysql.connector
import json
import time
import calendar
import datetime

rds_host  = config.db_endpoint
name = config.db_username
password = config.db_password
port = config.db_port

logger = logging.getLogger()
logger.setLevel(logging.INFO)



def getHomeScreenDetails(item):

    logger.info ("***getHomeScreenDetails  Begin***")

    if('abc' not in item):
        raise ValueError ({"errorMessage": {"status" : "fail", "reason" : "001 - Invalid request"}})

    testid = item['abc']

    db_name = config.db_name
    try:

       conn = mysql.connector.connect(host=rds_host, user=name, passwd=password, db=db_name, connect_timeout=5,use_unicode=True, charset="utf8mb4")
       cursor = conn.cursor()
       logger.info("SUCCESS: Connection to RDS mysql instance succeeded")

       get_states_query = "SELECT DISTINCT state FROM " + db_name + ".village"
       #data = ()
       cursor.execute(get_states_query)
       records = cursor.fetchall()
       no_of_states = len(records)

       get_villages_query = "SELECT DISTINCT villageId FROM " + db_name + ".village"
       cursor.execute(get_villages_query)
       records = cursor.fetchall()
       no_of_villages = len(records)

       get_water_planned = "SELECT human_pop_water_need, living_being_pop_water_need, business_const_water_need, crop_water_need FROM " + db_name + ".village_water_need"
       cursor.execute(get_water_planned)
       records = cursor.fetchall()

       total_water_planned = 0
       for rec in records:
           total_water_planned = total_water_planned + rec[0] + rec[1] +  rec[2] + rec[3]


       response = {"responseInfo" : {"responseCode": "000", "reasons": { "reasonCode": "", "desc": "success"}}, "waterBudgetDetail" : {"numberOfStates" : no_of_states, "numberOfVillages" : no_of_villages , "literOfWaterPlanned" : total_water_planned}}
       return (response)

    except Exception as e:
       logger.error(e)
       logger.error("ERROR: Unexpected error: Error in getHomeScreenDetails")
       raise ValueError ({"errorMessage": {"status" : "fail", "reason" : "002 - Unable to retrieve Water details"}})
    finally:
        conn.close()
        logger.info ("***getHomeScreenDetails End***")
