#!/usr/bin/python

import sys
import logging
import operator
import json
from functools import reduce
from lambdas import connectionManager
from models.masterDetailsModel import MasterDetails

logger = logging.getLogger()
logger.setLevel(logging.INFO)



def getMasterDetails(event, context):
    logger.info (event)
    item = event['pathParameters']['item']
   
    logger.info ("***getMasterDetails for %s", item)
    
    print(item)
    conn = None
    try:
       print("Hello")
       conn = connectionManager.getConnection()
       cursor = conn.cursor()
       logger.info("SUCCESS: Connection to RDS mysql instance succeeded")
       masterDetails = MasterDetails("000")
       if item == "crops" :
         crops_query = "SELECT DISTINCT cropName FROM crops"
         masterDetails.setCrops(__fetchRecords(cursor, crops_query))
       elif item == "waterStructures" :
         water_storage_query = "SELECT DISTINCT water_storage_type FROM water_storages"
         masterDetails.setWaterStructures(__fetchRecords(cursor, water_storage_query))
       elif item == "cattles" :
         cattle_query = "SELECT DISTINCT cattles FROM cattles"
         masterDetails.setCattles(__fetchRecords(cursor, cattle_query))
       else:
         raise ValueError ({"errorMessage": {"status" : "fail", "reason" : "incorrect master details"}})
       
       print(masterDetails)
      
       response = {"household_name": "name", "household_identifier": "unique_id", "village": "Sawarkhede", "state": "Maharashtra", "district": "Maha", "houshold_member_count": "12", "members": [{"name": "xyz", "gender": "F"}]}
       return dict(
        statusCode=200,
        body=str(masterDetails)
   )
       
       
    except Exception as e:
       logger.error(e)
       logger.error("ERROR: Unexpected error: Error in getMasterDetails")
       raise e
       #raise ValueError ({"errorMessage": {"status" : "fail", "reason" : "002 - Unable to retrieve Master details"}})
    finally:
        if conn != None:
            conn.close()
        logger.info ("***getMasterDetails End***")
        
def __fetchRecords(cursor, query):
        cursor.execute(query)
        records = cursor.fetchall()
        return reduce(operator.concat, records)

