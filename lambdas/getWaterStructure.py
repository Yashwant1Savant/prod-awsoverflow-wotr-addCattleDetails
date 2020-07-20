#!/usr/bin/python

import sys
import logging
import config
import mysql.connector
import json
import time
import calendar
import datetime
import connectionManager

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def getFarmerWaterStructureDetails(item):
    logger.info("***getFarmerWaterStructureDetails for %s", item)
    print(item)
    if('userId' not in item):
        raise ValueError ({"errorMessage": {"status" : "fail", "reason" : "001 - Invalid request"}})
    
    try:
        connection = connectionManager.getConnection()
        cursor = connection.cursor()
        logger.info("SUCCESS: Connection to RDS mysql instance succeeded")
        mobileNumber = item['userId']
        get_userid = "SELECT farmer_water_storage.water_storage_id,farmer_water_storage.capacity FROM farmer_water_storage,user WHERE mobileNo ="+mobileNumber+" AND farmer_water_storage.userId = user.userId"
        cursor.execute(get_userid)
        records_id = cursor.fetchall()
        print(records_id)
        print(records_id[0][0])
        Type_1 = 0
        Type_2 = 0
        Type_3 = 0
        Type_4 = 0
        Type_5 = 0
        Type_6 = 0
        for type in records_id:
            print(type)
            if type[0] == 1:
                Type_1 = type[1]
                response = {"responseCode": "000", "reasons": {"reasonCode": "", "desc": "success"}},{"waterStructure" : [{"unit": "LITER" ,"type": "Percolation Tank","capacity": str(Type_1) },{"unit": "LITER" ,"type": "Cement Check Dam","capacity": str(Type_2) },{"unit": "LITER" ,"type": "Earthen Bund","capacity": str(Type_3) },{"unit": "LITER" ,"type": "Plastic Lined Farm Ponds","capacity": str(Type_4) },{"unit": "LITER" ,"type": "Farm Ponds Without Lining","capacity": str(Type_5) },{"unit": "LITER" ,"type": "CC (Ha)","capacity": str(Type_6) }]}
            if type[0] == 2:
                Type_2 = type[1]
                response = {"responseCode": "000", "reasons": {"reasonCode": "", "desc": "success"}},{"waterStructure" : [{"unit": "LITER" ,"type": "Percolation Tank","capacity": str(Type_1) },{"unit": "LITER" ,"type": "Cement Check Dam","capacity": str(Type_2) },{"unit": "LITER" ,"type": "Earthen Bund","capacity": str(Type_3) },{"unit": "LITER" ,"type": "Plastic Lined Farm Ponds","capacity": str(Type_4) },{"unit": "LITER" ,"type": "Farm Ponds Without Lining","capacity": str(Type_5) },{"unit": "LITER" ,"type": "CC (Ha)","capacity": str(Type_6) }]}
            if type[0] == 3:
                Type_3 = type[1]
                response = {"responseCode": "000", "reasons": {"reasonCode": "", "desc": "success"}},{"waterStructure" : [{"unit": "LITER" ,"type": "Percolation Tank","capacity": str(Type_1) },{"unit": "LITER" ,"type": "Cement Check Dam","capacity": str(Type_2) },{"unit": "LITER" ,"type": "Earthen Bund","capacity": str(Type_3) },{"unit": "LITER" ,"type": "Plastic Lined Farm Ponds","capacity": str(Type_4) },{"unit": "LITER" ,"type": "Farm Ponds Without Lining","capacity": str(Type_5) },{"unit": "LITER" ,"type": "CC (Ha)","capacity": str(Type_6) }]}
            if type[0] == 4:
                Type_4 = type[1]
                response = {"responseCode": "000", "reasons": {"reasonCode": "", "desc": "success"}},{"waterStructure" : [{"unit": "LITER" ,"type": "Percolation Tank","capacity": str(Type_1) },{"unit": "LITER" ,"type": "Cement Check Dam","capacity": str(Type_2) },{"unit": "LITER" ,"type": "Earthen Bund","capacity": str(Type_3) },{"unit": "LITER" ,"type": "Plastic Lined Farm Ponds","capacity": str(Type_4) },{"unit": "LITER" ,"type": "Farm Ponds Without Lining","capacity": str(Type_5) },{"unit": "LITER" ,"type": "CC (Ha)","capacity": str(Type_6) }]}
            if type[0] == 5:
                Type_5 = type[1]
                response = {"responseCode": "000", "reasons": {"reasonCode": "", "desc": "success"}},{"waterStructure" : [{"unit": "LITER" ,"type": "Percolation Tank","capacity": str(Type_1) },{"unit": "LITER" ,"type": "Cement Check Dam","capacity": str(Type_2) },{"unit": "LITER" ,"type": "Earthen Bund","capacity": str(Type_3) },{"unit": "LITER" ,"type": "Plastic Lined Farm Ponds","capacity": str(Type_4) },{"unit": "LITER" ,"type": "Farm Ponds Without Lining","capacity": str(Type_5) },{"unit": "LITER" ,"type": "CC (Ha)","capacity": str(Type_6) }]}
            if type[0] == 6:
                Type_6 = type[1]
                response = {"responseCode": "000", "reasons": {"reasonCode": "", "desc": "success"}},{"waterStructure" : [{"unit": "LITER" ,"type": "Percolation Tank","capacity": str(Type_1) },{"unit": "LITER" ,"type": "Cement Check Dam","capacity": str(Type_2) },{"unit": "LITER" ,"type": "Earthen Bund","capacity": str(Type_3) },{"unit": "LITER" ,"type": "Plastic Lined Farm Ponds","capacity": str(Type_4) },{"unit": "LITER" ,"type": "Farm Ponds Without Lining","capacity": str(Type_5) },{"unit": "LITER" ,"type": "CC (Ha)","capacity": str(Type_6) }]}
            
        return(response)
        
    except Exception as e:
        logger.error(e)
        logger.error("Error: Unexpected error: Error in getFarmerWaterStructureDetails")
        raise
    finally:
        connection.close()
        logger.info("***getFarmerWaterStructureDetails End***")