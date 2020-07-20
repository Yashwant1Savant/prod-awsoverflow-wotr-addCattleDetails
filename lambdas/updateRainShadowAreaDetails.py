#!/usr/bin/python

import sys
import logging
import operator
import json
import connectionManager

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def updateRainShadowAreaDetails(item):
    logger.info("***updateRainShadowAreaDetails for %s", item)
    print(item)
    if('userId' not in item):
        raise ValueError ({"errorMessage": {"status" : "fail", "reason" : "001 - Invalid request"}})
    try:
        connection = connectionManager.getConnection()
        cursor = connection.cursor()
        logger.info("SUCCESS: Connection to RDS mysql instance succeeded")
        mobileNumber = item['userId']
        print("Mobile Number :", mobileNumber)
        print("Farmer's Rain Shadow Details:-")
        print(item['rainShadowRegion']['unit'],"-",item['rainShadowRegion']['area'])
    except Exception as e:
        logger.error(e)
        logger.error("Error: Unexpected error: Error in updateRainShadowAreaDetails")
        raise ValueError({"errorMessage": {"status": "fail", "reason": "Unable to updateRainShadowAreaDetails"}})
    finally:
        connection.close()
        logger.info("***updateRainShadowAreaDetails End***")
        