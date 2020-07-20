#!/usr/bin/python

import sys
import logging
import operator
import json
import connectionManager

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def getHouseholdDetails(item):
    logger.info("***getHouseholdDataDetails for %s", item)
    print(item)
    try:
       connection = connectionManager.getConnection()
       cursor = connection.cursor()
       logger.info("SUCCESS: Connection to RDS mysql instance succeeded")
       householdIdentifier = item['household_identifier']
       print("Household Identifier :", householdIdentifier)
       responseInfo = {"responseCode": "000", "reasons": {"reasonCode": "", "desc": "success"}}
       responseData = {"household_name": "name", "household_identifier": "xy", "village": "Sawarkhede", "state": "Maharashtra", "district": "Maha", "houshold_members": {"count": "12", "male": "6", "female": "6"}, "land_area": "200", "cattel_count": "20", "water_structures": "5"}
       response = { 'responseInfo' : responseInfo, 'householdDetails' : responseData  }
       
       return(response)
    except Exception as e:
       logger.error(e)
       logger.error("ERROR: Unexpected error: Error in getHouseholdDataDetails")
       raise 
    finally:
        connection.close()
        logger.info ("***getHouseholdDataDetails End***")
    