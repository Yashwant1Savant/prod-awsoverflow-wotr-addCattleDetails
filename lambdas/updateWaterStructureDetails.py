#!/usr/bin/python
import sys
import logging
from lambdas import config
from lambdas import connectionManager
import json
import time
import calendar
import datetime


logger = logging.getLogger()
logger.setLevel(logging.INFO)

def updateWaterStructureDetails(event,context):
    logger.info("***updateWaterStructureDetails for %s", event)
    name = (json.loads(event['body'])).get('userid')
    try:
        connection = connectionManager.getConnection()
        cursor = connection.cursor()
        logger.info("SUCCESS: Connection to RDS mysql instance succeeded")
        for details in range(len(event['body'])):
        
           print(details)
        return dict(
            body=str(details)
            )
    except Exception as e:
        logger.error(e)
        logger.error("Error: Unexpected error: Error in updateWaterStructureDetails")
        raise
    finally:
        connection.close()
        logger.info ("***getFarmerDetail End***")
    

