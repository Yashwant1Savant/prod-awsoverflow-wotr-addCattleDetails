#!/usr/bin/python

import sys
import logging
import operator
import json
from lambdas import connectionManager

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def updateCattleDetails(event, context):
    logger.info("***updateCattleDetails for %s", event)
    item = event['body']
    try:
        connection = connectionManager.getConnection()
        cursor = connection.cursor()
        userId = (json.loads(event['body'])).get('userId')
        cattleDetails = (json.loads(event['body'])).get('cattleDetails')
        for detail in cattleDetails:
            get_village_living_being_id = "Select wotr_cattle_master.cattle_cd From wotrdev.wotr_cattle_master, wotrdev.wotr_farmer_cattle Where wotr_farmer_cattle.farmer_id = "+userId+" AND wotr_cattle_master.cattle_description = \""+detail['id']+"\" AND wotr_farmer_cattle.cattle_cd = wotr_cattle_master.cattle_cd"
            cursor.execute(get_village_living_being_id)
            records_cattle = cursor.fetchall()
            update_cattle_count = "Update wotr_farmer_cattle Set wotr_farmer_cattle.quantity = \""+str(detail['count'])+"\" Where wotr_farmer_cattle.cattle_cd = "+str(records_cattle[0][0])+" AND wotr_farmer_cattle.farmer_id = "+userId
            cursor.execute(update_cattle_count)
            connection.commit()
        return dict(
            statusCode=200
        )
    except Exception as e:
        logger.error(e)
        logger.error("Error: Unexpected error: Error in updateCattleDetails")
        raise ValueError({"errorMessage": {"status": "fail", "reason": "Unable to updateCattleDetails"}})
    finally:
        connection.close()
        logger.info("***updateCattleDetails End***")