#!/usr/bin/python

import sys
import logging
import operator
import json
from functools import reduce
from lambdas import connectionManager
from lambdas import config
from models.responseInfoModel import ResponseInfo


logger = logging.getLogger()
logger.setLevel(logging.INFO)

db_name = config.db_name;
class CattleDetails(ResponseInfo):
    def __init__(self, responseCode):
        self.responseCode = responseCode
        self.detailsList = []
        super().__init__(responseCode)
    
    def __str__(self):
        return super().__str__("farmerCattleDetail", {'cattles': self.detailsList})
        
def getFarmerCattleDetails(event,context):
    
    #logger.info("***getFarmerCattleDetails for %s", event)
    item = event['pathParameters']['item']
    #item= 1
    try:
        connection = connectionManager.getConnection()
        cursor = connection.cursor()
        logger.info("SUCCESS: Connection to RDS mysql instance succeeded")
        get_cattle_details = "select "+db_name+".wotr_cattle_master.cattle_description,"+db_name+".wotr_farmer_cattle.quantity from "+db_name+".wotr_farmer_cattle inner join "+db_name+".wotr_cattle_master on "+db_name+".wotr_cattle_master.cattle_cd = "+db_name+".wotr_farmer_cattle.cattle_cd where "+db_name+".wotr_farmer_cattle.farmer_id ="+str(item)
        cursor.execute(get_cattle_details)
        cattleList = cursor.fetchall()
        #print(cattleList)
        detailsList = []
        cattleRes = dict(cattleList)
        for name in cattleRes:
            cattleResponse = CattleDetails("200")
            detailsList.append({'cattleName': name, 'cattleCount': cattleRes[name]})
            #print(detailsList)
            cattleResponse.detailsList = detailsList
            #print(cattleResponse)
        cattleRes['item'] = item
        #print(cattleResponse)
        return dict(
            body  = str(cattleResponse)
        )
    except Exception as e:
        logger.error(e)
        logger.error("Error: Unexpected error: Error in getFarmerCattleDetails")
        raise ValueError({"errorMessage": {"status": "fail", "reason": "Unable to getFarmerCattleDetails"}})
    finally:
        connection.close()
        logger.info("***getFarmerCattleDetails End***")
        
