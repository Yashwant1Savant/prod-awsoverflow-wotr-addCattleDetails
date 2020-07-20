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
db_name=config.db_name


def addCattleDetails(event,context):
    logger.info("***addCattleDetails for %s", event)

    
    try:
        connection = connectionManager.getConnection()
        cursor = connection.cursor()
        logger.info("SUCCESS: Connection to RDS mysql instance succeeded")
        user_id = (json.loads(event['body'])).get('userId')
        #user_id = "Trump3"
        # print(userid)
        cattleDetails = (json.loads(event['body'])).get('cattleDetails')
        #cattleDetails = event['body']['cattleDetails']
        # print(landArea)
        for details in cattleDetails:
            cattle_name = details['id']
            cattle_count = details['count']
            isCattleShared = details['isShared']
            cattle_details = ("Select wotr_user_detail.farmer_id, wotr_cattle_master.cattle_cd From {0}.wotr_farmer, {0}.wotr_cattle_master, {0}.wotr_user_detail Where wotr_cattle_master.cattle_description = '{1}' AND wotr_user_detail.user_id = '{2}' And wotr_farmer.farmer_id = wotr_user_detail.farmer_id".format(db_name,cattle_name,user_id))
            print(cattle_details)
            cursor.execute(cattle_details)
            records_cattle_details = cursor.fetchall()
            print(cattle_name)
            print(cattle_count)
            print(isCattleShared)
            print(records_cattle_details)
            # farmer_village_details = "Select distinct(wotr_user_detail.farmer_id), wotr_household_address.village_cd From {0}.wotr_farmer, {0}.wotr_farmer_cattle,  {0}.wotr_user_detail,  {0}.wotr_household_address,  {0}.wotr_household_master Where wotr_user_detail.user_id = '{1}' And wotr_farmer.farmer_id = wotr_user_detail.farmer_id AND wotr_farmer_cattle.farmer_id = wotr_farmer.farmer_id  AND wotr_farmer.household_id = wotr_household_master.household_identifier AND wotr_household_master.idwotr_household_master =  wotr_household_address.household_cd".format(db_name,user_id)
            farmer_village_details = "SELECT wotr_household_address.village_cd FROM {0}.wotr_user_detail, {0}.wotr_farmer, {0}.wotr_household_master, {0}.wotr_household_address where wotr_user_detail.user_id = '{1}' and wotr_user_detail.farmer_id = wotr_farmer.farmer_id and  wotr_farmer.household_id = wotr_household_master.household_identifier and wotr_household_address.household_cd = wotr_household_master.idwotr_household_master".format(db_name, user_id)
            print("farmer village details query", farmer_village_details)
            cursor.execute(farmer_village_details)
            records_farmer_village_details = cursor.fetchall()
            print('records_farmer_village_details ', records_farmer_village_details)
            
            farmer_details = ("Select wotr_user_detail.farmer_id, wotr_household_address.village_cd From {0}.wotr_farmer, {0}.wotr_farmer_cattle,  {0}.wotr_user_detail,  {0}.wotr_household_address,  {0}.wotr_household_master Where wotr_user_detail.user_id = '{1}' And wotr_farmer.farmer_id = wotr_user_detail.farmer_id AND wotr_farmer_cattle.farmer_id = wotr_farmer.farmer_id  AND wotr_farmer.household_id = wotr_household_master.household_identifier AND wotr_household_master.idwotr_household_master =  wotr_household_address.household_cd AND wotr_farmer_cattle.cattle_cd = {2}".format(db_name,user_id, records_cattle_details[0][1]))
            cursor.execute(farmer_details)
            records_farmer_id = cursor.fetchall()
            if len(records_farmer_id) > 0:
                continue
    #             response= {"responseInfo":  {"responseCode": "409", "reasons": [{"reasonCode": "0", "desc": "success"}]}}
    #             return {
    #         'statusCode': 200,
    #          'headers': {
    # "Access-Control-Allow-Origin" : "*", 
    # "Access-Control-Allow-Credentials" : True 
    #           },
    #         'body' :  json.dumps(response)
            # }
            print("@@@@@@@@@@@@@@")
            print(records_farmer_id)
            insert_cattle_details = ("Insert into {0}.wotr_farmer_cattle (farmer_id, cattle_cd, quantity, isshared, village_Id) values ({1}, {2} , {3}, '{4}', {5})".format(db_name,records_cattle_details[0][0],records_cattle_details[0][1],cattle_count, isCattleShared, records_farmer_village_details[0][0]))
            print(insert_cattle_details)
            cursor.execute(insert_cattle_details)
            connection.commit()
        statuscode=200
        response= {"responseInfo":  {"responseCode": "200", "reasons": [{"reasonCode": "0", "desc": "success"}]}}
        return {
            'statusCode': 200,
             'headers': {
    "Access-Control-Allow-Origin" : "*", 
    "Access-Control-Allow-Credentials" : True 
              },
            'body' : str(response)
        }
        # return dict(
        #     statusCode = statuscode,
        #     body = str(response)
        #     )
    except Exception as e:
        logger.error(e)
        logger.error("Error: Unexpected error: Error in addCattleDetails")
        raise
    finally:
        connection.close()
        logger.info ("***addCattleDetails End***")
    
'''
{
    "userId": "TestUser",
    "cattleDetails": [
      {
        "id": "Desi Bull",
        "count": 10,
        "isShared": "Y"
      },
      {
        "id": "Poultry Birds",
        "count": 20,
        "isShared": "N"
      }
    ]
  }
'''
