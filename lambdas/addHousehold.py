#!/usr/bin/python

import sys
import logging
import operator
import json
from functools import reduce
from lambdas import connectionManager
from models.masterDetailsModel import MasterDetails
from lambdas import config

logger = logging.getLogger()
logger.setLevel(logging.INFO)
db_name = config.db_name;

def addHousehold(event,context):
    logger.info("***addHousehold for %s", event['body'])


   
    try:
        
  
       name = (json.loads(event['body'])).get('household_name')
       identifier = (json.loads(event['body'])).get('household_identifier')
       state1 = (json.loads(event['body'])).get('state')
       taluka1 = ((json.loads(event['body'])).get('taluka'))
       village1 = (json.loads(event['body'])).get('village')
       district1 = (json.loads(event['body'])).get('district')
       address_line1 = (json.loads(event['body'])).get('address_line_1')
       address_line2 = (json.loads(event['body'])).get('address_line_2')
       connection = connectionManager.getConnection()
       cursor = connection.cursor()
       logger.info("SUCCESS: Connection to RDS mysql instance succeeded")
      
       Household_exists_query = 'SELECT household_name FROM '+db_name+'.wotr_household_master WHERE household_identifier=''" +str(identifier)"'
       cursor.execute(Household_exists_query)
       records = cursor.fetchall()
       print("records=",records)
       if not records:
           print("records=",records)
           insert_Householddetails_query = ("INSERT INTO wotr_household_master (household_name,household_identifier) VALUES ('{0}','{1}')".format(name,identifier))
           cursor.execute(insert_Householddetails_query)
           connection.commit()
           get_idwotr_household_master = ("SELECT idwotr_household_master FROM {0}.wotr_household_master WHERE household_identifier = '{1}'".format(db_name,identifier))
           cursor.execute(get_idwotr_household_master)
           records = cursor.fetchall()
           insert_householdaddress_query = ("INSERT INTO wotr_household_address (address_line1,address_line2,household_cd,taluka_cd) VALUES ('{0}','{1}',{2},'{3}')".format(address_line1,address_line2,records[0][0],taluka1))
           cursor.execute(insert_householdaddress_query)
           connection.commit()
           
           statuscode=200
           response="Household added"
           
       else:
           statuscode=409
           response="Household Identifier already exists"
           
          
       return dict(
            statusCode=statuscode
            )
            
    except Exception as e:
       logger.error(e)
       logger.error("ERROR: Unexpected error: Error in addHousehold")
       raise 
    finally:
        connection.close()
        logger.info ("***addHousehold End***")
