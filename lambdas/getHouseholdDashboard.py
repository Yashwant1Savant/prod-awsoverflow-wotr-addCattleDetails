#!/usr/bin/python


import sys
import logging
import operator
import json
from functools import reduce
from lambdas import connectionManager
from lambdas import config

logger = logging.getLogger()
logger.setLevel(logging.INFO)

db_name = config.db_name
member_list = []

def getHouseholdDashboard(event,context):

    
    logger.info ("***getHouseholdDashboard for %s", event)
    try:
       connection = connectionManager.getConnection()
       cursor = connection.cursor()
       logger.info("SUCCESS: Connection to RDS mysql instance succeeded")
       
       household_id = event["pathParameters"]["item"]
       get_household_name = ("SELECT household_name,idwotr_household_master,village_cd FROM {0}.wotr_household_master,{0}.wotr_household_address WHERE household_identifier = '{1}' AND wotr_household_address.household_cd = wotr_household_master.idwotr_household_master".format(db_name,household_id))
       cursor.execute(get_household_name)
       records = cursor.fetchall()

       get_address_details = ("SELECT wotr_village_details.Village_Full_name, wotr_taluka.taluka_name,wotr_district.district_name, wotr_state.state_name from {0}.wotr_taluka, {0}.wotr_district, {0}.wotr_state, {0}.wotr_village_details where wotr_village_details.Village_id = {1} AND wotr_village_details.taluka_id = wotr_taluka.taluka_cd AND wotr_taluka.distirict_cd = wotr_district.district_cd AND wotr_district.state_cd = wotr_state.state_cd".format(db_name,records[0][2]))
       cursor.execute(get_address_details)
       records1 = cursor.fetchall()

       get_household_member = ("SELECT First_name,Middle_name,Last_name,Gender FROM {0}.wotr_farmer WHERE household_id = {1}".format(db_name,records[0][1]))
       cursor.execute(get_household_member) 
       records_member = cursor.fetchall()
       
       for details in range(len(records_member)):
           member_details = {"name": "test","Gender":"F"}
           member_details['name'] = records_member[details][0]
           member_details['Gender'] = records_member[details][3]
           print(member_details)
         
           member_list.append(member_details)
       
       
       
       response = {"household_name": records[0][0], "household_identifier": str(household_id), "village": records1[0][0],"taluka": records1[0][1], "district": records1[0][2], "state": records1[0][3], "houshold_member_count": len(records_member), "members": member_list}
       return dict(
            body = json.dumps(response)
           )
       
    except Exception as e:
       logger.error(e)
       logger.error("ERROR: Unexpected error: Error in getHouseholdDashboard")
       raise 
    finally:
        connection.close()
        logger.info ("***getHouseholdDashboard End***")



         