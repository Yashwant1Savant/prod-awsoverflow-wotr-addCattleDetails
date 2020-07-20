#!/usr/bin/python


import sys
import logging
#import config
from lambdas import config
from lambdas import connectionManager
import json
import time
import calendar
import datetime

logger = logging.getLogger()
logger.setLevel(logging.INFO)



def getFarmerDetails(event, context):

    logger.info ("***getFarmerDetail  Begin***")

    #if('userId' not in item):
     #   raise ValueError ({"errorMessage": {"status" : "fail", "reason" : "001 - Invalid request"}})
    
    userid = event['pathParameters']['item']
    print(userid)
    #logger.info("Userid:",event['body']['userId'])
    #logger.info (event['body']['userId'])
    farmer_id=""
    farmer_name=""
    farmer_village=""
    farmer_state=""
    farmer_land_area=""
    farmer_cattle_quantity=""
    try:
    
        connection = connectionManager.getConnection()
        cursor = connection.cursor()
        logger.info("SUCCESS: Connection to RDS mysql instance succeeded")
       
        #responseInfo = {"responseCode": "000", "reasons": {"reasonCode": "", "desc": "success"}}
       
    #query to get farmer basic details
        #get_farmer_query = "SELECT wotr_farmer.farmer_id, wotr_farmer.First_name, wotr_farmer.Middle_name, wotr_farmer.Last_name ,wotr_village_details.Village_Full_name,wotr_taluka.taluka_name, wotr_district.district_name, wotr_state.state_name FROM wotrdev.wotr_farmer,wotrdev.wotr_farmer_address,wotrdev.wotr_village_details,wotrdev.wotr_taluka, wotrdev.wotr_district, wotrdev.wotr_state WHERE wotr_farmer_address.farmer_id=wotr_farmer.farmer_id AND wotr_farmer.farmer_id="+userid+" AND wotr_farmer_address.taluka_cd = wotr_taluka.taluka_cd AND wotr_village_details.taluka_id=wotr_taluka.taluka_cd AND wotr_taluka.distirict_cd = wotr_district.district_cd AND wotr_district.state_cd = wotr_state.state_cd"
        get_farmer_query = "SELECT wotr_farmer.farmer_id, wotr_farmer.First_name, wotr_farmer.Middle_name, wotr_farmer.Last_name ,wotr_village_details.Village_Full_name,wotr_taluka.taluka_name, wotr_district.district_name, wotr_state.state_name FROM wotrdev.wotr_farmer,wotrdev.wotr_farmer_address,wotrdev.wotr_village_details,wotrdev.wotr_taluka, wotrdev.wotr_district, wotrdev.wotr_state,wotrdev.wotr_user_detail WHERE wotr_farmer_address.farmer_id=wotr_farmer.farmer_id AND wotr_user_detail.user_id='"+str(userid)+"' AND wotr_farmer_address.taluka_cd = wotr_taluka.taluka_cd AND wotr_village_details.taluka_id=wotr_taluka.taluka_cd AND wotr_taluka.distirict_cd = wotr_district.district_cd AND wotr_district.state_cd = wotr_state.state_cd AND wotr_farmer.farmer_id=wotr_user_detail.farmer_id"
        cursor.execute(get_farmer_query)
        farmer_records = cursor.fetchall()
        #records=list(cursor)
        print(farmer_records)
        print("LIST Cursor: ", farmer_records)
        for row in farmer_records:
            farmer_id=row[0]
            farmer_name=row[1]+" "+row[2]+" "+row[3]
            farmer_village=row[4]
            farmer_taluka=row[5]
            farmer_district=row[6]
            farmer_state=row[7]
    
    #query to get land area of farmer
        get_landArea_query="select sum(area) from wotrdev.farmer_crop where farmer_id="+str(farmer_id)
        cursor.execute(get_landArea_query)
        landArea_record=cursor.fetchall()
        
        for row in landArea_record:
            farmer_land_area=row[0]
            farmer_land_area=int(farmer_land_area)
        
        #print(farmer_land_area)
            
    #query to get cattle quantity of farmer
        get_farmer_cattle_quantity="select sum(quantity) from wotrdev.wotr_farmer_cattle where farmer_id="+str(farmer_id)
        cursor.execute(get_farmer_cattle_quantity)
        cattleQuantity_record=cursor.fetchall()
        
        for row in cattleQuantity_record:
            farmer_cattle_quantity=row[0]
            farmer_cattle_quantity=int(farmer_cattle_quantity)
        
        print(farmer_cattle_quantity)
        
    
    #query to get season crop area
        get_farmer_crop_seasonal_area="select cropTypeId,area from wotrdev.farmer_crop where farmer_id="+str(farmer_id)
        #print(get_farmer_crop_seasonal_area)
        cursor.execute(get_farmer_crop_seasonal_area)
        crop_seasonal_area_record=cursor.fetchall()
    
        print(crop_seasonal_area_record)
        crop_1=0
        crop_2=0
        crop_3=0
        crop_4=0
        for key, val in crop_seasonal_area_record:
            #print(val, key)
            #print(type(key))
            #print(type(val))
            if (key == 1):
                #print("inside if")
                crop_1+=val;
                
            if (key == 2):
                #print("inside 2")
                crop_2+=val;
            
                    
            if (key == 3):
                #print("inside 3")
                crop_3+=val;
        
                    
            if (key == 4):
                #print("inside 4")
                crop_4+=val;
            
        print("Crop 1:",crop_1)
        print("Crop 2:",crop_2)
        print("Crop 3:",crop_3)
        print("Crop 4:",crop_4)
        
        
    #create JSON response and return it
        res = {"responseInfo":  {"responseCode": "200", "reasons": [{"reasonCode": "0", "desc": "success"}]}, "farmerDetails": [{"farmerName": farmer_name, "village": farmer_village,"taluka": farmer_taluka,"district": farmer_district,"state": farmer_state, "waterBudget": {"availableWater": "2000","Unit": "Litres", "waterAvailability": "DEFICIT"}, "waterStructureCount": "2", "landArea": farmer_land_area,"Unit": "Hectres" ,"cattleCount": farmer_cattle_quantity, "rainShadowregion": "300", "cropSeasonalAreaInHectares": [{"season": "kharif", "area": crop_1}, {"season": "rabi", "area": crop_2}, {"season": "summer", "area": crop_3}, {"season": "perennial", "area": crop_4}]}]}
        #print(res)
        #return res
        return dict(
            body  = json.dumps(res)
        )
    
    except Exception as e:
       logger.error(e)
       logger.error("ERROR: Unexpected error: Error in getFarmerDetail")
       raise 
    finally:
        connection.close()
        logger.info ("***getFarmerDetail End***")