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

def updateFarmerCropDetails(event,context):
    
    logger.info ("***updateFarmerCropDetails for %s", event)

    

    db_name = config.db_name
    try:
       connection = connectionManager.getConnection()
       cursor = connection.cursor()
       logger.info("SUCCESS: Connection to RDS mysql instance succeeded")
       #farmerid = (json.loads(event['body'])).get('farmerid')
       userid = (json.loads(event['body'])).get('userid')
       crop_details = (json.loads(event['body'])).get('cropDetails')
       for detail in crop_details:
            print("detail:",detail)
            crop_type = detail['type']
            print("crop_type:",crop_type)
            for values in detail['crops']:
                print(values)
                crop_name = values['cropID']
                print("crop_name:",crop_name)
                crop_area = values['area']
                print("crop_area:",crop_area)
                crop_unit = values['unit']
                print("crop_unit:",crop_unit)
                get_id_query = ("SELECT wotr_user_detail.farmer_id,cropTypeSeason,cropTypeId,crops.cropId FROM {0}.wotr_user_detail,{0}.crop_types,{0}.crops WHERE user_id = '{3}' AND crop_types.cropTypeSeason = '{1}' AND crops.cropName = '{2}'".format(db_name,detail['type'],values['cropID'],userid))
                cursor.execute(get_id_query)
                records = cursor.fetchall()
                print(records)
                get_farmer_crop_id_query = ("SELECT farmer_crop.farmerCropId FROM {0}.farmer_crop WHERE cropTypeId = {1} AND cropId = {2} AND farmer_id = {3} ".format(db_name,records[0][2],records[0][3],records[0][0]))
                cursor.execute(get_farmer_crop_id_query)
                records1 = cursor.fetchall()
                update_crop_query = ("UPDATE {0}.farmer_crop SET farmer_crop.area = {1} WHERE farmerCropId = {2}".format(db_name,crop_area,records1[0][0]))
                cursor.execute(update_crop_query)
                connection.commit()
                       
       response = { 'responseInfo' : "Crop Details updated succesfully" }
       return dict(
       statusCode=200
       )
    except Exception as e:
       logger.error(e)
       logger.error("ERROR: Unexpected error: Error in updatecropdetail")
       raise 
    finally:
        connection.close()
        logger.info ("***getFarmerDetail End***")
        
        
        
        '''
     {
            "userid": "adi_1",
            "cropDetails": [
      {
        "type": "Kharif",
        "crops": [
          {
            "cropID": "Cotton",
            "unit": "Hectares",
            "area": "1000"
          },
          {
            "cropID": "Wool",
            "unit": "Hectares",
            "area": "1000"
          }
        ]
      },
      {
  "type": "Rabi",
  "crops": [
    {
      "cropID": "Almonds",
      "unit": "Hectares",
      "area": "1000"
    },
    {
      "cropID": "Ground Nut",
      "unit": "Hectares",
      "area": "1000"
    }
  ]
}
    ]
}
        
        '''