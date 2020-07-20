#!/usr/bin/python

import sys
import logging
import operator
import json
from lambdas import config
from lambdas import connectionManager

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def getFarmerCropDetails(event, context):
     
     logger.info("***getFarmerCropDetails ***")
     userid = event['pathParameters']['item']
     
     
     try:
          connection = connectionManager.getConnection()
          cursor = connection.cursor()
          logger.info("SUCCESS: Connection to RDS mysql instance succeeded")
       
          getCropDetailsQuery = "SELECT farmer_crop.cropTypeId,farmer_crop.area,crops.cropName FROM wotrdev.wotr_user_detail,wotrdev.farmer_crop,wotrdev.crops where wotr_user_detail.user_id='"+userid+"' AND farmer_crop.cropId=crops.cropId AND wotr_user_detail.farmer_id=farmer_crop.farmer_id"
          cursor.execute(getCropDetailsQuery)
          Crop_record=cursor.fetchall()
          crop1=[]
          crop2=[]
          crop3=[]
          crop4=[]
          for x in range(len(Crop_record)):
               crop_detail={'cropId':'val','unit':'Hectares','area':'val1'}
               if (Crop_record[x][0] == 1):
                    crop_detail['cropId']=Crop_record[x][2]
                    crop_detail['area']=Crop_record[x][1]
                    crop1.append(crop_detail)
               elif (Crop_record[x][0] == 2):
                    crop_detail['cropId']=Crop_record[x][2]
                    crop_detail['area']=Crop_record[x][1]
                    crop2.append(crop_detail)
               elif (Crop_record[x][0] == 3):
                    crop_detail['cropId']=Crop_record[x][2]
                    crop_detail['area']=Crop_record[x][1]
                    crop3.append(crop_detail)
               else :
                    crop_detail['cropId']=Crop_record[x][2]
                    crop_detail['area']=Crop_record[x][1]
                    crop4.append(crop_detail)
              
          res = {"responseInfo": {"responseCode": "200", "reasons": [{"reasonCode": "0", "desc": "success"}]}, "cropDetails": [{"type": "Kharif","crops": crop1},{"type": "Rabi","crops": crop2},{"type": "Summer","crops": crop3},{"type": "Perennial","crops": crop4}]}
          print(json.dumps(res))
          return dict(
            body  = str(json.dumps(res))
          )
     except Exception as e:
          logger.error(e)
          logger.error("Error: Unexpected error: Error in getFarmerCropDetails")
          raise ValueError({"errorMessage": {"status": "fail", "reason": "Unable to updateCattleDetails"}})
     finally:
          connection.close()
          logger.info("***getFarmerCropDetails***")