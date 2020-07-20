#!/usr/bin/python

from models.responseInfoModel import ResponseInfo

class MasterDetails(ResponseInfo):

    def __init__(self, responseCode):
        self.responseCode = responseCode
        self.crops = []
        self.villages = []
        self.cattles = []
        self.waterStructures = []
        super().__init__(responseCode)

    def __str__(self):
        return super().__str__("masterDetails", {"crops": self.crops, "villages": self.villages,"cattles": self.cattles, "waterStructures": self.waterStructures})
        
    def setCrops(self, crops):
        self.crops = crops
        
    def setVillages(self, villages):
        self.villages = villages
        
    def setCattles(self, cattles):
        self.cattles = cattles
        
    def setWaterStructures(self, waterStructures):
        self.waterStructures = waterStructures        