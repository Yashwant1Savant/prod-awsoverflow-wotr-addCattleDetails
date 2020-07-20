#!/usr/bin/python

import json

class ResponseInfo:

    def __init__(self, responseCode):
        self.responseCode = responseCode
        self.reasons = []

    def __str__(self, key, payload):
        return json.dumps({"responseInfo" : {"responseCode": self.responseCode, "reasons": self.reasons}, key: payload})
        
    def addReason(self, reason, desc):
        self.responseCode = responseCode
        self.reasons.append({"reasonCode": reason, "desc": desc})