#!/usr/bin/python

import sys
import logging
from lambdas import config
import libraries.mysql.connector as connector
import json
import time
import calendar
import datetime

rds_host  = config.db_endpoint
name = config.db_username
password = config.db_password
port = config.db_port
db_name = config.db_name

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def getConnection():
    conn = connector.connect(host=rds_host, user=name, passwd=password, db=db_name, connect_timeout=5,use_unicode=True, charset="utf8mb4")
    logger.info("Connection Successful!!")
    return conn