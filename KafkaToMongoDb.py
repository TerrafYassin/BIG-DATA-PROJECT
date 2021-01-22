#!/usr/bin/env python
# coding: utf-8

# In[1]:


from kafka import KafkaConsumer
from pymongo import MongoClient
from json import loads
import os
import sys
import optparse
import json 
import xmltodict 
from time import sleep
from kafka import KafkaProducer 


# In[16]:


from kafka import KafkaProducer 
producer = KafkaProducer(value_serializer=lambda v: json.dumps(v).encode('utf-8'))


# In[10]:


import xml.etree.ElementTree as ET


# In[17]:


# Open the XML file
#root = ET.parse("C:/Users/YASSIN TERRAF/Desktop/SUMO tuto/BERRECHID_MAP/BerrechidTrace.xml").getroot()

# Iterate over all timesteps
#for timestep in root:
    # Get the time value of the timestep
#    time = timestep.attrib
    # Iterate over all pedestrians in a timestep
 #   rows = []
  #  for vehicle in timestep:
 #       rows.append(dict(time, **vehicle.attrib))
  #      print(vehicle.attrib)
  #      producer.send('earliest', value=vehicle.attrib)


# In[18]:


consumer = KafkaConsumer("mytopic",
                     bootstrap_servers=['localhost:9092'],
                     group_id=None,
                     auto_offset_reset='earliest')


# In[19]:


client = MongoClient('localhost:27017')
collection = client.myalldata.vehicule


# In[20]:

for message in consumer:
    message = message.value
    jsonCnv= json.loads(message)
    collection.insert_one(jsonCnv)

    

