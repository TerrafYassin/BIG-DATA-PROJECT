from time import sleep
from kafka import KafkaProducer 
from kafka import KafkaConsumer
#from pymongo import MongoClient
from json import loads
import os
import sys
import optparse
import json 
import xmltodict 
from time import sleep
import xml.etree.ElementTree as ET
from kafka import KafkaProducer 

# we need to import some python modules from the $SUMO_HOME/tools directory
if 'SUMO_HOME' in os.environ:
    tools = os.path.join(os.environ['SUMO_HOME'], 'tools')
    sys.path.append(tools)
else:
    sys.exit("please declare environment variable 'SUMO_HOME'")


from sumolib import checkBinary  # Checks for the binary in environ vars
import traci


def get_options():
    opt_parser = optparse.OptionParser()
    opt_parser.add_option("--nogui", action="store_true",
                         default=False, help="run the commandline version of sumo")
    options, args = opt_parser.parse_args()
    return options


# contains TraCI control loop
def run():
    step = 0
    while step < 1000:
        traci.simulationStep()
        print(step)


        # if step == 100:
        #     traci.vehicle.changeTarget("1", "e9")
        #     traci.vehicle.changeTarget("3", "e9")

        step += 1

    traci.close()
    sys.stdout.flush()
    
def where_json(file_name):
    return os.path.exists(file_name)

# main entry point
if __name__ == "__main__":
    
    options = get_options()
  
        
    # Open the XML 
    #producer = KafkaProducer(value_serializer=lambda v: json.dumps(v).encode('utf-8'))
    producer = KafkaProducer(value_serializer=lambda v: json.dumps(v).encode('utf-8'),api_version=(0, 10, 1))
    root = ET.parse("C:/Users/HP/Desktop/SUMO_ACCIDENT/SUMO_ACCIDENT/sumoTrace.xml").getroot()
    
    # Iterate over all timesteps
    for timestep in root:
        # Get the time value of the timestep
        time = timestep.attrib
        
        # Iterate over all pedestrians in a timestep
        rows = []
        for vehicle in timestep:
            rows.append(dict(time, **vehicle.attrib))

        for ele in rows:
            if ele['type']=='car' or ele['type']=='taxi':
               ele['minGap']='500'
            else :
               ele['minGap']='0'
            print(ele)
            producer.send('TopicS', value=ele)
            sleep(1)

            
    

    # check binary
    if options.nogui:
        sumoBinary = checkBinary('sumo')
    else:
        sumoBinary = checkBinary('sumo-gui')

    # traci starts sumo as a subprocess and then this script connects and runs
    traci.start([sumoBinary, "-c", "C:/Users/HP/Desktop/SUMO_ACCIDENT/SUMO_ACCIDENT/Configuration.sumo.cfg"
                 ])
    run()
   
    # open the input xml file and read 
# data in form of python dictionary  
# using xmltodict module 

        
