import json
import string
import glob
import sys
import os
import datetime
import copy
import kafka

def write(filename):
    with open(filename) as json_file:
        data = json.load(json_file)
    newJson = {}
    vesselInfo = {}
    vesselInfo["source"] = data.get("Source")
    vesselInfo["sCAC"] = data.get("SCAC")
    vesselInfo["vesselCode"] = None
    vesselInfo["vesselName"] = data.get("Vessel Name")
    vesselInfo["voyageNumber"] = data.get("Voyage Number")
    newJson["vesselInfo"] = vesselInfo
    index = 0
    vesselStops = []
    stop = {}
    stopNumber = 1
    for key, value in data.items():
        if(index < 4): #get past base data
            pass
        elif(index % 3 == 1): #build stop object
            stop = {}
            stop["portName"] = key
            stop["unlocCode"] = None
        elif(index % 3 == 2): #arrival date
            stop["estimatedArrival"] = value.replace("*","") + "/" + str(datetime.datetime.now().year) #assume current year
        elif(index % 3 == 0): #departure date
            stop["estimatedDeparture"] = value.replace("*","") + "/" + str(datetime.datetime.now().year) #assume current year
            stop["sequenceNumber"] = stopNumber
            stopNumber += 1
            vesselStops.append(stop)
        index += 1
    newJson["vesselStops"] = vesselStops
    with open(filename, 'w') as outfile:  
        json.dump(newJson, outfile)
    producer = kafka.KafkaProducer(bootstrap_servers=['10.138.0.2:9092'],
                                    value_serializer=lambda x: json.dumps(x).encode('utf-8'),
                                    linger_ms = 10)
    producer.send('RPA_SAILINGSCHEDULE_QA', value=newJson)
    producer.flush()

def testMain(vessel):
    write("ContainerInformation\\"+vessel+".json")

def main(cwd):
    path=""
    for x in cwd.split("\\"):
        path += x + "\\\\" #just to add escape sequences for the glob method to work fine
    fileList = glob.glob(r""+path+"ContainerInformation\\*.json", recursive = True)
    fileList.sort(key=os.path.getmtime)
    for file_name in fileList:
        write(file_name)

if __name__ == "__main__":
    #testMain(sys.argv[1])
    main(sys.argv[1])