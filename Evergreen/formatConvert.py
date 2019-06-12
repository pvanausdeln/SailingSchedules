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
    newJson["Source"] = data.get("Source")
    newJson["SCAC"] = data.get("SCAC")
    newJson["Vessel Name"] = data.get("Vessel Name")
    newJson["Voyage Number"] = data.get("Voyage Number")
    index = 0
    stop = {}
    for key, value in data.items():
        if(index < 4): #get past base data
            pass
        elif(index % 3 == 1): #build stop object
            stop[key] = {}
        elif(index % 3 == 2): #arrival date
            stop[key.replace("Estimated Arrival ","")]["Estimated Arrival"] = value.replace("*","") + "/" + str(datetime.datetime.now().year) #assume current year
        elif(index % 3 == 0): #departure date
            stop[key.replace("Estimated Departure ","")]["Estimated Departure"] = value.replace("*","") + "/" + str(datetime.datetime.now().year) #assume current year
        index += 1
    newJson["Vessel Stops"] = stop
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