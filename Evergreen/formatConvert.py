import json
import string
import glob
import sys
import os

def main(filename, cwd):
    path=""
    for x in cwd.split("\\"):
        path += x + "\\\\" #just to add escape sequences for the glob method to work fine
    with open(path + "ContainerInformation\\" + filename) as json_file:
        data = json.load(json_file)
    newJson = {}
    newJson["Source"] = data.get("Source")
    newJson["SCAC"] = data.get("SCAC")
    newJson["Vessel Name"] = data.get("Vessel Name")
    newJson["Voyage Number"] = data.get("Voyage Number")
    newJson["Vessel Stops"] = {}
    index = 0
    stop = {}
    for key, value in data.items():
        if(index < 4): #get past base data
            pass
        elif(index % 3 == 1): #build stop object
            stop[key] = {}
        elif(index % 3 == 2): #arrival date
            stop[key.replace("Estimated Arrival ","")]["Estimated Arrival"] = value.replace("*","")
        elif(index % 3 == 0): # departure date
            stop[key.replace("Estimated Departure ","")]["Estimated Departure"] = value.replace("*","")
        index += 1
    newJson["Vessel Stops"] = stop
    with open(path + "ContainerInformation\\" + filename, 'w') as outfile:  
        json.dump(newJson, outfile)
    


if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2])