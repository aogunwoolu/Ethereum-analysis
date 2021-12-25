import json

'''
json to csv converter, modified to return the status of scams (offline/active)
'''
with open('scams.json') as json_file:
    data = json.load(json_file)
    key=[]
    for p in data["result"]:
        key.append(p)
    for k in key:
        # added to file the "status" line to get offline dates
        addr=data["result"][k]["addresses"]
        print(data["result"][k]["id"], end=',')
        print(data["result"][k]["status"], end='')
        for a in addr:
            print (",",a,end='')
        print("")

#hadoop fs -get /data/ethereum/scams.json
#python jsonToCSV.py > scams.csv
