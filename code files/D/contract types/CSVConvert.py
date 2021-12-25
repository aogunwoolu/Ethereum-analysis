'''
conversion of mapreduce output to CSV file
'''
with open('features.txt') as features:
    for lines in features.readlines():
        linearr = lines.split("\t")
        addr = linearr[0].replace("\"","")
        featuresarr = linearr[1].replace("[","").replace("]","").replace(" ","").replace("\"","").split(",")
        print(addr,end=",")
        print(featuresarr[0],end=",")
        print(featuresarr[1],end=",")
        print(featuresarr[2],end=",")
        print(featuresarr[3],end="")