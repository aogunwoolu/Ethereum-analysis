import pyspark
import time
import re

#starting the spark context for future operations
sc = pyspark.SparkContext()

'''
filtering out lines that:
    - are not length 7 (after splitting with comma)
    - don't have a integer at the 4th position
    - address is null
'''
def is_good_line(line):
    try:
        fields = line.split(',')
        if len(fields)!=7:
            return False

        if (int(fields[3]) == 0) or (fields[2] == "null"):
            return False

        return True

    except:
        return False


'''
calculates average by summing total and counts in the reduce by key
'''
def reducer(a,b):
    total = a[0] + b[0]
    counter = a[1] + b[1]

    return (total, counter)


'''
imports transactions.csv from hadoop cluster
'''
lines = sc.textFile("/data/ethereum/transactions")

'''
calls the filtering method, collecting all lines that return true on the calls
'''
clean_lines = lines.filter(is_good_line)

'''
transactions line structure:
  0                 1           2         3     4     5             6        
block_number, from_address, to_address, value, gas, gas_price, block_timestamp

mapper selecting block timestamp by month and value from each good line (and a 1 for count)
'''
countsPerDay = clean_lines.map(lambda l: (time.strftime("%m/%Y",time.gmtime((int(l.split(',')[-1])))), (int(l.split(',')[5]),1)))#,l.split(',')[2],float(l.split(',')[6])*float(l.split(',')[7])

'''
calls average reducer method to reduce
'''
counts = countsPerDay.reduceByKey(reducer)

'''
collect result from RDD
'''
result = counts.collect()

'''
outputs the top result in a csv format
'''
for record in result:
    print("{day},{avg}".format(day=record[0], avg=record[1][0]/record[1][1]))
