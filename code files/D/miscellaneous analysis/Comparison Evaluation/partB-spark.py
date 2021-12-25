from pyspark.sql import SparkSession
from pyspark.sql.types import *
from operator import add
from time import time
import pyspark
import re

#starting the spark context for future operations
sc = pyspark.SparkContext()

'''
filtering out lines that:
    - are not length 7 (after splitting with comma)
    - are not length 5 (after splitting with comma)
    - don't have a integer at the 4th position
'''
def is_good_line(line):
    try:
        fields = line.split(',')
        if len(fields) == 7: # transactions
            str(fields[2]) # to_addr
            if int(fields[3]) == 0:
                return False
        elif len(fields) == 5: # contracts
            str(fields[0]) # contract addr
        else:
            return False
        return True
    except:
        return False

'''
get lines from transactions and contracts
'''
transactions = sc.textFile('/data/ethereum/transactions').filter(is_good_line) 
contracts = sc.textFile('/data/ethereum/contracts').filter(is_good_line) 


'''
map transactions to address and value
'''
addr_value= transactions.map(lambda l: (l.split(',')[2], int(l.split(',')[3])))

'''
aggregate transaction values by key
'''
aggregrate_values = addr_value.reduceByKey(lambda a,b: a+b)

'''
join on the map of contract addresses
'''
step3 = aggregrate_values.join(contracts.map(lambda x: (x.split(',')[0], 'contract'))) # joinn

'''
take top 10 of join
'''
top10 = step3.takeOrdered(10, key = lambda x: -x[1][0])

for record in top10:
    print("{},{}".format(record[0], int(record[1][0])))

