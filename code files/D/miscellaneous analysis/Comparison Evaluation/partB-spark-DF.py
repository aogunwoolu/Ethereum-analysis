from pyspark.sql import SparkSession
from pyspark.sql.types import *
from operator import add
from time import time
import pyspark
import re

sc = pyspark.SparkContext()
spark = SparkSession(sc)

'''
filtering out lines that:
    - are not length 7 (after splitting with comma)
    - don't have a integer at the 4th position
    - address is null
'''
def transactions_is_good_line(line):
    try:
        fields = line.split(',')
        if len(fields)!=7: return False

        if str(fields[2]) == "null": return False

        if int(fields[3]) == 0: return False

        return True

    except:
        return False

'''
filtering out lines that:
    - are not length 5 (after splitting with comma)
'''
def contracts_is_good_line(line):
    fields = line.split(',')
    if len(fields)!=5:
        return False
    return True

'''
method for running part B in spark

loads transactions and contracts dataset with spark, filtering our malformed lines

changing each of them into dataframes (defining transactions schema so values are able
to be operated on as they are large numbers)

filter out any null values in transactions after dataframe conversion

mapping address exclusively with contracts
joining on address column in both dataframes and summing each by value based on 
address groups

converting back to an rdd from dataframe

taking the top 10 from that rdd
'''
lines = sc.textFile("/data/ethereum/transactions")

clean_lines = lines.filter(transactions_is_good_line)

counts = clean_lines.map(lambda l: (l.split(',')[2], float(l.split(',')[3])))

transaction_counts = counts.reduceByKey(lambda a, b: a + b)

schema = StructType([StructField("address", StringType(), False), StructField("value", DoubleType(), False)])

transaction_countsDF = transaction_counts.toDF(schema)

transaction_countsDF = transaction_countsDF.filter("value is not null")



lines = sc.textFile("/data/ethereum/contracts")

clean_lines = lines.filter(contracts_is_good_line)

contractsDF = clean_lines.map(lambda l: (l.split(',')[0],)).toDF(["address"])



contract_totals_indiv = transaction_countsDF.join(contractsDF, "address")

contract_totals_sum = contract_totals_indiv.groupBy("address").sum("value")

contract_totals_rdd = contract_totals_sum.rdd.map(lambda x: tuple(x))


top10 = contract_totals_rdd.takeOrdered(10, key = lambda x: -x[1])

count = 1
for record in top10:
    print("{count},{addr},{contactCount}".format(addr=record[0], contactCount=record[1], count=count))
    count+=1
