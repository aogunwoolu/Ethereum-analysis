import pyspark
import re

#starting the spark context for future operations
sc = pyspark.SparkContext()

'''
filtering out lines that:
    - are not length 9 (after splitting with comma)
    - don't have a integer at the 5th position
'''
def is_good_line(line):
    try:
        fields = line.split(',')
        if len(fields)!=9:
            return False

        int(fields[4])

        return True

    except:
        return False

'''
imports blocks.csv from hadoop cluster
'''
lines = sc.textFile("/data/ethereum/blocks")

'''
calls the filtering method, collecting all lines that return true on the calls
'''
clean_lines = lines.filter(is_good_line)

'''
blocks line structure:
   0    1    2        3       4       5        6        7          8
number,hash,miner,difficulty,size,gas_limit,gas_used,timestamp,transaction_count

mapper selecting miner address and size from each good line
'''
counts = clean_lines.map(lambda l: (l.split(',')[2], int(l.split(',')[4])))

'''
reduces the size by the key (which is the miner address), producing an aggregate size
for each miner address
'''
counts = counts.reduceByKey(lambda a, b: a + b)

'''
recieving every single result in the counts RDD, we can do a takeOrdered
action and specify the argument 10 to get the top 10
'''
top10 = counts.takeOrdered(10, key = lambda x: -x[1])

'''
outputs the top 10 result in a csv format
'''
count = 1
for record in top10:
    print("{count}. {miner}: {blockCount}".format(miner=record[0], blockCount=record[1], count=count))
    count+=1
