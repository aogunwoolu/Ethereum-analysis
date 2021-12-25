import pyspark
import time
import re

sc = pyspark.SparkContext()

#we will use this function later in our filter transformation
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

def C_is_good_line(line):
    try:
        fields = line.split(',')
        if len(fields)!=5:
            return False

        return True

    except:
        return False

def reducer(a,b):
    total = a[0] + b[0]
    counter = a[1] + b[1]

    return (total, counter)

# transactions = sc.textFile('/data/ethereum/transactions').filter(is_good_line) # maybe add the qmul anderoeda thingy
# contracts = sc.textFile('/data/ethereum/contracts').filter(is_good_line) # here too in the textfile name


# addr_value= transactions.map(lambda l: (l.split(',')[2], int(l.split(',')[3])))
# aggregrate_values = addr_value.reduceByKey(lambda a,b: a+b)
# # aggregrate_add_value = addr_value.aggregateByKey(zero_val, seq_op, comb_op) # aggregate
# step3 = aggregrate_values.join(contracts.map(lambda x: (x.split(',')[0], 'contract'))) # joinn
# top10 = step3.takeOrdered(10, key = lambda x: -x[1][0]) # top 10

lines = sc.textFile("/data/ethereum/transactions")
Clines = sc.textFile("/data/ethereum/contracts")

clean_lines = lines.filter(is_good_line)
C_clean_lines = Clines.filter(C_is_good_line)

addr_value= clean_lines.map(lambda l: (l.split(',')[2], (l.split(',')[-1], int(l.split(',')[4]))))
aggregrate_values = addr_value.reduceByKey(lambda a,b: a + b)


contractsWithTransactions = aggregrate_values.join(C_clean_lines.map(lambda x: (x.split(',')[0]))) 
top10 = contractsWithTransactions.take(10)
#top10 = contract_aggregrate_values.takeOrdered(10, key = lambda x: -x[1]) # top 10

for record in top10:
    for i in range(0,len(record[1]),2):
        date = time.gmtime((int(record[1][i])))
        dateStr = time.strftime("%d/%m/%Y",date)
        print("{},{},{}".format(record[0], dateStr, record[1][i+1]))