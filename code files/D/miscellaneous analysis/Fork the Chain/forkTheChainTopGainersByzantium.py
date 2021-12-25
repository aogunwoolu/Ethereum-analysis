import pyspark
import time
import re

sc = pyspark.SparkContext()

'''
range checker method for verifying that dates are between the specified date

Byzantium fork is in this example for getting top earners
'''
def is_time_between(check_time=None):
    # Byzantium fork occured at Oct-16-2017 05:22:11 AM +UTC, checking 3 months ahead
    # to make sure gains are primarily from the fork
    begin_time = time.strptime("16 Oct 2017", "%d %b %Y")
    end_time = time.strptime("16 Jan 2018", "%d %b %Y")

    return check_time >= begin_time and check_time <= end_time


'''
filtering out lines that:
    - are not length 7 (after splitting with comma)
    - don't have a integer at the 4th position
    - address is null
    - the dates are between the specified dates
'''
def is_good_line(line):
    try:
        fields = line.split(',')
        if len(fields)!=7:
            return False

        if (int(fields[3]) == 0) or (fields[2] == "null") or not fields[2]:
            return False

        date = time.gmtime((int(fields[-1])))

        if not is_time_between(date): return False

        return True

    except:
        return False

'''
map the address through time by day with value for that transaction
'''
def mapper(line):
    lines = line.split(',')
    date = time.gmtime((int(lines[-1])))
    dateStr = time.strftime("%d/%m/%Y",date)
    toAddr = lines[2]
    amount = lines[3]

    return (("{address},{date}".format(date=dateStr,address=toAddr),int(amount)))


'''
imports transactions.csv from hadoop cluster
'''
lines = sc.textFile("/data/ethereum/transactions")

'''
calls the filtering method, collecting all lines that return true on the calls
'''
clean_lines = lines.filter(is_good_line)

'''
calls time by day mapper to map
'''
countsPerDay = clean_lines.map(mapper)

'''
reduces the size by the key (which is the block timestamp per day), producing an aggregate transaction value
for each day
'''
counts = countsPerDay.reduceByKey(lambda a, b: a + b)

'''
recieving every single result in the counts RDD, we can do a takeOrdered
action and specify the argument 10 to get the top 10
'''
result = counts.takeOrdered(10, key = lambda x: -x[1])

'''
outputs the top 10 result in a csv format (top 10 earners between the dates)
'''
for record in result:
    print("{address},{count}".format(address=record[0], count=record[1]))
