"""
coursework
"""
from mrjob.job import MRJob, MRStep
import re
import time

#this is a regular expression that finds all the words inside a String
WORD_REGEX = re.compile(r"\b\w+\b")

#This line declares the class Lab1, that extends the MRJob format.
class coursework(MRJob):
    #1B.
    '''
    checking there are 7 columns (verifying the passed file is transactions)
    if it is transactions: get the last item (the block timestamp)
    calculate the timestamp from this timestamp - only yielding month and year
    yield this timestamp as key and 1 as value (to count how many transations occured that month)

    block_number, from_address, to_address, value, gas, gas_price, block_timestamp
    '''

    def mapper(self,_,line):
        try:
            fields = line.split(",")
            if (len(fields) == 7):
                timestamp = time.strftime("%m/%Y",time.gmtime((int(fields[-1]))))
                value = int(fields[3])
                yield(timestamp, value)
        except:
            pass

    #1B.
    '''
    taking values and adding them preemptively producing a key value pair of the
    timestamp and the total with the count in a tuple
    '''

    def combiner(self, val, count):
        c = 0
        subtot = 0
        for tl in count:
            subtot += tl
            c += 1
        yield(val, (subtot,c))

    # 1B.
    '''
    reducer recieves all subtotal and count pairs, cycling through the
    generator 'count' and appending those values to the final

    finally yielding the date with the average (total sum / count)
    '''

    def reducer(self, val, count):
        s = 0
        tc = 0
        for c in count:
            s += c[0]
            tc += c[1]
        yield(val, s/tc)

if __name__ == '__main__':
    coursework.run()
