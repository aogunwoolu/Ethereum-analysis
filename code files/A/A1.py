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
    #1A.
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
                timestamp = time.strftime("%m/%Y",time.gmtime((int(fields[-1])/1000)))
                yield(timestamp, 1)
        except Exception as e:
            pass

    # 1A.
    '''
    reducer recieves all individual transactions occuring on the specified key day
    '''
    def reducer(self, val, count):
        yield(val, sum(count))

if __name__ == '__main__':
    coursework.run()
