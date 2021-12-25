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
    '''
    checking there are 7 columns (verifying the passed file is transactions)
    if it is transactions: get the last item (the block timestamp)
    calculate the timestamp from this timestamp - only yielding month and year
    yield this timestamp as key and 1 as value (to count how many transations occured that month)

    transactions:
    block_number, from_address, to_address, value, gas, gas_price, block_timestamp
    '''

    def mapper(self,_,line):
        try:
            fields = line.split(",")
            if (len(fields) == 7):
                value = int(fields[-4])
                recipient = fields[2]
                if value != 0 and recipient != "null":
                    yield(recipient, value)
                pass
        except:
            pass

    '''
    taking values and adding them preemptively producing a key value pair of the
    address and the total
    '''

    def combiner(self,toAddr, value):
        yield(toAddr, sum(value))

    # 1B.
    '''
    reducer recieves all subtotals associated with the address and sums all
    subtotals
    '''

    def reducer(self, toAddr, count):
        yield(toAddr, sum(count))

if __name__ == '__main__':
    coursework.run()
