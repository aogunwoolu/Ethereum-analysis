"""Lab 1. Basic wordcount
"""
from mrjob.job import MRJob, MRStep
import re
import time
from datetime import datetime

class partD(MRJob):

    '''
    checking if the valuess are within +-5% of each other which is part of the 
    wash trading criteria we defined
    '''
    def rangeChecker(self, toCheck, checkAgainst):
        return ((toCheck > checkAgainst*0.95) and (toCheck < checkAgainst*1.05))

    '''
    we are feeding in the transactions files, so we would need to check whether 
    the length is 7 (or it would be a malformed line).

    transaction file is structured like:
    block_number, from_address, to_address, value, gas, gas_price, block_timestamp

    to find addresses with loops we would yield to addresses and from addresses
    as if both appear in the reducer that means there is some loop
    '''
    def mapper(self,_,line):
        try:
            fields = line.split(",")
            if (len(fields) == 7):
                from_addr = fields[1]
                to_addr = fields[2]
                value = float(fields[3])
                if value > 0 and to_addr != 'null':
                    yield(to_addr,('TO', value))
                    yield(from_addr,('FROM', value))
        except Exception as e:
            pass

    '''
    summing totals and how many loops that occur with each potential wash trade

    if both TO and FROM exist, we increment count by 1 as that is a confirmed cycle
    back to that address. 

    yield if the count of cycles is over 10 (as cycles of less than 10 are most
    likely not wash trades). another condition affecting the yield is whether the
    amount is within the 5% error margin set
    '''
    def reducer(self, addr, items):
        TO = False
        TOTOTAL = 0
        FROMTOTAL = 0
        FROM = False

        count = 0

        for itm in items:
            if TO and FROM:
                TO = False
                FROM = False
                count += 1

            if (itm[0] == 'TO'): TO = True; TOTOTAL += itm[1]
            if (itm[0] == 'FROM'): FROM = True; FROMTOTAL += itm[1]

        if (count > 10) and self.rangeChecker(TOTOTAL, FROMTOTAL):
            yield(addr,count)

if __name__ == '__main__':
    partD.JOBCONF= { 'mapreduce.job.reduces': '5' }
    partD.run()
