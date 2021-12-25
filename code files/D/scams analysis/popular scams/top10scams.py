from mrjob.job import MRJob, MRStep
import re
import time
from datetime import datetime

class partD(MRJob):

    '''
    check if string is an integer (is digit alternative)
    '''

    def checkInt(self, str):
        try:
            float(str)
            return True
        except ValueError:
            return False

    '''
    defining extra reducer for calculating the top 10 most lucrative scams for
    analysis
    '''
    def steps(self):
        return [MRStep(mapper=self.mapper,
                       reducer=self.addr_reducer),
                MRStep(reducer=self.scamID_reducer),
                MRStep(reducer=self.topTen_reducer)
                ]

    '''
    joining transactions with scams, with scams.csv (converted scams json)

    the following is how the files are structured:

        transactions.csv:
        block_number, from_address, to_address, value, gas, gas_price, block_timestamp

        outputB1.tsv:
        scamID, address1, address2, address3, ...(n-4 addresses)..., addressn

    checking if the length is 7 and the value column is an int

    as both of these may have length 7

    in order to capture the joins, the keys must be addresses. This means there
    must be a for loop through all scam associated addresses. The scam ID would
    be part of the value for a rejoin later on
    '''
    def mapper(self,_,line):
        try:
            fields = line.split(",")
            if (len(fields) == 7 and self.checkInt(fields[4])):
                value = float(fields[3])
                to_addr = fields[2]
                if value > 0:
                    yield(to_addr, ('T', value))
            else:
                scamID = fields[0]
                for addr in fields[1:]:
                    yield(addr.replace(" ",""), ('S', scamID))
        except:
            pass

    '''
    if both a transaction and scam address exist (scam id is not -1):
    there is a join between this address, so we are allowed to yield
    this to be joined with the rest of the scam addresses if not, 
    the address does not belong to a contract
    '''
    def addr_reducer(self, addr, items):
        totalValue = 0
        scamID = -1

        for transOrScam in items:
            if transOrScam[0] == 'T':
                totalValue += transOrScam[1]
            else:
                scamID = transOrScam[1]

        if scamID != -1:
            yield(scamID, totalValue)

    '''
    aggregating total values of each address associated with a scam to
    get total from scamID.

    yielding everything to 1 reducer to get the top 10 total value
    scams
    '''
    def scamID_reducer(self, scamIDDate, values):
        total = 0
        for val in values:
            total += val

        yield(None,(scamIDDate, total))

    '''
    recieving every single result in the 'values' generator, we can do a sorted
    call and select the range 0 to 10 to select the top 10
    '''
    def topTen_reducer(self, _, values):
        topTen = sorted(values, key=lambda x: x[1], reverse=True)[0:10]
        r = 1
        for ranking in topTen:
            yield(r,f'{ranking[0]}, {ranking[1]}')
            r += 1

if __name__ == '__main__':
    partD.JOBCONF= { 'mapreduce.job.reduces': '5' }
    partD.run()
