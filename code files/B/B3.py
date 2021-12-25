"""
coursework
"""
from mrjob.job import MRJob, MRStep
import re
import time

class coursework(MRJob):
    '''
    MRStep allows the running of 2 reducers directly after one and other
    '''
    def steps(self):
        return [MRStep(mapper=self.mapper,
                        reducer=self.reducer),
                MRStep(reducer=self.reducerTT)]

    '''
    identical to B part 2 mapper, because there to get top 10 of part 2's
    out, we can just run part 2 again slightly modified
    '''

    def mapper(self,_,line):
        try:
            fields = line.split(",")

            if (len(fields) == 1):
                fields = line.split("\t")
                if (len(fields) == 2):
                    value = int(fields[1])
                    recipient = fields[0].replace("\"","")
                    yield(recipient, ('T', value))
            else:
                if (len(fields) == 5):
                    contractAddr = fields[0]
                    yield(contractAddr, ('C', fields[4]))
        except:
            pass

    '''
    identical to part 2, except we are reducing None as the key to make all of the results
    to go a single reducer. This next reducer is the top 10 calculator
    '''
    def reducer(self, key, values):
        contractExists = False
        transactionsTotal = 0

        for val in values:
            if val[0] == 'T':
                transactionsTotal += int(val[1])
            if val[0] == 'C':
                contractExists = True

        if (contractExists and transactionsTotal != 0):
            yield(None, (key, transactionsTotal))

    '''
    recieving every single result in the 'vals' generator, we can do a sorted
    call and select the range 0 to 10 to select the top 10
    '''
    def reducerTT(self, _, vals):
        topTen = sorted(vals, key=lambda x: x[1], reverse=True)[0:10]
        r = 1
        for ranking in topTen:
            yield(r,f'{ranking[0]},{ranking[1]}')
            r += 1

if __name__ == '__main__':
    coursework.run()
