"""Lab 1. Basic wordcount
"""
from mrjob.job import MRJob, MRStep
import re
import time
from datetime import datetime

class partD(MRJob):

    '''
    identical to washtradingAddr.py except with an additional top 10
    reducer to get the most likely culprits of wash trading
    '''
    def steps(self):
        return [MRStep(mapper=self.mapper,
                        reducer=self.reducer),
                MRStep(reducer=self.reducerTT)]

    def rangeChecker(self, toCheck, checkAgainst):
        return ((toCheck > checkAgainst*0.95) and (toCheck < checkAgainst*1.05))

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
            yield(None,(addr,count))

    def reducerTT(self, _, vals):
        topTen = sorted(vals, key=lambda x: x[1], reverse=True)[0:10]
        r = 1
        for ranking in topTen:
            yield(r,f'{ranking[0]},{ranking[1]}')
            r += 1

if __name__ == '__main__':
    partD.JOBCONF= { 'mapreduce.job.reduces': '5' }
    partD.run()
