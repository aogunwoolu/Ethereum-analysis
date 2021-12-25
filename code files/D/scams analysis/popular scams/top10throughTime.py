from mrjob.job import MRJob, MRStep
import re
import time
from datetime import datetime

class partD(MRJob):

    '''
    id table dictionary to store top 10 scams
    '''
    id_table = {}

    '''
    MRStep allows the running of 2 reducers directly after one and other
    '''
    def steps(self):
        return [
            MRStep(
                mapper_init=self.mapper_join_init,
                mapper=self.mapper,
                reducer=self.addr_reducer
            ),
            MRStep(
                reducer=self.scamID_reducer
            )
        ]

    '''
    load the top 10 scams preemptively to filter out scams in the mapper
    these are loaded into id_table dictionary

    with address as key and total value as value
    '''
    def mapper_join_init(self):
        with open("top10scams.tsv") as f:
            for line in f:
                fields = line.split("\t")
                ActualFields = fields[1].replace("\"","").split(", ")
                key = ActualFields[0]
                val = ActualFields[1]
                self.id_table[key] = val

    '''
    similar to the mapper in top 10 scams, with 2 differences:

    1. transactions have time as part of value
    2. scams are filtered to only include top 10 scams
    '''
    def mapper(self,_,line):
        try:
            fields = line.split(",")
            if (len(fields) == 7 and fields[3].isdigit()):
                value = int(fields[3])
                to_addr = fields[2]
                Btime = time.strftime("%m/%Y",time.gmtime((int(fields[-1]))))
                if value > 0:
                    yield(to_addr, ('T', value, Btime))
            else:
                scamID = fields[0]

                if scamID in self.id_table:
                    for addr in fields[2:]:
                        yield (addr.replace(" ", ""), ('S', scamID))
        except Exception as e:
            pass

    '''
    similar to top10scams reducer, except instead of finally yielding the scam ID,
    we would yield both the scam ID and date (therefore tracking through time)
    '''
    def addr_reducer(self, addr, items):
        totalValue = 0
        scamID = -1
        transactions = []

        for transOrScam in items:
            if transOrScam[0] == 'S':
                scamID = transOrScam[1]
            else:
                totalValue += transOrScam[1]
                transactions.append((transOrScam[1],transOrScam[2]))

        if scamID != -1:
            for trns in transactions:
                yield (f'{scamID} {trns[1]}', trns[0])

    '''
    identical to top10scams except yielding the date and id as key instead of
    none as we have already filtered scams to be top 10, here we are just
    mapping through time 
    '''
    def scamID_reducer(self, scamIDDate, values):
        total = 0
        for val in values:
            total += val

        yield(scamIDDate, total)

#this part of the python script tells to actually run the defined MapReduce job. Note that Lab1 is the name of the class
if __name__ == '__main__':
    partD.JOBCONF= { 'mapreduce.job.reduces': '5' }
    partD.run()
