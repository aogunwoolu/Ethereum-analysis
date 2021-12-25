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
    similar to the mapper in top 10 scams, with 2 differences:

    1. transactions have time as part of value
    2. scams are filtered to only offline scams
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
                status = fields[1]

                if status == "Offline":
                    for addr in fields[2:]:
                        yield(addr.replace(" ",""), ('S', scamID))
        except Exception as e:
            pass

    '''
    similar to top10scams reducer, except instead of finally yielding the scam ID,
    we would yield both the scam ID and date but only the date of the most recent
    transaction date (as we are assuming the offline date is the last transaction date)
    '''
    def reducer(self, addr, items):
        totalValue = 0
        scamID = -1
        mostrecent = None
        trans = []

        for transOrScam in items:
            if transOrScam[0] == 'S':
                scamID = transOrScam[1]
            else:
                totalValue += transOrScam[1]
                trans.append((transOrScam[1],transOrScam[2]))


        if scamID != -1:
            for transactions in trans:
                trnsdate = datetime.strptime(transactions[1],'%m/%Y').date()
                if (mostrecent == None):
                    mostrecent = trnsdate
                    continue
                if (trnsdate > mostrecent):
                    mostrecent = trnsdate
            if mostrecent != None:
                yield (scamID, (totalValue, f"{mostrecent}"))

if __name__ == '__main__':
    partD.JOBCONF= { 'mapreduce.job.reduces': '5' }
    partD.run()
