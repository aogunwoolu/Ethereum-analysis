"""
coursework
"""
from mrjob.job import MRJob, MRStep
import re
import time

class coursework(MRJob):
    '''
    From the output of the first job (getting aggragates of addresses) we can
    filter out the transactions that are not contracts by doing a join on
    the contract csv file.

    To do this we would upload the result to the hadoop and specify the file
    in the run command (after the contracts hadoop address -
    hdfs://andromeda.eecs.qmul.ac.uk/data/ethereum/contracts), with the address of
    this secondary file being hdfs://andromeda.eecs.qmul.ac.uk/user/qbo30/filename

    alternatively, we could specify the file locally (before -r hadoop)

    after getting an input of both of these files, we can then operate on them
    by checking which file is being read at a given time.

    the following is how the files are structured:

        contracts.csv:
        address, is_erc20, is_erc721, block_number, block_timestamp

        outputB1.tsv:
        address	aggregate_val

        **note that all addresses have quotation marks

    therefore, the mapper first splits by comma (checking if the file is a comma
    seperated value file), if the length is 1 this means that there are no commas
    to split by so we may assume that it is a tab seperated variable file, so we
    split by tab. If the length is 2 (because outputB1 is <address	aggregate_val>)
    we strip quote marks from the address (first field) and yield to potentially join
    with the address as the key along with the value

    if the length is any other length we know there is at least 1 comma therefore
    it is a csv. we check it against the length of the contracts csv (5) and yield
    with address to join
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
                    yield(contractAddr, ('C'))
        except:
            pass

    '''
    if both a transaction and contract exist: there is a join between this
    address, so we are allowed to yield this address and the summed total values
    if not, the address does not belong to a contract
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
            yield(key, transactionsTotal)

if __name__ == '__main__':
    coursework.run()
