"""
coursework
"""
from mrjob.job import MRJob, MRStep
import re
import time

class coursework(MRJob):
    '''
    the following is how the files are structured:

        contracts.csv:
        address, is_erc20, is_erc721, block_number, block_timestamp

        transactions.csv:
        block_number, from_address, to_address, value, gas, gas_price, block_timestamp

    the mapper first splits by comma (checking if the file is a comma
    seperated value file), if the length is 7, it must be transactions and if it is length 5
    this means it is contracts. From both of these we will select the items we want
    for our kmeans clustering algorithm

    joined together by transactions to contracts
    '''

    def mapper(self,_,line):
        try:
            fields = line.split(",")
            if (len(fields) == 7):
                value = int(fields[-4])
                gas = int(fields[-3])
                gas_price = int(fields[-2])
                recipient = fields[2]
                if value != 0 and recipient != "null":
                    yield(recipient, ('T', value, gas, gas_price, 1))
            else:
                if (len(fields) == 5):
                    contractAddr = fields[0]
                    block_no = fields[-2]
                    yield(contractAddr, ('C', block_no))
        except:
            pass

    '''
    features extracted: average gas price, average gas, block number, total value
    '''

    def reducer(self, key, values):
        count = 0
        contractExists = False
        gasPrice = 0
        gas = 0
        block_number = None
        total_value = 0

        for val in values:
            if val[0] == 'T':
                gasPrice += val[-2]
                gas += val[-3]
                total_value += val[1]
                count += val[-1]

            if val[0] == 'C':
                contractExists = True
                block_number = val[1]

        if (contractExists and total_value != 0):
            avg_gas_price = gasPrice/count
            avg_gas = gas/count
            yield(key, (avg_gas_price, avg_gas, block_number, total_value))

if __name__ == '__main__':
    coursework.JOBCONF= { 'mapreduce.job.reduces': '25' }
    coursework.run()
