from mrjob.job import MRJob
from mrjob.step import MRStep

class TotalAmountCustSort(MRJob):
    def steps(self):
        return [ MRStep(mapper = self.mapper_by, reducer = self.reducer_by), MRStep(mapper = self.mapper_sort, reducer = self.reducer_sort) ]
    def mapper_by(self, _, line):
        (customerid, idno, amount) = line.split(',')
        yield int(customerid), float(amount)
        
    def reducer_by(self, customerid, amount):
        yield customerid, '%04.02f'%sum(amount)
        
    def mapper_sort(self, customerid, total):
        yield total, customerid
        
    def reducer_sort(self, total, customerid):
        for x in customerid:
            yield total, x
        
if __name__ == '__main__':
	TotalAmountCustSort.run()

# !python TotalAmountCustSort.py "DataA1.csv" > totamountcustsort.txt