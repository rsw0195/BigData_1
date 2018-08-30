from mrjob.job import MRJob

class TotalAmountCust(MRJob):

	def mapper(self, _, line):
		(customerid, idno, amount) = line.split(',')
		yield customerid.zfill(2), float(amount)

	def reducer(self, customerid, amount):
		yield customerid, '%04.02f' % sum(amount)

if __name__ == '__main__':
	TotalAmountCust.run()

# !python TotalAmountCust.py DataA1.csv > totamountcust.txt