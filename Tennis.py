from mrjob.job import MRJob

class Tennis(MRJob):
    def mapper(self, _, line):
        (_, _, _, _, _, _, _, _, _, _, winner_name, _, _, winner_ioc, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, minutes, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _) = line.split(',')
        yield (winner_name, winner_ioc), int(minutes)
        
    def reducer(self, person, minutes):
        yield person, sum(minutes)
        
if __name__ == '__main__':
    Tennis.run()
    
# !python Tennis.py "wta_matches_2016.csv" > winners.txt