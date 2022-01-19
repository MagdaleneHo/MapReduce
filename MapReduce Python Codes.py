# Objective 1
#!/usr/bin/env python
# coding = utf-8
from statistics import mean
import csv
from mrjob.job import MRJob
from mrjob.step import MRStep

class BudgetTop (MRJob):

    def movie_budget(self, movid):
        with open("/home/hadoop/Downloads/objective1_1.csv", "r") as infile:
           reader = csv.reader(infile, delimiter='\t')
           next(reader)
           for line in reader:
               if int(movid) == int(line[0]):
                   return line[1]

    def mapper1 (self, _, line):
        (movie_id, rating, title, budget) = line.split ('\t')
        yield movie_id, float(budget) 
 
    def reducer1 (self, key, values):
        yield key, mean(values)
    
    def mapper2 (self, key, values):
        yield None, (values, key)
    
    def reducer2 (self, _, values):
        i=0
        for budget, key in sorted(values, reverse=True):
             i+=1
             if i<=20:
                  yield (key, budget), self.movie_budget(int(key))

    def steps(self):
        return [MRStep (mapper = self.mapper1, reducer = self.reducer1),
        MRStep (mapper=self.mapper2, reducer=self.reducer2)
        ]

if __name__ == '__main__':
    BudgetTop.run ()

# Objective 1.1
#!/usr/bin/env python
# coding = utf-8
from statistics import mean
from mrjob.job import MRJob
from mrjob.step import MRStep

class AverageTop1 (MRJob):
    def mapper1 (self, _, line):
        (movie_id, rating, title, budget) = line.split ('\t')
        yield movie_id, float(rating) 
 
    def reducer1 (self, key, values):
        yield key, mean(values)

    def steps(self):
        return [MRStep (mapper = self.mapper1, reducer = self.reducer1)
        ]
 
if __name__ == '__main__':
    AverageTop1.run ()    
	
# Objective 2
#!/usr/bin/env python
# coding = utf-8

from mrjob.job import MRJob
from mrjob.step import MRStep

class RatingsBreakdown (MRJob):
    def mapper_get_ratings (self, _, line):
        (movie_id, rating, title, budget) = line.split ('\t')
        yield rating, 1
 
    def reducer_count_ratings (self, key, values):
        yield key, sum(values)
 
    def steps(self):
        return [MRStep (mapper = self.mapper_get_ratings,
                        reducer = self.reducer_count_ratings)]
 
if __name__ == '__main__':
    RatingsBreakdown.run ()

# Objective 3
#!/usr/bin/env python
# coding = utf-8
from statistics import mean
from mrjob.job import MRJob
from mrjob.step import MRStep

class AverageTop (MRJob):
    def mapper1 (self, _, line):
        (movie_id, rating, title, budget) = line.split ('\t')
        yield title, float(rating) 
 
    def reducer1 (self, key, values):
        yield key, mean(values)
    
    def mapper2 (self, key, values):
        yield None, (values, key)
    
    def reducer2 (self, _, values):
        i=0
        for rating, key in sorted(values, reverse=True):
            i+=1
            if i<=10:
                yield key, rating
    
    def steps(self):
        return [MRStep (mapper = self.mapper1, reducer = self.reducer1),
        MRStep (mapper=self.mapper2, reducer=self.reducer2)
        ]
 
if __name__ == '__main__':
    AverageTop.run ()

# Objective 3.1
#!/usr/bin/env python
# coding = utf-8
from statistics import mean
from mrjob.job import MRJob
from mrjob.step import MRStep

class AverageTop1 (MRJob):
    def mapper1 (self, _, line):
        (movie_id, rating, title, budget) = line.split ('\t')
        yield title, float(rating) 
 
    def reducer1 (self, key, values):
        yield key, mean(values)

    def steps(self):
        return [MRStep (mapper = self.mapper1, reducer = self.reducer1)
        ]
 
if __name__ == '__main__':
    AverageTop1.run ()    
	
# Objective 4
#!/usr/bin/env python
# coding = utf-8

from mrjob.job import MRJob
from mrjob.step import MRStep

class MovieCount (MRJob):
    def mapper1 (self, _, line):
        (movie_id, rating, title, budget) = line.split ('\t')
        yield title, 1 
 
    def reducer1 (self, key, values):
        yield None, (sum(values), key)
     
    def reducer2 (self, _, values):
        for count, key in sorted(values, reverse=True):
            yield key, int(count)
    
    def steps(self):
        return [MRStep (mapper = self.mapper1, reducer = self.reducer1),
        MRStep (reducer=self.reducer2)
        ]
 
if __name__ == '__main__':
    MovieCount.run ()