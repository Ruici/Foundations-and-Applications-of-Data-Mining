import MapReduce
import sys
import re

mr = MapReduce.MapReduce()

A = dict()
B = dict()

def mapper(record):
    # key: document identifier
    # value: document contents
	i = record[0]
	j = record[1]
	#print i, j, record[1]
	mr.emit_intermediate((i,j), record[2])
	
def reducer(key, list_of_values):
    # key: word
    # value: list of occurrence counts
	#print list_of_values
	total = 0
	for v in list_of_values:
		total += v
	mr.emit((key[0], key[1], total))
			
# Do not modify below this line
# =============================
if __name__ == '__main__':
  inputdata = open(sys.argv[1])
  mr.execute(inputdata, mapper, reducer)
