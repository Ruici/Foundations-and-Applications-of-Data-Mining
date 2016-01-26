import json
import re
import MapReduce
import sys


mr = MapReduce.MapReduce()


def mapper(record):
	text =  record[1]
	docuID = record[0]
	words = re.findall(r"[\w]+", text)
	
	count = dict()
	
	for w in words:
		w = w.lower()
		if w in count:
			count[w] = count[w] + 1
		else:
			count[w] = 1
			
	for w in count:
			mr.emit_intermediate(w,[docuID,count[w]])

def reducer(key, list_of_values):
    # key: word
    # value: list of occurrence counts
    total = 0
    for v in list_of_values:
    	total += 1
        
    mr.emit((key, total, list_of_values))

if __name__ == '__main__':

	inputdata = open(sys.argv[1])
	mr.execute(inputdata, mapper, reducer)



