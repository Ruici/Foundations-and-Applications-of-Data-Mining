import json
import re
import MapReduce
import sys

mr = MapReduce.MapReduce()

# =============================
# Do not modify above this line

def mapper(record):
	leng = len(record)
	count = dict()
	#print record
	for i in range (0, leng):
		for j in range (i+1, leng):
			#print record[i]
			#print record[j]
			if (record[i],record[j]) in count:
				count[(record[i],record[j])] = count[(record[i],record[j])]+1
			else:
				count[(record[i],record[j])] = 1
			
	for w in count:
			mr.emit_intermediate(w,count[w])


def reducer(key, list_of_values):
    # key: word
    # value: list of occurrence counts
    total = 0
   # print key
    for v in list_of_values:
      total += v
    #print key, total
    if total >= 100:
    	mr.emit((key[0],key[1]))

# Do not modify below this line
# =============================
if __name__ == '__main__':

	inputdata = open(sys.argv[1])
	mr.execute(inputdata, mapper, reducer)



