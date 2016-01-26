import MapReduce
import sys
import re


mr = MapReduce.MapReduce()

# =============================
# Do not modify above this line
A = dict()
B = dict()

def mapper(record):
    # key: document identifier
    # value: document contents
	i = record[0]
	j = record[1]
	A[(i,j)] = record[2]
	mr.emit_intermediate(j,('A', i, A[(i,j)]))
	
	j = record[0]
	k = record[1]
	B[(j,k)] = record[2]
	mr.emit_intermediate(j,('B', k, B[(j,k)]))
	
def reducer(key, list_of_values):
    # key: word
    # value: list of occurrence counts
	#print list_of_values
	a = {}
	b = {}
	for tuple in list_of_values:
		if tuple[0] == 'A':
			a[tuple[1]] = tuple[2]
		else:
			b[tuple[1]] = tuple[2]
	
	for i in a:
		for j in b:
			mr.emit((i,j, a[i]*b[j]))
			
# Do not modify below this line
# =============================
if __name__ == '__main__':
  inputdata = open(sys.argv[1])
  mr.execute(inputdata, mapper, reducer)
