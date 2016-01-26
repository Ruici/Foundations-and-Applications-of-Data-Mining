import MapReduce
import sys
import re



mr = MapReduce.MapReduce()

# =============================
# Do not modify above this line
A = dict()
B = dict()

def mapper(record):
    i = record[0]
	j = record[1]
	A[(i,j)] = record[2]
	for k in range(0,5):
		mr.emit_intermediate((i,k),('A', j, A[(i,j)]))
	
	j = record[0]
	k = record[1]
	B[(j,k)] = record[2]
	for i in range(0,5):
		mr.emit_intermediate((i,k),('B',j,B[(j,k)]))
	
def reducer(key, list_of_values):
    # key: word
    # value: list of occurrence counts
    total = 0
    
    a = dict()
    b = dict()
    for tuple in list_of_values:
    	if tuple[0] == 'A':
    		a[tuple[1]] = tuple[2]
    	else:
    		b[tuple[1]] = tuple[2]
    for ele in a:
    	if ele not in b:
    		b[ele] = 0
    for ele in b:
    	if ele not in a:
    		a[ele] = 0
    for m in a:
    	total += a[m] * b[m]
    mr.emit((key[0], key[1], total))

# Do not modify below this line
# =============================
if __name__ == '__main__':
  inputdata = open(sys.argv[1])
  mr.execute(inputdata, mapper, reducer)
