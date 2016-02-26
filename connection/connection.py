import MapReduce
import sys
import re

"""
Word Count Example in the Simple Python MapReduce Framework
"""

mr = MapReduce.MapReduce()

# =============================
# Do not modify above this line
def mapper(record):
    # key: friend A / friend B
    # value: friend B / friend A
    mr.emit_intermediate(record[0], record[1])
    mr.emit_intermediate(record[1], record[0])

def reducer(key, list_of_values):
    # key: friend Z
    # value: list of friend X and friend Y
    friendZone = []
    for v in list_of_values:
        friendZone.append(v)

    # sort the list
    friendZone.sort()

    # check the length of the friendZone list
    if len(friendZone) == 2:
        friendZone.append(key)
        mr.emit(friendZone)
    elif len(friendZone) > 2:
        # computing comboniation
        for i in range(0, len(friendZone) - 1):
            for j in range(i + 1, len(friendZone)):
                combo = []
                combo.append(friendZone[i])
                combo.append(friendZone[j])
                combo.append(key)
                mr.emit(combo)

# Do not modify below this line
# =============================
if __name__ == '__main__':
    inputdata = open(sys.argv[1])
    mr.execute(inputdata, mapper, reducer)