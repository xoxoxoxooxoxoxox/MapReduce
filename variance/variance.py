import MapReduce
import sys
import re

mr = MapReduce.MapReduce()

def mapper(record):
    # key: fixed
    # value: original number, square of the number, number of the number in each line
    num = 0
    squareNum = 0
    quantity = 0

    for number in record:
        num += number
        squareNum += number ** 2
        quantity += 1
        
    mr.emit_intermediate(0, [num, squareNum, quantity])

def reducer(key, list_of_values):
    # key: number
    # value: answer
    numSum = 0
    squareSum = 0
    total = 0

    for pair in list_of_values:
        numSum += pair[0]
        squareSum += pair[1]
        total += pair[2]

    var = (squareSum / float(total)) - ((numSum / float(total)) ** 2)
    mr.emit(var)

if __name__ == '__main__':
    inputdata = open(sys.argv[1])
    mr.execute(inputdata, mapper, reducer)
