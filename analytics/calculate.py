import csv
from functools import reduce
with open('dump.csv', newline='') as csvfile:
    spamreader = csv.reader(csvfile, delimiter=' ', quotechar='|')
    l=[]
    for row in spamreader:
        l.append(row)

print(reduce(lambda a,b:a+b, l))