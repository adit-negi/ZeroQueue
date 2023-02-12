import csv
from functools import reduce
with open('dump.csv', newline='') as csvfile:
    spamreader = csv.reader(csvfile, delimiter=' ', quotechar='|')
    l=[]
    for row in spamreader:
        if row:
            l.append(row[0])

print(l)