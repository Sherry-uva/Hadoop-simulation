import csv

with open('duration500sec_SHJRatio5%_arrivalRate0.2-tasks-hybrid1.csv', 'r') as csvfile, open('test.csv', 'wt') as output:
	csvreader = csv.reader(csvfile, delimiter=",")
	writer = csv.writer(output)
	for line in csvreader:
		print line
		writer.writerow((line))