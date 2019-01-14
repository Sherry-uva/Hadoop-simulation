import csv

with open('duration500sec_SHJRatio5%_arrivalRate0.2-tasks-hybrid1.csv', 'r') as csvfile:
	csvreader = csv.reader(csvfile, delimiter=",")
	for line in csvreader:
		print line
		with open('test.csv', 'wt') as output:
			writer = csv.writer(output)
			writer.writerow((line))