import csv, pdb
from math import ceil
import shelve

workload = "/Users/sherry/Box Sync/Hadoop-simulation-hybrid/input/SWIM/100jobs_SHJRatio5%_arrivalRate0.2.csv"

with open(workload, 'r') as csvfile, open('largeRJ.txt', 'w') as output:
	reader = csv.reader(csvfile)
	count = 0
	maxInput = 0
	maxInputJob = []
	for job in reader:
		if int(job[4]) < 2*1024**3 and int(job[3]) > 100*1024**3:
			count += 1
			if int(job[3]) > 1024**4:
				print 'super large RJ'
				output.write("%s\n" % job[0])
# 				output.write("%s, %s\n" % (job[0], job[6]))
			print job
			if int(job[3]) > maxInput:
				maxInput = int(job[3])
				maxInputJob = job
	print 'Total number of regular job with input larger than 100 GB is ' + str(count)
	print maxInputJob


with open(workload, 'r') as csvfile, open('largeSHJ.txt', 'w') as output:
	reader = csv.reader(csvfile)
	count = 0
	for job in reader:
		if int(job[4]) > 2*1024**3 and int(job[3])/int(job[4]) < 1000 and int(job[3]) > 1024**4:
			count += 1
			print 'super large SHJ'
			output.write("%s\n" % job[0])
			print job
print count	

# largeRJPaths = [line.strip() for line in open("largeRJ.txt", 'r')]
# with open(workload, 'r') as tsvfile:
# 	count = 0
# 	tsvreader = csv.reader(tsvfile, delimiter="\t")
# 	for job in tsvreader:
# 		if job[6] in largeRJPaths:
# 			print job
# 			count += 1
# print count
