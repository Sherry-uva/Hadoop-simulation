from math import ceil, floor
import pdb, csv
from itertools import cycle

workload = "/Users/sherry/Box Sync/Hadoop-simulation-electrical/input/SWIM/FB-2010_samples_24_times_1hr_withInputPaths_0.tsv"

# with open(workload, 'r') as tsvfile:
# 	minInput = 10**9
# 	count = 0
# 	tsvreader = csv.reader(tsvfile, delimiter="\t")
# 	for job in tsvreader:
# 		if int(job[4]) > 2*1024**3 and int(job[3]) > 1024**4 and int(job[3])/int(job[4]) > 1000:
# # 		if int(job[4]) > 2*1024**3:
# 			print int(float(job[3])/int(job[4]))
# 			print job
# 			tmp = floor(int(job[4])/(1.2*1024**3))
# 			print 'Number of map racks :' + str(tmp)
# 			if tmp < 2:
# 				print '!!!!!!!!!!!!!!!!!'
# 			count += 1
# 			if  int(job[3]) < minInput:
# 				minInput = int(job[3]) 
# # 				maxJobId = job[0]
# # print maxJobId + ': ' + str(maxInput) 
# print minInput
# print count
# # 		if int(job[3]) == 0 and int(job[4]) > 0:
# # 			print job


with open(workload, 'r') as tsvfile:
	tsvreader = csv.reader(tsvfile, delimiter="\t")
	for job in tsvreader: