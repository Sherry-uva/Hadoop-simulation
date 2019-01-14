import sys, os, csv, pdb
sys.path.insert(0, '/Users/sherry/Box Sync/Hadoop-simulation-electrical')
from math import ceil
from random import expovariate, random

import preProcessPath
from HDFS import datasetPlaceWithPath
from AM import resourceRequestGenerator

workload = "/Users/sherry/Box Sync/Hadoop-simulation-electrical/input/SWIM/FB-2010_samples_3_times_1hr_withInputPaths_0.tsv"

SHJRatio = float(sys.argv[2])
arrivalRate = float(sys.argv[3])
numTotalSHJs = int(sys.argv[1])
trace = 'numSHJ' + str(numTotalSHJs) + '_SHJRatio' + str(int(SHJRatio)) + '%_arrivalRate' + str(arrivalRate)
# traceDuration = float(sys.argv[1])
# trace = 'duration' + str(int(traceDuration)) + 'sec_SHJRatio' + str(int(SHJRatio)) + '%_arrivalRate' + str(arrivalRate)
workLoad = '/Users/sherry/Box Sync/Hadoop-simulation-electrical/input/SWIM/' + trace + '.csv'


# with open(workload, 'r') as tsvfile, open('input/SHJs.csv', 'w') as output1, open('input/RJs.csv', 'w') as output2:
# 	tsvreader = csv.reader(tsvfile, delimiter="\t")
# 	writer1 = csv.writer(output1)
# 	writer2 = csv.writer(output2)
# 	for job in tsvreader:
# 		if int(job[4]) > 2*1024**3 and int(job[3])/int(job[4]) < 1000:
# 			writer1.writerow((job[0:7]))
# 		else:
# 			writer2.writerow((job[0:7]))
			
# with open('input/SHJs.csv', 'r') as csvfile1, open('input/RJs.csv', 'r') as csvfile2, open(workLoad, 'w') as jobMix:
# 	readerSHJ = csv.reader(csvfile1)
# 	readerRJ = csv.reader(csvfile2)
# 	writer = csv.writer(jobMix)
# 	numSHJs = 0
# 	numJobs = 0
# 	time = 0
# 	i = 0
# 	while time < traceDuration:
# 		interArriTime = int(ceil(expovariate(arrivalRate)))
# 		time += interArriTime
# 		numJobs += 1
# 		jobType = random()
# 		if jobType < SHJRatio/100:
# 			numSHJs += 1
# 			job = next(readerSHJ)
# 			job.append(job[0])
# 			job[0] = 'job' + str(i)
# 			job[1] = 'SHJ'
# 		else:
# 			job = next(readerRJ)
# 			job.append(job[0])
# 			job[0] = 'job' + str(i)
# 			job[1] = 'RJ'
# 		job[2] = interArriTime
# 		writer.writerow((job))
# 		i += 1
# print 'Total number of jobs in the trace is ' + str(numJobs)
# print 'number of SHJs: ' + str(numSHJs)



with open(workLoad, 'r') as csvfile, open('input/largeJobs_'+trace+'.txt', 'w') as output:
	reader = csv.reader(csvfile)
	count = 0
	for job in reader:
		if int(job[3]) > 1024**4:
			count += 1
			print 'super large job'
			output.write("%s\n" % job[0])
			print job
print count	

	
preProcessPath.processPath(workLoad, trace)	

datasetPlaceWithPath.populateHDFS(workLoad, trace)

resourceRequestGenerator.generator(workLoad, trace)
	


