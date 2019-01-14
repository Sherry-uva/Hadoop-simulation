import csv, sys
sys.path.insert(0, '/Users/sherry/Box Sync/Hadoop-simulation-electrical')

SHJRatios = [5, 10, 20]
lambdas = [0.1, 0.2, 0.3, 0.6, 0.9, 1.2]

for SHJRatio in SHJRatios:
	for Lambda in lambdas:
		trace = 'numSHJ40_SHJRatio' + str(SHJRatio) + '%_arrivalRate' + str(Lambda)
		count1 = 0
		count2 = 0
		with open('input/SWIM/FB-modifiedI/'+trace+'.csv', 'r') as csvfile:
				reader = csv.reader(csvfile)
				for line in reader:
						if int(line[4]) > 1024**4:
								print line
								count1 += 1
						if int(line[3]) > 1024**4:
								print line
								count2 += 1
		print trace + ': ' + str(count1) + ', ' + str(count2)
