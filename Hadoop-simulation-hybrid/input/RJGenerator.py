from random import random, randint 
import numpy as np
import csv

# with open('RJs.csv', 'w') as output:
# 	writer = csv.writer(output)
# 	for count in range(10000):
# 		jobId = 'job'+str(count)
# 		inputPath = 'inputPathRJ'+str(count) 
# 		jobSizeCategory = random()
# 		if jobSizeCategory < 0.4:	
# 			numMaps = randint(1, 9)
# 		elif jobSizeCategory < 0.8:
# 			numMaps = randint(10, 99)
# 		elif jobSizeCategory < 0.98:
# 			numMaps = randint(100, 499)
# 		else:
# 			numMaps = randint(500, 10000)
# # 		if numMaps < 10000:
# # 			flagLargeJob = random()
# # 			if flagLargeJob < 0.1:
# # 				inputSize = numMaps*1024**3
# # 			else:
# # 				inputSize = numMaps*128*1024*1024
# # 		else:
# # 			inputSize = numMaps*128*1024*1024
# 		inputSize = numMaps*128*1024*1024
# 		flagNoReduce = random()
# 		if flagNoReduce < 0.1:
# 			shuffleSize = 0
# 		elif flagNoReduce < 0.8:
# 			shuffleSize = int(random()*0.8*1024**3) # in bytes
# 		else:
# 			shuffleSize = int(random()*2*1024**3) # in bytes
# 		writer.writerow((jobId, 0, 0, inputSize, shuffleSize, 0, inputPath))


with open('RJs-larger.csv', 'w') as output:
	writer = csv.writer(output)
	for count in range(10000):
		jobId = 'job'+str(count)
		inputPath = 'inputPathRJ'+str(count) 
		jobSizeCategory = random()
		if jobSizeCategory < 0.2:	
			numMaps = randint(1, 9)
		elif jobSizeCategory < 0.7:
			numMaps = randint(10, 99)
		elif jobSizeCategory < 0.98:
			numMaps = randint(100, 499)
		else:
			numMaps = randint(500, 10000)
		inputSize = numMaps*128*1024*1024
		flagNoReduce = random()
		if flagNoReduce < 0.1:
			shuffleSize = 0
		elif flagNoReduce < 0.8:
			shuffleSize = int(random()*0.8*1024**3) # in bytes
		else:
			shuffleSize = int(random()*2*1024**3) # in bytes
		writer.writerow((jobId, 0, 0, inputSize, shuffleSize, 0, inputPath))
		
		
		
		
		
		