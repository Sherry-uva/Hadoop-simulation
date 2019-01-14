trace <- "/Users/sherry/Box Sync/Hadoop-simulation-optical/input/SWIM/numSHJ40_SHJRatio10%_arrivalRate1.2_0.csv"
jobs <- read.csv(file=trace, header=FALSE, sep=",")
jobs <- as.matrix(jobs)

shuffleSize <- c()
for (i in 1:nrow(jobs)) {
  inputSize <- as.numeric(jobs[i,4])
  if (inputSize < 1024**4) {
    numMaps = as.integer(ceiling(inputSize/(1024.0*1024*128)))
  } else{
    numMaps = as.integer(ceiling(inputSize/(1024.0^3)))
  }
  numReduces = as.integer(ceiling(numMaps/8.0))
  shuffleSize = c(shuffleSize, ceiling(as.double(jobs[i,5])/(numMaps*numReduces)))
}

shuffleSize <- shuffleSize/(1024^2)
hist(shuffleSize)