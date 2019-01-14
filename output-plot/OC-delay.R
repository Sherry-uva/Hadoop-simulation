rm(list=ls(all=TRUE)) 

delay <- function(numRacks, SHJRatio, lambda, index) {
  directory <- "/Users/sherry/Box\ Sync/Hadoop-simulation-results/OC-delay/"
  trace <- paste("numSHJ", numRacks, "_SHJRatio", SHJRatio, "%_arrivalRate", lambda, "_", index, "-OC.csv", sep = "") 
  delay <- read.csv(file=paste(directory, trace, sep = "") , header=FALSE, sep="\t")
  delay <- as.matrix(delay)
  delay <- as.numeric(delay[,1])
  hist(delay, main = paste(SHJRatio, "%SHJ, ", "lambda=", lambda, ", trace", index, sep=""))
}

delay(40, 10, 1.5, 0)