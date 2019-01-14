rm(list=ls(all=TRUE)) 

overlapping <- function(SHJRatio, lambda, index) {
  par(mfrow=c(1,1))
  directory1 <- "/Users/sherry/Box Sync/Hadoop-simulation-hybrid/input/SWIM/FB-modifiedI/"
  directory2 <- "/Users/sherry/Box Sync/Hadoop-simulation-results/new-traces/FB-M1/12racks/"
  trace <- paste("numSHJ", 40, "_SHJRatio", SHJRatio, "%_arrivalRate", lambda, sep = "")
  startTime  <- read.csv(file=paste(directory1, trace, ".csv", sep = ""), header=FALSE, sep=",")
  startTime <- as.matrix(startTime)
  startTime <- as.numeric(startTime[,2])
  complTime  <- read.csv(file=paste(directory2, trace, "-jobComplTime", index, ".csv", sep = "") , header=TRUE, sep=",")
  complTime <- as.matrix(complTime)
  numSHJs <- 0
  numRJs <- 0
  finishTime <- max(as.numeric(complTime[,3:5]))
  plot(range(c(0,1200)), range(c(0, (nrow(complTime)-1))), axes=FALSE, type="n",xlab='Time (s)',ylab='job indices',cex.lab=1.2)
  box()
  axis(2, at=(0:round(nrow(complTime)/100))*100, cex.axis=1.5)
  axis(1, at=(0:round(1200/100))*100, cex.axis=1)
  for(i in 1:nrow(complTime)) {
      if(complTime[i,2] == "RJ") {
          segments(startTime[i],numRJs+40, as.numeric(complTime[i,3])+startTime[i], numRJs+40)
          numRJs <- numRJs+1
      }
      else {
          segments(startTime[i],numSHJs, as.numeric(complTime[i,3])+startTime[i], numSHJs)
          numSHJs <- numSHJs+1
      }
  }
}

overlapping(10, 0.6, 4)