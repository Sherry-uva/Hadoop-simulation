rm(list=ls(all=TRUE)) 

draw <- function(SHJRatio, lambda, index) {
  par(mfrow=c(1,1))
  directory <- "/Users/sherry/Box Sync/Hadoop-simulation-results/new-traces/FB-M1/12racks/"
  trace <- paste("numSHJ", 40, "_SHJRatio", SHJRatio, "%_arrivalRate", lambda, "-SHJs", index, ".csv", sep = "") 
  SHJs <- read.csv(file=paste(directory, trace, sep = "") , header=TRUE, sep=",")
  SHJs <- as.matrix(SHJs)
  maxSHJComplTime <- max(SHJs[,2], SHJs[,3], SHJs[,4])
  plot(range(c(0,40)), range(c(0, maxSHJComplTime)), axes=FALSE, type="n",xlab='SHJ indices',ylab='job completion time (s)',cex.lab=1.2)
  title(paste("numSHJ", 40, "_SHJRatio", SHJRatio, "%_arrivalRate", lambda, sep = ""))
  box()
  axis(2, at=(0:as.integer(maxSHJComplTime/100))*100, cex.axis=1.5)
  axis(1, at=1:40, labels=SHJs[,1], cex.axis=0.8)
  lines(1:40, SHJs[,2], col='green', type = "b", pch=21)
  lines(1:40, SHJs[,3], col='blue', type = "b", pch=23)
  lines(1:40, SHJs[,4], col='red', type = "b", pch=24)
  
  z <- c("SHJ-elec.", "SHJ-hybrid-25%", "SHJ-hybrid-0%")
  boxplot(SHJs[,2],SHJs[,3],SHJs[,4], names=z, ylab='job completion time')
  title(paste("numSHJ", 40, "_SHJRatio", SHJRatio, "%_arrivalRate", lambda, sep = ""))
  
  
  trace <- paste("numSHJ", 40, "_SHJRatio", SHJRatio, "%_arrivalRate", lambda, "-RJs", index, ".csv", sep = "") 
  RJs <- read.csv(file=paste(directory, trace, sep = "") , header=TRUE, sep=",")
  RJs <- as.matrix(RJs)
  z <- c("RJ-elec.", "RJ-hybrid-25%", "RJ-hybrid-0%")
  boxplot(RJs[,2],RJs[,3],RJs[,4], names=z, ylab='job completion time')
  title(paste("numSHJ", 40, "_SHJRatio", SHJRatio, "%_arrivalRate", lambda, sep = ""))

  plot(ecdf(RJs[,2]), col='green', xlab='regular job completion time',ylab='CDF', main=paste("numSHJ", 40, "_SHJRatio", SHJRatio, "%_arrivalRate", lambda, sep = ""))
  # lines(ecdf(RJs[,3]), col='blue')
  lines(ecdf(RJs[,4]), col='red')

  maxRJComplTime <- max(RJs[,2], RJs[,3], RJs[,4])
  plot(range(c(0,nrow(RJs))), range(c(0, maxRJComplTime)), axes=FALSE, type="n",xlab='RJ indices',ylab='job completion time (s)',cex.lab=1.2)
  title(paste("numSHJ", 40, "_SHJRatio", SHJRatio, "%_arrivalRate", lambda, sep = ""))
  box()
  axis(2, at=(0:as.integer(maxRJComplTime/10))*10, cex.axis=1.5)
  axis(1, at=1:nrow(RJs), labels=RJs[,1], cex.axis=0.8)
  lines(1:nrow(RJs), RJs[,2], col='green', type = "b", pch=21)
  lines(1:nrow(RJs), RJs[,3], col='blue', type = "b", pch=23)
  lines(1:nrow(RJs), RJs[,4], col='red', type = "b", pch=24)
  
  maxDiff = max(RJs[,2]-RJs[,3], RJs[,2]-RJs[,4])
  minDiff = min(RJs[,2]-RJs[,3], RJs[,2]-RJs[,4])
  plot(range(c(0,nrow(RJs))), range(c(minDiff, maxDiff)), axes=FALSE, type="n",xlab='RJ indices',ylab='job completion time differences (s)',cex.lab=1.2)
  title(paste("numSHJ", 40, "_SHJRatio", SHJRatio, "%_arrivalRate", lambda, sep = ""))
  box()
  axis(2, at=(as.integer(minDiff/10):as.integer(maxDiff/10))*10, cex.axis=1.5)
  axis(1, at=1:nrow(RJs), labels=RJs[,1], cex.axis=0.8)
  lines(1:nrow(RJs), RJs[,2]-RJs[,3], col='blue', type = "b", pch=21)
  lines(1:nrow(RJs), RJs[,2]-RJs[,4], col='red', type = "b", pch=23)
}

