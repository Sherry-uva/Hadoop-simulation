rm(list=ls(all=TRUE)) 

draw <- function(SHJRatio, lambda, index) {
  par(mfrow=c(1,1))
  directory <- "/Users/sherry/Box Sync/Hadoop-simulation-results/new-traces/larger-RJs-ideal-optical/4racks/"
  trace <- paste("numSHJ", 60, "_SHJRatio", SHJRatio, "%_arrivalRate", lambda, "-jobComplTime", index, ".csv", sep = "") 
  complTime <- read.csv(file=paste(directory, trace, sep = "") , header=TRUE, sep=",")
  complTime <- as.matrix(complTime)
  SHJs <- matrix(nrow = 60, ncol = 4)
  RJs <- matrix(nrow = nrow(complTime)-60, ncol = 4)
  nSHJ <- 1
  nRJ <- 1
  for(i in 1:nrow(complTime)) {
      if(complTime[i,2]=="RJ") {
          RJs[nRJ,] = c(complTime[i,1],complTime[i,3],complTime[i,4],complTime[i,5])
          nRJ <- nRJ + 1
      }
      else {
          SHJs[nSHJ,] = c(complTime[i,1],complTime[i,3],complTime[i,4],complTime[i,5])
          nSHJ <- nSHJ + 1
      }
  }
  class(SHJs) <- "numeric"
  class(RJs) <- "numeric"

  maxSHJComplTime <- max(SHJs[,2], SHJs[,3], SHJs[,4])
  plot(range(c(0,60)), range(c(0, maxSHJComplTime)), axes=FALSE, type="n",xlab='SHJ indices',ylab='job completion time (s)',cex.lab=1.2)
  title(paste("numSHJ", 60, "_SHJRatio", SHJRatio, "%_arrivalRate", lambda, sep = ""))
  box()
  axis(2, at=(0:as.integer(maxSHJComplTime/100))*100, cex.axis=1.5)
  axis(1, at=1:60, labels=SHJs[,1], cex.axis=0.8)
  lines(1:60, SHJs[,2], col='green', type = "b", pch=21)
  lines(1:60, SHJs[,3], col='blue', type = "b", pch=23)
  lines(1:60, SHJs[,4], col='red', type = "b", pch=24)
  
  z <- c("SHJ-elec.", "SHJ-hybrid-25%", "SHJ-hybrid-0%")
  boxplot(SHJs[,2],SHJs[,3],SHJs[,4], names=z, ylab='job completion time')
  title(paste("numSHJ", 60, "_SHJRatio", SHJRatio, "%_arrivalRate", lambda, sep = ""))
  
  plot(ecdf(SHJs[,2]), col='green', xlab='shuffle-heavy job completion time',ylab='CDF', main=paste("numSHJ", 60, "_SHJRatio", SHJRatio, "%_arrivalRate", lambda, sep = ""))
  lines(ecdf(SHJs[,3]), col='blue')
  lines(ecdf(SHJs[,4]), col='red')

  z <- c("RJ-elec.", "RJ-hybrid-25%", "RJ-hybrid-0%")
  boxplot(RJs[,2],RJs[,3],RJs[,4], names=z, ylab='job completion time')
  title(paste("numSHJ", 60, "_SHJRatio", SHJRatio, "%_arrivalRate", lambda, sep = ""))
  
  plot(ecdf(RJs[,2]), col='green', xlab='regular job completion time',ylab='CDF', main=paste("numSHJ", 60, "_SHJRatio", SHJRatio, "%_arrivalRate", lambda, sep = ""))
  lines(ecdf(RJs[,3]), col='blue')
  lines(ecdf(RJs[,4]), col='red')
  
  maxRJComplTime <- max(RJs[,2], RJs[,3], RJs[,4])
  plot(range(c(0,nrow(RJs))), range(c(0, maxRJComplTime)), axes=FALSE, type="n",xlab='RJ indices',ylab='job completion time (s)',cex.lab=1.2)
  title(paste("numSHJ", 60, "_SHJRatio", SHJRatio, "%_arrivalRate", lambda, sep = ""))
  box()
  axis(2, at=(0:as.integer(maxRJComplTime/10))*10, cex.axis=1.5)
  axis(1, at=1:nrow(RJs), labels=RJs[,1], cex.axis=0.8)
  lines(1:nrow(RJs), RJs[,2], col='green', type = "b", pch=21)
  lines(1:nrow(RJs), RJs[,3], col='blue', type = "b", pch=23)
  lines(1:nrow(RJs), RJs[,4], col='red', type = "b", pch=24)
  
  maxDiff = max(RJs[,2]-RJs[,3], RJs[,2]-RJs[,4])
  minDiff = min(RJs[,2]-RJs[,3], RJs[,2]-RJs[,4])
  plot(range(c(0,nrow(RJs))), range(c(minDiff, maxDiff)), axes=FALSE, type="n",xlab='RJ indices',ylab='job completion time differences (s)',cex.lab=1.2)
  title(paste("numSHJ", 60, "_SHJRatio", SHJRatio, "%_arrivalRate", lambda, sep = ""))
  box()
  axis(2, at=(as.integer(minDiff/10):as.integer(maxDiff/10))*10, cex.axis=1.5)
  axis(1, at=1:nrow(RJs), labels=RJs[,1], cex.axis=0.8)
  lines(1:nrow(RJs), RJs[,2]-RJs[,3], col='blue', type = "b", pch=21)
  lines(1:nrow(RJs), RJs[,2]-RJs[,4], col='red', type = "b", pch=23)
}

draw(10,0.3,1)





