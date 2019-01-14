rm(list=ls(all=TRUE)) 

responseTime <- function(numRacks, SHJRatio, lambda) {
  directory <- "/Users/sherry/Box\ Sync/Hadoop-simulation-results/jobCompletionTime-M2/max/"
  trace <- paste("numSHJ", 40, "_SHJRatio", SHJRatio, "%_arrivalRate", lambda, sep = "") 
  responseTime <- read.csv(file=paste(directory, trace, sep = "") , header=TRUE, sep=",")
  responseTime <- as.matrix(responseTime)
  z <- c("Original\nRJs", "75%\nRJs", "100%\nRJs", "Original\nSHJs", "75%\nSHJs", "100%\nSHJs")
  boxplot(as.numeric(responseTime[,1]),as.numeric(responseTime[,3]),as.numeric(responseTime[,5]),
          as.numeric(responseTime[,2]),as.numeric(responseTime[,4]),as.numeric(responseTime[,6]),
          names=z, yaxt = 'n', ylab="max. job response time (s)", ylim=c(min(as.numeric(responseTime)),max(as.numeric(responseTime))), cex.lab=3,lwd=2,cex=2.2,cex.axis=2.5)
  axis(2, at=seq(500,2500,500), labels=seq(500,2500,500), mgp=c(4,1,0), cex.lab=2,cex=2.2,cex.axis=2.2)
  title(paste("max. job response time, ", SHJRatio, "%SHJ, ", "lambda=", lambda, sep=""))
}


par(mar=c(5,9,4,2)+0.1, oma = c(2,2,1,1) + 0.1, mgp = c(4, 4, 0))
responseTime(40, 20, 0.3)


