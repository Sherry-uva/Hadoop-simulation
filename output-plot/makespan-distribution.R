rm(list=ls(all=TRUE)) 

makespan4 <- function(numRacks, SHJRatio, lambda) {
  directory <- "/Users/sherry/Box\ Sync/Hadoop-simulation-results/makespan-M2/"
  trace <- paste("numSHJ", 40, "_SHJRatio", SHJRatio, "%_arrivalRate", lambda, sep = "") 
  makespan4 <- read.csv(file=paste(directory, trace, sep = "") , header=TRUE, sep="\t")
  makespan4 <- as.matrix(makespan4)
  # z <- c("Original\n4", "75%\n4", "100%\n4", "Original\n12", "75%\n12", "100%\n12")
  z <- c("Original", "75%", "100%")
  boxplot(as.numeric(makespan4[,3]),as.numeric(makespan4[,1]),as.numeric(makespan4[,2]),
          names=z, ylim=c(min(as.numeric(makespan4)),max(as.numeric(makespan4))), ylab='', cex.lab=1.8,lwd=2,cex=2,cex.axis=2)
  # axis(2, at=c(0,200,400,600,800,1000), labels=c(0,200,400,600,800,1000),mgp=c(4,1,0), cex.lab=2,cex=2.2,cex.axis=2.2)
  #title(paste("4racks, ", SHJRatio, "%SHJ, ", "lambda=", lambda, sep=""))
  title(ylab="makespan (s)", mgp=c(3,1,0),cex.lab=2.5)
}

makespan12 <- function(numRacks, SHJRatio, lambda) {
  directory <- "/Users/sherry/Box\ Sync/Hadoop-simulation-results/makespan-M2/"
  trace <- paste("numSHJ", 40, "_SHJRatio", SHJRatio, "%_arrivalRate", lambda, sep = "") 
  makespan12 <- read.csv(file=paste(directory, trace, sep = "") , header=TRUE, sep="\t")
  makespan12 <- as.matrix(makespan12)
  z <- c("Original", "75%", "100%")
  boxplot(as.numeric(makespan12[,3]),as.numeric(makespan12[,1]),as.numeric(makespan12[,2]),
          names=z, ylim=c(min(as.numeric(makespan12)),max(as.numeric(makespan12))), ylab='', cex.lab=1.8,lwd=2,cex=2,cex.axis=2)
  # axis(2, at=c(0,200,400,600,800,1000), labels=c(0,200,400,600,800,1000),mgp=c(4,1,0), cex.lab=2,cex=2.2,cex.axis=2.2)
  #title(paste("12racks, ", SHJRatio, "%SHJ, ", "lambda=", lambda, sep=""))
  title(ylab="makespan (s)", mgp=c(3,1,0),cex.lab=2.5)
}

par(mar=c(5,5,4,25)+0.1, oma = c(2,2,1,1) + 0.1, mgp = c(1, 1.1, 0))
# makespan4(40, 20, 0.3)
makespan12(40, 5, 1.5)


