rm(list=ls(all=TRUE)) 

boxplotTS1 <- function(SHJRatio1, lambda1, index1, SHJRatio2, lambda2, index2) {
  par(mfrow=c(1,1), mar=c(5, 6, 4, 2), oma = c(2,2,1,1) + 0.1, mgp = c(3, 5, 0))
  complTime1 <- complTimeTS1(4, SHJRatio1, lambda1, index1)
  complTimeSHJ1 <- matrix(unlist(complTime1[1]), ncol = 5, byrow = FALSE)
  complTimeRJ1 <- matrix(unlist(complTime1[2]), ncol = 5, byrow = FALSE)
  complTimeAll1 <- matrix(unlist(complTime1[3]), ncol = 5, byrow = FALSE)
  complTime2 <- complTimeTS1(12, SHJRatio2, lambda2, index2)
  complTimeSHJ2 <- matrix(unlist(complTime2[1]), ncol = 5, byrow = FALSE)
  complTimeRJ2 <- matrix(unlist(complTime2[2]), ncol = 5, byrow = FALSE)
  complTimeAll2 <- matrix(unlist(complTime2[3]), ncol = 5, byrow = FALSE)
  
  # z <- c("EPS-only\n4 racks", "Hybrid-25%\n4 racks", "Hybrid-0%\n4 racks", "EPS-only\n12 racks", "Hybrid-25%\n12 racks", "Hybrid-0%\n12 racks")
  z <- c("Original\n4", "75%\n4", "100%\n4", "Original\n12", "75%\n12", "100%\n12")
  boxplot(as.numeric(complTimeSHJ1[,3]),as.numeric(complTimeSHJ1[,4]),as.numeric(complTimeSHJ1[,5]),
          as.numeric(complTimeSHJ2[,3]),as.numeric(complTimeSHJ2[,4]),as.numeric(complTimeSHJ2[,5]), 
          names=z, yaxt='n', ylim=c(0,640), ylab='job response time (s)', cex.lab=3,lwd=2,cex=2.2,cex.axis=2.5)
  axis(2, at=c(0,200,400,600,800), labels=c(0,200,400,600,800),mgp=c(4,1,0), cex.lab=2,cex=2.2,cex.axis=2.2)
  title(paste("shuffle-heavy jobs, TS1, ", SHJRatio1, "%SHJ, ", "lambda=", lambda1, ", ", lambda2, sep=""))
  
  z <- c("Original\n4", "75%\n4", "100%\n4", "Original\n12", "75%\n12", "100%\n12")
  boxplot(as.numeric(complTimeRJ1[,3]),as.numeric(complTimeRJ1[,4]),as.numeric(complTimeRJ1[,5]),
          as.numeric(complTimeRJ2[,3]),as.numeric(complTimeRJ2[,4]),as.numeric(complTimeRJ2[,5]), 
          names=z, yaxt='n', ylim=c(0,340),ylab='job response time (s)', cex.lab=3,lwd=2,cex=2.2,cex.axis=2.5)
  axis(2, at=c(0,100,200,300), labels=c(0,100,200,300),mgp=c(4,1,0), cex.lab=2,cex=2.2,cex.axis=2.2)
  title(paste("regular jobs, TS1, ", SHJRatio1, "%SHJ, ", "lambda=", lambda1, ", ", lambda2, sep=""))
  
  z <- c("Original\n4", "75%\n4", "100%\n4", "Original\n12", "75%\n12", "100%\n12")
  boxplot(as.numeric(complTimeAll1[,3]),as.numeric(complTimeAll1[,4]),as.numeric(complTimeAll1[,5]),
          as.numeric(complTimeAll2[,3]),as.numeric(complTimeAll2[,4]),as.numeric(complTimeAll2[,5]), 
          names=z, yaxt='n', ylim=c(0,640), ylab='job response time (s)', cex.lab=3,lwd=2,cex=2.2,cex.axis=2.5)
  axis(2, at=c(0,200,400,600,800), labels=c(0,200,400,600,800),mgp=c(4,1,0), cex.lab=2,cex=2.2,cex.axis=2.2)
  title(paste("all jobs, TS1, ", SHJRatio1, "%SHJ, ", "lambda=", lambda1, ", ", lambda2, sep=""))
}

boxplotTS2 <- function(SHJRatio1, lambda1, index1, SHJRatio2, lambda2, index2) {
  complTime1 <- complTimeTS2(4, SHJRatio1, lambda1, index1)
  complTimeSHJ1 <- matrix(unlist(complTime1[1]), ncol = 5, byrow = FALSE)
  complTimeRJ1 <- matrix(unlist(complTime1[2]), ncol = 5, byrow = FALSE)
  complTimeAll1 <- matrix(unlist(complTime1[3]), ncol = 5, byrow = FALSE)
  complTime2 <- complTimeTS2(12, SHJRatio2, lambda2, index2)
  complTimeSHJ2 <- matrix(unlist(complTime2[1]), ncol = 5, byrow = FALSE)
  complTimeRJ2 <- matrix(unlist(complTime2[2]), ncol = 5, byrow = FALSE)
  complTimeAll2 <- matrix(unlist(complTime2[3]), ncol = 5, byrow = FALSE)
  
  z <- c("EPS-only, 4racks", "Hybrid-25%, 4racks", "Hybrid-0%, 4racks", "EPS-only, 12racks", "Hybrid-25%, 12racks", "Hybrid-0%, 12racks")
  boxplot(as.numeric(complTimeSHJ1[,3]),as.numeric(complTimeSHJ1[,4]),as.numeric(complTimeSHJ1[,5]),
          as.numeric(complTimeSHJ2[,3]),as.numeric(complTimeSHJ2[,4]),as.numeric(complTimeSHJ2[,5]), names=z, 
          ylab='job completion time', cex.lab=2,lwd=3.5,cex=2.4,cex.axis=2)
  title(paste("shuffle-heavy jobs, TS2, ", SHJRatio1, "%SHJ, ", "lambda=", lambda1, ", ", lambda2, sep=""))
  
  z <- c("EPS-only, 4racks", "Hybrid-25%, 4racks", "Hybrid-0%, 4racks", "EPS-only, 12racks", "Hybrid-25%, 12racks", "Hybrid-0%, 12racks")
  boxplot(as.numeric(complTimeRJ1[,3]),as.numeric(complTimeRJ1[,4]),as.numeric(complTimeRJ1[,5]),
          as.numeric(complTimeRJ2[,3]),as.numeric(complTimeRJ2[,4]),as.numeric(complTimeRJ2[,5]), names=z, 
          ylab='job completion time', cex.lab=2,lwd=3.5,cex=2.4,cex.axis=2)
  title(paste("regular jobs, TS2, ", SHJRatio1, "%SHJ, ", "lambda=", lambda1, ", ", lambda2, sep=""))
  
  z <- c("EPS-only, 4racks", "Hybrid-25%, 4racks", "Hybrid-0%, 4racks", "EPS-only, 12racks", "Hybrid-25%, 12racks", "Hybrid-0%, 12racks")
  boxplot(as.numeric(complTimeAll1[,3]),as.numeric(complTimeAll1[,4]),as.numeric(complTimeAll1[,5]),
          as.numeric(complTimeAll2[,3]),as.numeric(complTimeAll2[,4]),as.numeric(complTimeAll2[,5]), names=z, 
          ylab='job completion time', cex.lab=2,lwd=3.5,cex=2.4,cex.axis=2)
  title(paste("all jobs, TS2, ", SHJRatio1, "%SHJ, ", "lambda=", lambda1, ", ", lambda2, sep=""))
}


complTimeTS1 <- function(numRacks, SHJRatio, lambda, index) {
  directory <- paste("/Users/sherry/Box Sync/Hadoop-simulation-results/new-traces/FB-M1/",numRacks,"racks/",sep="")
  trace <- paste("numSHJ", 40, "_SHJRatio", SHJRatio, "%_arrivalRate", lambda, sep = "") 
  jobs <- read.csv(file=paste(directory, trace, "-jobComplTime", index, ".csv", sep = "") , header=TRUE, sep=",")
  jobs <- as.matrix(jobs)
  
  numSHJs <- 0
  numRJs <- 0
  for(i in 1:nrow(jobs)) {
    if(jobs[i,2] == "RJ") {
      numRJs <- numRJs + 1
    }else {
      numSHJs <- numSHJs + 1
    }
  }
  
  SHJcomplTime <- matrix(nrow=numSHJs, ncol=5)
  RJcomplTime <- matrix(nrow=numRJs, ncol=5)
  count1 <-1
  count2 <-1
  for(i in 1:nrow(jobs)) {
    if(jobs[i,2] == "RJ") {
      RJcomplTime[count1,] <- jobs[i,1:5]
      count1 <- count1+1
    }else {
      SHJcomplTime[count2,] <- jobs[i,1:5]
      count2 <- count2+1
    }
  }
  
  return (list(SHJcomplTime, RJcomplTime, jobs[,1:5]))
}

complTimeTS2 <- function(numRacks, SHJRatio, lambda, index) {
  directory <- paste("/Users/sherry/Box Sync/Hadoop-simulation-results/new-traces/larger-RJs-ideal-optical/",numRacks,"racks/",sep="")
  trace <- paste("numSHJ", 60, "_SHJRatio", SHJRatio, "%_arrivalRate", lambda, sep = "") 
  jobs <- read.csv(file=paste(directory, trace, "-jobComplTime", index, ".csv", sep = "") , header=TRUE, sep=",")
  jobs <- as.matrix(jobs)
  
  numSHJs <- 0
  numRJs <- 0
  for(i in 1:nrow(jobs)) {
    if(jobs[i,2] == "RJ") {
      numRJs <- numRJs + 1
    }else {
      numSHJs <- numSHJs + 1
    }
  }
  
  SHJcomplTime <- matrix(nrow=numSHJs, ncol=5)
  RJcomplTime <- matrix(nrow=numRJs, ncol=5)
  count1 <-1
  count2 <-1
  for(i in 1:nrow(jobs)) {
    if(jobs[i,2] == "RJ") {
      RJcomplTime[count1,] <- jobs[i,1:5]
      count1 <- count1+1
    }else {
      SHJcomplTime[count2,] <- jobs[i,1:5]
      count2 <- count2+1
    }
  }
  
  return (list(SHJcomplTime, RJcomplTime, jobs[,1:5]))
}

boxplotTS1(20,0.3,4,20,1.6,4)
# boxplotTS2(20,0.3,1,20,1.5,1)



