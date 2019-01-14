rm(list=ls(all=TRUE)) 

boxplotTS <- function(numRacks, SHJRatio, lambda, index1, index2) {
  par(mfrow=c(1,1), mar=c(5, 6, 4, 2), oma = c(2,2,1,1) + 0.1, mgp = c(3, 5, 0))
  complTime <- complTimeTS(numRacks, SHJRatio, lambda, index1, index2)
  complTimeSHJ <- matrix(unlist(complTime[1]), ncol = 8, byrow = FALSE)
  complTimeRJ <- matrix(unlist(complTime[2]), ncol = 8, byrow = FALSE)
  complTimeAll <- matrix(unlist(complTime[3]), ncol = 8, byrow = FALSE)
  maxSHJ <- max(as.numeric(complTimeSHJ[,3:8]))
  maxRJ <- max(as.numeric(complTimeRJ[,3:8]))
  
  # z <- c("Original\n2", "75%\n2", "100%\n2", "Original\n3", "75%\n3", "100%\n3")
  z <- c("Original\n2", "Original\n3", "75%\n2", "75%\n3", "100%\n2", "100%\n3")
  boxplot(as.numeric(complTimeSHJ[,3]),as.numeric(complTimeSHJ[,6]),as.numeric(complTimeSHJ[,4]),
          as.numeric(complTimeSHJ[,7]),as.numeric(complTimeSHJ[,5]),as.numeric(complTimeSHJ[,8]), 
          names=z, yaxt='n', ylim=c(0,maxSHJ), ylab='job response time (s)', cex.lab=3,lwd=2,cex=2.2,cex.axis=2.5)
  axis(2, at=c(0,200,400,600,800,1000), labels=c(0,200,400,600,800,1000),mgp=c(4,1,0), cex.lab=2,cex=2.2,cex.axis=2.2)
  title(paste("shuffle-heavy jobs, TS1, ", numRacks, "racks, ", SHJRatio, "%SHJ, ", "lambda=", lambda, sep=""))
  
  z <- c("Original\n2", "Original\n3", "75%\n2", "75%\n3", "100%\n2", "100%\n3")
  boxplot(as.numeric(complTimeRJ[,3]),as.numeric(complTimeRJ[,6]),as.numeric(complTimeRJ[,4]),
          as.numeric(complTimeRJ[,7]),as.numeric(complTimeRJ[,5]),as.numeric(complTimeRJ[,8]), 
          names=z, yaxt='n', ylim=c(0,maxRJ),ylab='job response time (s)', cex.lab=3,lwd=2,cex=2.2,cex.axis=2.5)
  #axis(2, at=c(0,50,100,150), labels=c(0,50,100,150),mgp=c(4,1,0), cex.lab=2,cex=2.2,cex.axis=2.2)
  if (numRacks == 4) {
    axis(2, at=c(0,100,200,300), labels=c(0,100,200,300),mgp=c(4,1,0), cex.lab=2,cex=2.2,cex.axis=2.2)
  } else {
    axis(2, at=c(0,50,100,150), labels=c(0,50,100,150),mgp=c(4,1,0), cex.lab=2,cex=2.2,cex.axis=2.2)
  }
  title(paste("regular jobs, TS1, ", numRacks, "racks, ", SHJRatio, "%SHJ, ", "lambda=", lambda, sep=""))
  
  z <- c("Original\n2", "Original\n3", "75%\n2", "75%\n3", "100%\n2", "100%\n3")
  boxplot(as.numeric(complTimeAll[,3]),as.numeric(complTimeAll[,6]),as.numeric(complTimeAll[,4]),
          as.numeric(complTimeAll[,7]),as.numeric(complTimeAll[,5]),as.numeric(complTimeAll[,8]), 
          names=z, yaxt='n', ylim=c(0,maxSHJ), ylab='job response time (s)', cex.lab=3,lwd=2,cex=2.2,cex.axis=2.5)
  axis(2, at=c(0,200,400,600,800,1000), labels=c(0,200,400,600,800,1000),mgp=c(4,1,0), cex.lab=2,cex=2.2,cex.axis=2.2)
  title(paste("all jobs, TS1, ", numRacks, "racks, ", SHJRatio, "%SHJ, ", "lambda=", lambda, sep=""))
}


complTimeTS <- function(numRacks, SHJRatio, lambda, index1, index2) {
  directory <- paste("/Users/sherry/Box Sync/Hadoop-simulation-results/new-traces/FB-M1/",numRacks,"racks/",sep="")
  trace <- paste("numSHJ", 40, "_SHJRatio", SHJRatio, "%_arrivalRate", lambda, sep = "") 
  jobs <- read.csv(file=paste(directory, trace, "-jobComplTime", index1, ".csv", sep = "") , header=TRUE, sep=",")
  jobs <- as.matrix(jobs)
  
  directory <- paste("/Users/sherry/Box Sync/Hadoop-simulation-results/new-traces/FB-M1/3replicas/",numRacks,"racks/",sep="")
  jobs3r <- read.csv(file=paste(directory, trace, "-jobComplTime", index2, ".csv", sep = "") , header=TRUE, sep=",")
  jobs3r <- as.matrix(jobs3r)
  
  numSHJs <- 0
  numRJs <- 0
  for(i in 1:nrow(jobs)) {
    if(jobs[i,2] == "RJ") {
      numRJs <- numRJs + 1
    }else {
      numSHJs <- numSHJs + 1
    }
  }
  
  SHJcomplTime <- matrix(nrow=numSHJs, ncol=8)
  RJcomplTime <- matrix(nrow=numRJs, ncol=8)
  count1 <-1
  count2 <-1
  for(i in 1:nrow(jobs)) {
    if(jobs[i,2] == "RJ") {
      RJcomplTime[count1,1:5] <- jobs[i,1:5]
      RJcomplTime[count1,6:8] <- jobs3r[i,3:5]
      count1 <- count1+1
    }else {
      SHJcomplTime[count2,1:5] <- jobs[i,1:5]
      SHJcomplTime[count2,6:8] <- jobs3r[i,3:5]
      count2 <- count2+1
    }
  }
  
  return (list(SHJcomplTime, RJcomplTime, cbind(jobs[,1:5], jobs3r[,3:5])))
}



# boxplotTS(4,20,0.3,4,1)
boxplotTS(12,20,1.5,4,1)


