rm(list=ls(all=TRUE)) 

fairness<-function(SHJRatio, lambda, index) {
  trace <- paste("numSHJ", 60, "_SHJRatio", SHJRatio, "%_arrivalRate", lambda, sep = "") 
  data <- read.csv(file=paste("/Users/sherry/Box Sync/Hadoop-simulation-results/new-traces/larger-RJs-ideal-optical/4racks/hybrid-25%/", trace, "-fairness", index, ".csv", sep = ""), header=TRUE, sep=",") 
  data <- as.matrix(data)
  for(i in 1:nrow(data)) {
    if(substring(data[i,1],1,3)!="job") {
      break
    }
  }
  hybrid25PerJobFair<-data[1:(i-1),]
  hybrid25Fair<-data[i:nrow(data),]
  hybrid25Fair<-as.numeric(as.vector(t(hybrid25Fair)))
  hybrid25Fair<-hybrid25Fair[!is.na(hybrid25Fair)]
  
  data <- read.csv(file=paste("/Users/sherry/Box Sync/Hadoop-simulation-results/new-traces/larger-RJs-ideal-optical/4racks/hybrid-0%/", trace, "-fairness", index, ".csv", sep = ""), header=TRUE, sep=",") 
  data <- as.matrix(data)
  for(i in 1:nrow(data)) {
    if(substring(data[i,1],1,3)!="job") {
      break
    }
  }
  hybrid0PerJobFair<-data[1:(i-1),]
  hybrid0Fair<-data[i:nrow(data),]
  hybrid0Fair<-as.numeric(as.vector(t(hybrid0Fair)))
  hybrid0Fair<-hybrid0Fair[!is.na(hybrid0Fair)]
  
  data <- read.csv(file=paste("/Users/sherry/Box Sync/Hadoop-simulation-results/new-traces/larger-RJs-ideal-optical/4racks/electrical/", trace, "-fairness", index, ".csv", sep = ""), header=TRUE, sep=",") 
  data <- as.matrix(data)
  for(i in 1:nrow(data)) {
    if(substring(data[i,1],1,3)!="job") {
      break
    }
  }
  electricalPerJobFair<-data[1:(i-1),]
  electricalFair<-data[i:nrow(data),]
  electricalFair<-as.numeric(as.vector(t(electricalFair)))
  electricalFair<-electricalFair[!is.na(electricalFair)]
  
  totalTime<-min(length(hybrid25Fair), length(hybrid0Fair), length(electricalFair))
  hybrid25Fair<-hybrid25Fair[1:totalTime]
  hybrid0Fair<-hybrid0Fair[1:totalTime]
  electricalFair<-electricalFair[1:totalTime]
  
  par(mfrow=c(2,1), oma = c(5,1,0,0) + 0.1, mar = c(0,5,3,1) + 0.1, mai = c(1, 1, 0.5, 0.1))
  maxUnfair<-max(as.numeric(hybrid25PerJobFair[,3]), as.numeric(hybrid0PerJobFair[,3]), as.numeric(electricalPerJobFair[,3]))
  plot(as.numeric(hybrid25PerJobFair[1:50,3]), ylim=c(0,maxUnfair), xlim=c(0,50), col='blue', type = "b", xlab='job indices',ylab='per-job unfairness',cex.lab=2, lwd=3,cex=2,cex.axis=2, main=paste("4racks: ", trace, sep=''))
  lines(as.numeric(hybrid0PerJobFair[1:50,3]), col='red', type = "b",lwd=3,cex=2)
  lines(as.numeric(electricalPerJobFair[1:50,3]), col='green', type = "b",lwd=3,cex=2)
  for(i in 1:nrow(hybrid25PerJobFair)) {
    if(hybrid25PerJobFair[i,2] =="SHJ") {
      points(i,as.numeric(hybrid25PerJobFair[i,3]), pch=16, col='blue',lwd=3,cex=2)
      points(i,as.numeric(hybrid0PerJobFair[i,3]), pch=16, col='red',lwd=3,cex=2)
      points(i,as.numeric(electricalPerJobFair[i,3]), pch=16, col='green',lwd=3,cex=2)
    }
  }
  segments(0,5,2,5, col='green', lwd=3,cex=1.5)
  text(8.2, 5, "Original Hadoop",cex=1.8)
  segments(0,4.3,2,4.3, col='blue', lwd=3,cex=1.5)
  text(6.5, 4.3, "HHN-75%",cex=1.8)
  segments(0,3.6,2,3.6, col='red', lwd=3,cex=1.5)
  text(6.8,3.6, "HHN-100%",cex=1.8)
  points(0,2.9, pch=1, col='green',lwd=3,cex=2)
  points(1,2.9, pch=1, col='blue',lwd=3,cex=2)
  points(2,2.9, pch=1, col='red',lwd=3,cex=2)
  text(7,2.9, "regular jobs",cex=1.8)
  points(0,2.2, pch=16, col='green',lwd=3,cex=2.5)
  points(1,2.2, pch=16, col='blue',lwd=3,cex=2.5)
  points(2,2.2, pch=16, col='red',lwd=3,cex=2.5)
  text(9.25,2.2, "shuffle-heavy jobs",cex=1.8)
}

complTime <- function(SHJRatio, lambda, index) {
  directory <- "/Users/sherry/Box Sync/Hadoop-simulation-results/new-traces/larger-RJs-ideal-optical/4racks/"
  trace <- paste("numSHJ", 60, "_SHJRatio", SHJRatio, "%_arrivalRate", lambda, sep = "") 
  jobs <- read.csv(file=paste(directory, trace, "-jobComplTime", index, ".csv", sep = "") , header=TRUE, sep=",")
  jobs <- as.matrix(jobs)
  maxSHJComplTime <- max(as.numeric(jobs[,3]), as.numeric(jobs[,4]), as.numeric(jobs[,5]))
  # plot(jobs[,3], ylim=c(0, maxSHJComplTime), col='green', type = "b", xlab='jobId',ylab='job completion time (s)')
  # lines(jobs[,4], col='blue', type = "b")
  # lines(jobs[,5], col='red', type = "b")
  # for(i in 1:nrow(jobs)) {
  #   if(jobs[i,2] =="SHJ") {
  #     points(i,as.numeric(jobs[i,3]), pch=16, col='green')
  #     points(i,as.numeric(jobs[i,4]), pch=16, col='blue')
  #     points(i,as.numeric(jobs[i,5]), pch=16, col='red')
  #   }
  # }
  maxDiff<-max(as.numeric(jobs[,3])-as.numeric(jobs[,4]), as.numeric(jobs[,3])-as.numeric(jobs[,5]))
  minDiff<-min(as.numeric(jobs[,3])-as.numeric(jobs[,4]), as.numeric(jobs[,3])-as.numeric(jobs[,5]))
  plot(as.numeric(jobs[1:50,3])-as.numeric(jobs[1:50,4]), ylim=c(minDiff, 30), xlim=c(0,50), col='blue', type = "b", xlab='job indices',ylab='job-response-time difference (s)', cex.lab=2, lwd=3,cex=2,cex.axis=2)
  lines(as.numeric(jobs[1:50,3])-as.numeric(jobs[1:50,5]), col='red', type = "b",lwd=3,cex=2)
  for(i in 1:nrow(jobs)) {
    if(jobs[i,2] =="SHJ") {
      points(i,as.numeric(jobs[i,3])-as.numeric(jobs[i,4]), pch=16, col='blue',lwd=3,cex=2)
      points(i,as.numeric(jobs[i,3])-as.numeric(jobs[i,5]), pch=16, col='red',lwd=3,cex=2)
    }
  }
  segments(-1,-80,1,-80, col='blue', lwd=3,cex=1.5)
  text(9.6, -80, "Original Hadoop - HHN-75%",cex=1.5)
  segments(-1,-100,1,-100, col='red', lwd=3,cex=1.5)
  text(9.8, -100, "Original Hadoop - HHN-100%",cex=1.5)
}

fairness(20, 0.3, 1)
complTime(20, 0.3, 1)


