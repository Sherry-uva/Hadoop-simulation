rm(list=ls(all=TRUE)) 

fairness<-function(SHJRatio, lambda, index) {
  trace <- paste("numSHJ", 60, "_SHJRatio", SHJRatio, "%_arrivalRate", lambda, sep = "") 
  data <- read.csv(file=paste("/Users/sherry/Box Sync/Hadoop-simulation-results/new-traces/larger-RJs-ideal-optical/12racks/hybrid-25%/", trace, "-fairness", index, ".csv", sep = ""), header=TRUE, sep=",") 
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
  
  data <- read.csv(file=paste("/Users/sherry/Box Sync/Hadoop-simulation-results/new-traces/larger-RJs-ideal-optical/12racks/hybrid-0%/", trace, "-fairness", index, ".csv", sep = ""), header=TRUE, sep=",") 
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
  
  data <- read.csv(file=paste("/Users/sherry/Box Sync/Hadoop-simulation-results/new-traces/larger-RJs-ideal-optical/12racks/electrical/", trace, "-fairness", index, ".csv", sep = ""), header=TRUE, sep=",") 
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
  
  par(mfrow=c(3,1), oma = c(2,1,0,1) + 0.1, mar = c(0,4,2,0) + 0.1)
  maxUnfair<-max(as.numeric(hybrid25PerJobFair[,3]), as.numeric(hybrid0PerJobFair[,3]), as.numeric(electricalPerJobFair[,3]))
  plot(as.numeric(hybrid25PerJobFair[,3]), ylim=c(0,maxUnfair), col='blue', type = "b", xlab='jobId',ylab='per job unfairness', main=paste("12racks: ", trace, sep=''))
  lines(as.numeric(hybrid0PerJobFair[,3]), col='red', type = "b")
  lines(as.numeric(electricalPerJobFair[,3]), col='green', type = "b")
  for(i in 1:nrow(hybrid25PerJobFair)) {
    if(hybrid25PerJobFair[i,2] =="SHJ") {
      points(i,as.numeric(hybrid25PerJobFair[i,3]), pch=16, col='blue')
      points(i,as.numeric(hybrid0PerJobFair[i,3]), pch=16, col='red')
      points(i,as.numeric(electricalPerJobFair[i,3]), pch=16, col='green')
    }
  }
}

complTime <- function(SHJRatio, lambda, index) {
  directory <- "/Users/sherry/Box Sync/Hadoop-simulation-results/new-traces/larger-RJs-ideal-optical/12racks/"
  trace <- paste("numSHJ", 60, "_SHJRatio", SHJRatio, "%_arrivalRate", lambda, sep = "") 
  jobs <- read.csv(file=paste(directory, trace, "-jobComplTime", index, ".csv", sep = "") , header=TRUE, sep=",")
  jobs <- as.matrix(jobs)
  maxSHJComplTime <- max(as.numeric(jobs[,3]), as.numeric(jobs[,4]), as.numeric(jobs[,5]))
  plot(jobs[,3], ylim=c(0, maxSHJComplTime), col='green', type = "b", xlab='jobId',ylab='job completion time (s)')
  lines(jobs[,4], col='blue', type = "b")
  lines(jobs[,5], col='red', type = "b")
  for(i in 1:nrow(jobs)) {
    if(jobs[i,2] =="SHJ") {
      points(i,as.numeric(jobs[i,3]), pch=16, col='green')
      points(i,as.numeric(jobs[i,4]), pch=16, col='blue')
      points(i,as.numeric(jobs[i,5]), pch=16, col='red')
    }
  }
  maxDiff<-max(as.numeric(jobs[,3])-as.numeric(jobs[,4]), as.numeric(jobs[,3])-as.numeric(jobs[,5]))
  minDiff<-min(as.numeric(jobs[,3])-as.numeric(jobs[,4]), as.numeric(jobs[,3])-as.numeric(jobs[,5]))
  plot(as.numeric(jobs[,3])-as.numeric(jobs[,4]), ylim=c(minDiff, maxDiff), col='blue', type = "b", xlab='jobId',ylab='job completion time difference (s)')
  lines(as.numeric(jobs[,3])-as.numeric(jobs[,5]), col='red', type = "b")
  for(i in 1:nrow(jobs)) {
    if(jobs[i,2] =="SHJ") {
      points(i,as.numeric(jobs[i,3])-as.numeric(jobs[i,4]), pch=16, col='blue')
      points(i,as.numeric(jobs[i,3])-as.numeric(jobs[i,5]), pch=16, col='red')
    }
  }
}

fairness(20, 0.6, 1)
complTime(20, 0.6, 1)


