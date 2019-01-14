rm(list=ls(all=TRUE)) 

systemSize<-function(SHJRatio1, lambda1, index1, SHJRatio2, lambda2, index2) {
  fairness1 <- fairness(4, SHJRatio1, lambda1, index1)
  fairnessSHJ1 <- matrix(unlist(fairness1[1]), ncol = 5, byrow = FALSE)
  fairnessRJ1 <- matrix(unlist(fairness1[2]), ncol = 5, byrow = FALSE)
  fairnessAll1 <- matrix(unlist(fairness1[3]), ncol = 5, byrow = FALSE)
  fairness2 <- fairness(12, SHJRatio2, lambda2, index2)
  fairnessSHJ2 <- matrix(unlist(fairness2[1]), ncol = 5, byrow = FALSE)
  fairnessRJ2 <- matrix(unlist(fairness2[2]), ncol = 5, byrow = FALSE)
  fairnessAll2 <- matrix(unlist(fairness2[3]), ncol = 5, byrow = FALSE)
  
  par(mfrow=c(1,1), oma = c(2,2,1,1) + 0.1, mar = c(4,5,2,0) + 0.1)
  maxUnfair <- max(as.numeric(fairnessSHJ1[,c(3,5)]), as.numeric(fairnessSHJ2[,c(3,5)]))
  plot(ecdf(as.numeric(fairnessSHJ1[,3])), xlim=c(0,maxUnfair), mgp=c(2.7,1,0), col='blue', xlab='per-job unfairness',ylab='CDF', main="unfairness of SHJs", cex.lab=2,lwd=3.5,cex=2.4,cex.axis=2)
  lines(ecdf(as.numeric(fairnessSHJ2[,3])), col='red', cex.lab=1.8,lwd=3,cex=2)
  lines(ecdf(as.numeric(fairnessSHJ1[,5])), col='green', cex.lab=1.8,lwd=3,cex=2)
  lines(ecdf(as.numeric(fairnessSHJ2[,5])), col='cyan', cex.lab=1.8,lwd=3,cex=2)
  segments(1.3,0.25,1.4,0.25, col='blue', lwd=4)
  points(1.35,0.25, pch=16, col='blue',lwd=3,cex=2.5)
  text(1.8, 0.25, "4racks, OSM-25%",cex=2)
  segments(1.3,0.18,1.4,0.18, col='green', lwd=4)
  points(1.35,0.18, pch=16, col='green',lwd=3,cex=2.5)
  text(1.77,0.18, "4racks, electrical",cex=2)
  segments(1.3,0.11,1.4,0.11, col='red', lwd=4)
  points(1.35,0.11, pch=16, col='red',lwd=3,cex=2.5)
  text(1.8, 0.11, "12racks, OSM-25%",cex=2)
  segments(1.3,0.04,1.4,0.04, col='cyan', lwd=4)
  points(1.35,0.04, pch=16, col='cyan',lwd=3,cex=2.5)
  text(1.77, 0.04, "12racks, electrical",cex=2)

  maxUnfair <- max(as.numeric(fairnessRJ1[,c(3,5)]), as.numeric(fairnessRJ2[,c(3,5)]))
  plot(ecdf(as.numeric(fairnessRJ1[,3])), xlim=c(0,2), mgp=c(2.7,1,0), col='blue', xlab='per-job unfairness',ylab='CDF', main="unfairness of RJs", cex.lab=2,lwd=3.5,cex=2.4,cex.axis=2)
  lines(ecdf(as.numeric(fairnessRJ2[,3])), col='red', cex.lab=1.8,lwd=3,cex=2)
  lines(ecdf(as.numeric(fairnessRJ1[,5])), col='green', cex.lab=1.8,lwd=3,cex=2)
  lines(ecdf(as.numeric(fairnessRJ2[,5])), col='cyan', cex.lab=1.8,lwd=3,cex=2)
  segments(1.3,0.25,1.4,0.25, col='blue', lwd=4)
  points(1.35,0.25, pch=16, col='blue',lwd=3,cex=2.5)
  text(1.73, 0.25, "4racks, OSM-25%",cex=2)
  segments(1.3,0.18,1.4,0.18, col='green', lwd=4)
  points(1.35,0.18, pch=16, col='green',lwd=3,cex=2.5)
  text(1.7,0.18, "4racks, electrical",cex=2)
  segments(1.3,0.11,1.4,0.11, col='red', lwd=4)
  points(1.35,0.11, pch=16, col='red',lwd=3,cex=2.5)
  text(1.73, 0.11, "12racks, OSM-25%",cex=2)
  segments(1.3,0.04,1.4,0.04, col='cyan', lwd=4)
  points(1.35,0.04, pch=16, col='cyan',lwd=3,cex=2.5)
  text(1.7, 0.04, "12racks, electrical",cex=2)
  
  # maxUnfair <- max(as.numeric(fairnessAll1[,c(3,5)]), as.numeric(fairnessAll2[,c(3,5)]))
  # plot(ecdf(as.numeric(fairnessAll1[,3])), xlim=c(0,maxUnfair), mgp=c(2.7,1,0), col='blue', xlab='per-job unfairness',ylab='CDF', main="unfairness of all jobs", cex.lab=1.8,lwd=3,cex=2,cex.axis=2)
  # lines(ecdf(as.numeric(fairnessAll2[,3])), col='red', cex.lab=1.8,lwd=3,cex=2)
  # lines(ecdf(as.numeric(fairnessAll1[,5])), col='green', cex.lab=1.8,lwd=3,cex=2)
  # lines(ecdf(as.numeric(fairnessAll2[,5])), col='cyan', cex.lab=1.8,lwd=3,cex=2)
  # 
  complTime1 <- complTime(4, SHJRatio1, lambda1, index1)
  complTimeSHJ1 <- matrix(unlist(complTime1[1]), ncol = 5, byrow = FALSE)
  complTimeRJ1 <- matrix(unlist(complTime1[2]), ncol = 5, byrow = FALSE)
  complTimeAll1 <- matrix(unlist(complTime1[3]), ncol = 5, byrow = FALSE)
  complTime2 <- complTime(12, SHJRatio2, lambda2, index2)
  complTimeSHJ2 <- matrix(unlist(complTime2[1]), ncol = 5, byrow = FALSE)
  complTimeRJ2 <- matrix(unlist(complTime2[2]), ncol = 5, byrow = FALSE)
  complTimeAll2 <- matrix(unlist(complTime2[3]), ncol = 5, byrow = FALSE)

  maxComplTime <- max(as.numeric(complTimeSHJ1[,3:4]), as.numeric(complTimeSHJ2[,3:4]))
  plot(ecdf(as.numeric(complTimeSHJ1[,4])), xlim=c(0,maxComplTime), mgp=c(2.7,1,0), col='blue', xlab='job completion time (s)',ylab='CDF', main="job completion time of SHJs", cex.lab=2,lwd=3.5,cex=2.4,cex.axis=2)
  lines(ecdf(as.numeric(complTimeSHJ2[,4])), col='red', cex.lab=1.8,lwd=3,cex=2)
  lines(ecdf(as.numeric(complTimeSHJ1[,3])), col='green', cex.lab=1.8,lwd=3,cex=2)
  lines(ecdf(as.numeric(complTimeSHJ2[,3])), col='cyan', cex.lab=1.8,lwd=3,cex=2)
  segments(200,0.25,220,0.25, col='blue', lwd=4)
  points(210,0.25, pch=16, col='blue',lwd=3,cex=2.5)
  text(285, 0.25, "4racks, Hybrid-25%",cex=2)
  segments(200,0.18,220,0.18, col='green', lwd=4)
  points(210,0.18, pch=16, col='green',lwd=3,cex=2.5)
  text(280,0.18, "4racks, EPS-only",cex=2)
  segments(200,0.11,220,0.11, col='red', lwd=4)
  points(210,0.11, pch=16, col='red',lwd=3,cex=2.5)
  text(285, 0.11, "12racks, Hybrid-25%",cex=2)
  segments(200,0.04,220,0.04, col='cyan', lwd=4)
  points(210,0.04, pch=16, col='cyan',lwd=3,cex=2.5)
  text(280, 0.04, "12racks, EPS-only",cex=2)
  
  maxComplTime <- max(as.numeric(complTimeRJ1[,3:4]), as.numeric(complTimeRJ2[,3:4]))
  plot(ecdf(as.numeric(complTimeRJ1[,4])), xlim=c(0,maxComplTime), mgp=c(2.7,1,0), col='blue', xlab='job completion time (s)',ylab='CDF', main="job completion time of RJs", cex.lab=2,lwd=3.5,cex=2.4,cex.axis=2)
  lines(ecdf(as.numeric(complTimeRJ2[,4])), col='red', cex.lab=1.8,lwd=3,cex=2)
  lines(ecdf(as.numeric(complTimeRJ1[,3])), col='green', cex.lab=1.8,lwd=3,cex=2)
  lines(ecdf(as.numeric(complTimeRJ2[,3])), col='cyan', cex.lab=1.8,lwd=3,cex=2)
  segments(175,0.25,195,0.25, col='blue', lwd=4)
  points(185,0.25, pch=16, col='blue',lwd=3,cex=2.5)
  text(255, 0.25, "4racks, Hybrid-25%",cex=2)
  segments(175,0.18,195,0.18, col='green', lwd=4)
  points(185,0.18, pch=16, col='green',lwd=3,cex=2.5)
  text(250,0.18, "4racks, EPS-only",cex=2)
  segments(175,0.11,195,0.11, col='red', lwd=4)
  points(185,0.11, pch=16, col='red',lwd=3,cex=2.5)
  text(255, 0.11, "12racks, Hybrid-25%",cex=2)
  segments(175,0.04,195,0.04, col='cyan', lwd=4)
  points(185,0.04, pch=16, col='cyan',lwd=3,cex=2.5)
  text(250, 0.04, "12racks, EPS-only",cex=2)
  # segments(670,0.25,740,0.25, col='blue', lwd=4)
  # points(705,0.25, pch=16, col='blue',lwd=3,cex=2.5)
  # text(920, 0.25, "4racks, OSM-25%",cex=2)
  # segments(670,0.18,740,0.18, col='green', lwd=4)
  # points(705,0.18, pch=16, col='green',lwd=3,cex=2.5)
  # text(905,0.18, "4racks, electrical",cex=2)
  # segments(670,0.11,740,0.11, col='red', lwd=4)
  # points(705,0.11, pch=16, col='red',lwd=3,cex=2.5)
  # text(920, 0.11, "12racks, OSM-25%",cex=2)
  # segments(670,0.04,740,0.04, col='cyan', lwd=4)
  # points(705,0.04, pch=16, col='cyan',lwd=3,cex=2.5)
  # text(905, 0.04, "12racks, electrical",cex=2)

  # maxComplTime <- max(as.numeric(complTimeAll1[,3:4]), as.numeric(complTimeAll2[,3:4]))
  # plot(ecdf(as.numeric(complTimeAll1[,4])), xlim=c(0,maxComplTime), mgp=c(2.7,1,0), col='blue', xlab='job completion time (s)',ylab='CDF', main="job completion time of all jobs", cex.lab=1.8,lwd=3,cex=2,cex.axis=2)
  # lines(ecdf(as.numeric(complTimeAll2[,4])), col='red', cex.lab=1.8,lwd=3,cex=2)
  # lines(ecdf(as.numeric(complTimeAll1[,3])), col='green', cex.lab=1.8,lwd=3,cex=2)
  # lines(ecdf(as.numeric(complTimeAll2[,3])), col='cyan', cex.lab=1.8,lwd=3,cex=2)
}

fairness <- function(numRacks, SHJRatio, lambda, index) {
  trace <- paste("numSHJ", 60, "_SHJRatio", SHJRatio, "%_arrivalRate", lambda, sep = "") 
  data <- read.csv(file=paste("/Users/sherry/Box Sync/Hadoop-simulation-results/new-traces/larger-RJs-ideal-optical/", numRacks, "racks/hybrid-25%/", trace, "-fairness", index, ".csv", sep = ""), header=TRUE, sep=",") 
  data <- as.matrix(data)
  numSHJs <- 0
  numRJs <- 0
  for(i in 1:nrow(data)) {
    if(substring(data[i,1],1,3)!="job") {
      break
    }
    if(data[i,2] == "RJ") {
      numRJs <- numRJs + 1
    }else {
      numSHJs <- numSHJs + 1
    }
  }
  numJobs <- i-1
  perJobFairness <- matrix(nrow=numJobs, ncol=5)
  for(i in 1:numJobs) {
    perJobFairness[i,1:2] <- data[i,1:2]
    perJobFairness[i,3] <- as.numeric(data[i,3])
  }

  data <- read.csv(file=paste("/Users/sherry/Box Sync/Hadoop-simulation-results/new-traces/larger-RJs-ideal-optical/", numRacks, "racks/hybrid-0%/", trace, "-fairness", index, ".csv", sep = ""), header=TRUE, sep=",") 
  data <- as.matrix(data)
  for(i in 1:numJobs) {
    perJobFairness[i,4] <- as.numeric(data[i,3])
  }
  
  data <- read.csv(file=paste("/Users/sherry/Box Sync/Hadoop-simulation-results/new-traces/larger-RJs-ideal-optical/", numRacks, "racks/electrical/", trace, "-fairness", index, ".csv", sep = ""), header=TRUE, sep=",") 
  data <- as.matrix(data)
  for(i in 1:numJobs) {
    perJobFairness[i,5] <- as.numeric(data[i,3])
  }
  
  SHJFairness <- matrix(nrow=numSHJs, ncol=5)
  RJFairness <- matrix(nrow=numRJs, ncol=5)
  count1 <-1
  count2 <-1
  for(i in 1:numJobs) {
    if(data[i,2] == "RJ") {
      RJFairness[count1,] <- perJobFairness[i,]
      count1 <- count1+1
    }else {
      SHJFairness[count2,] <- perJobFairness[i,]
      count2 <- count2+1
    }
  }
  
  return (list(SHJFairness, RJFairness, perJobFairness[1:numJobs,]))
}

complTime <- function(numRacks, SHJRatio, lambda, index) {
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

systemSize(10, 0.3, 1, 10, 1.5, 1)


