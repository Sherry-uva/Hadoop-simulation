rm(list=ls(all=TRUE)) 

fairness<-function(SHJRatio, lambda, index) {
  trace <- paste("numSHJ", 40, "_SHJRatio", SHJRatio, "%_arrivalRate", lambda, sep = "") 
  directory <- "/Users/sherry/Box Sync/Hadoop-simulation-results/new-traces/FB-M1/12racks/"
  data <- read.csv(file=paste(directory, "hybrid-25%/", trace, "-fairness", index, ".csv", sep = ""), header=TRUE, sep=",") 
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
  
  data <- read.csv(file=paste(directory, "hybrid-0%/", trace, "-fairness", index, ".csv", sep = ""), header=TRUE, sep=",") 
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
  
  data <- read.csv(file=paste(directory, "electrical/", trace, "-fairness", index, ".csv", sep = ""), header=TRUE, sep=",") 
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
  
  # plot(ecdf(hybrid25Fair), col='blue', xlab='Overall unfairness',ylab='CDF', main=trace)
  # lines(ecdf(hybrid0Fair), col='red')
  # lines(ecdf(electricalFair), col='green')
  
  # par(mfrow=c(1,1), mar=c(5, 4, 4, 4) + 0.1)
  # maxUnfair<-max(hybrid25Fair, hybrid0Fair, electricalFair)
  # plot(range(c(0,totalTime)), range(c(0, maxUnfair)), axes=FALSE, type="n",xlab='',ylab='',cex.lab=1.2, main=paste("12racks: ", trace, sep=''))
  # mtext("Time (in seconds)", side=1, col="black",line=2, cex=1.2)
  # mtext("Overall unfainess", side=2, line=2.5, cex=1.2)
  # box()
  # axis(2, ylim=c(0,maxUnfair),las=1)
  # axis(1, at=(0:7)*2000,cex.axis=1)
  # 
  # colors = c('blue', 'red', 'green')
  # lines(hybrid25Fair, col=colors[1])
  # lines(hybrid0Fair, col=colors[2])
  # lines(electricalFair, col=colors[3])
  
  par(mfrow=c(2,1), oma = c(5,1,0,0) + 0.1, mar = c(0,5,3,1) + 0.1, mai = c(1, 1, 0.5, 0.1), mgp = c(3, 1, 0))
  maxUnfair<-max(as.numeric(hybrid25PerJobFair[,3]), as.numeric(hybrid0PerJobFair[,3]), as.numeric(electricalPerJobFair[,3]))
  a <- as.numeric(hybrid25PerJobFair[,3])
  a <- ifelse(a > 2, a-19, a)
  # plot(a, ylim=c(0,maxUnfair), col='blue', type = "b", xlab='jobId',ylab='per-job unfairness',cex.lab=1.8, cex.axis=2, main=paste("12racks: ", trace, sep=''))
  plot(a[100:150], xaxt="n", yaxt='n',ylim=c(0,2.5), col='blue', type = "b", xlab='job indices',ylab='per-job unfairness',cex.lab=1.7,lwd=3,cex=2,cex.axis=1.2, main=paste("12racks: ", trace, sep=''))
  axis(2,at=c(0,0.5,1,1.5,2,2.5), labels=c(0,0.5,1,1.5,21,21.5),cex.axis=1.5)
  axis(1,at=c(1,11,21,31,41,51), labels=c(10:15)*10,cex.axis=1.5)
  axis.break(2,1.7,style="slash")
  segments(1,2,5,2, col='green', lwd=4,cex=1.5)
  points(3,2, pch=1, col='green',cex=2,lwd=3)
  text(14, 2, "Original Hadoop",cex=1.5)
  segments(1,1.6,5,1.6, col='blue', lwd=4,cex=1.5)
  points(3,1.6, pch=1, col='blue',cex=2,lwd=4)
  text(12, 1.6, "HHN-75%",cex=1.5)
  # lines(as.numeric(hybrid0PerJobFair[,3]), col='red', type = "b")
  lines(as.numeric(electricalPerJobFair[100:150,3]), col='green', type = "b", lwd=3,cex=1.5)
  print(sum(as.numeric(hybrid25PerJobFair[,3])))
  print(sum(as.numeric(hybrid0PerJobFair[,3])))
  print(sum(as.numeric(electricalPerJobFair[,3])))
  for(i in 100:150) {
    if(hybrid25PerJobFair[i,2] =="SHJ") {
      b <- as.numeric(hybrid25PerJobFair[i,3])
      b <- ifelse(b > 2, b-19, b)
      points(i-99,b, pch=16, col='blue', cex=2)
      # points(i,as.numeric(hybrid0PerJobFair[i,3]), pch=16, col='red')
      points(i-99,as.numeric(electricalPerJobFair[i,3]), pch=16, col='green',cex=2)
    }
  }
}

complTime <- function(SHJRatio, lambda, index) {
  directory <- "/Users/sherry/Box Sync/Hadoop-simulation-results/new-traces/FB-M1/12racks/"
  trace <- paste("numSHJ", 40, "_SHJRatio", SHJRatio, "%_arrivalRate", lambda, sep = "") 
  jobs <- read.csv(file=paste(directory, trace, "-jobComplTime", index, ".csv", sep = "") , header=TRUE, sep=",")
  jobs <- as.matrix(jobs)
  maxSHJComplTime <- max(as.numeric(jobs[,3]), as.numeric(jobs[,4]), as.numeric(jobs[,5]))
  # plot(jobs[,3], xlim=c(100,200), ylim=c(0, maxSHJComplTime), col='green', type = "b", xlab='jobId',ylab='job completion time (s)',cex.lab=1.8, cex.axis=1.8)
  # lines(jobs[,4], col='blue', type = "b")
  # # lines(jobs[,5], col='red', type = "b")
  # for(i in 1:nrow(jobs)) {
  #   if(jobs[i,2] =="SHJ") {
  #     points(i,as.numeric(jobs[i,3]), pch=16, col='green')
  #     points(i,as.numeric(jobs[i,4]), pch=16, col='blue')
  #     # points(i,as.numeric(jobs[i,5]), pch=16, col='red')
  #   }
  # }
  maxDiff<-max(as.numeric(jobs[,3])-as.numeric(jobs[,4]))
  minDiff<-min(as.numeric(jobs[,3])-as.numeric(jobs[,4]))
  print(as.numeric(jobs[,3])-as.numeric(jobs[,4]))
  # plot(as.numeric(jobs[,3])-as.numeric(jobs[,4]), xlim=c(100,200), ylim=c(minDiff, maxDiff), col='blue', type = "b", xlab='job indices',ylab='compl. time differences (s)',cex.lab=1.75, cex.axis=2)
  a <- as.numeric(jobs[,3])-as.numeric(jobs[,4])
  a <- ifelse(a < -50, a+250, a)
  plot(a[100:150], xaxt="n", yaxt="n", ylim=c(-80,20), col='blue', type = "b", xlab='job indices',ylab='job-response-time difference (s)',cex.lab=1.7, cex.axis=1.2, lwd=3,cex=2)
  axis(2,at=c(-1:4)*-20, labels=c(-20,0,20,40,310,330)*-1,cex.axis=1.4)
  axis(1,at=c(1,11,21,31,41,51), labels=c(10:15)*10,cex.axis=1.5)
  axis.break(2,-50,style="slash")
  for(i in 100:150) {
    if(jobs[i,2] =="SHJ") {
      b <- as.numeric(jobs[i,3])-as.numeric(jobs[i,4])
      b <- ifelse(b < -50, b+250, b)
      points(i-99,b, pch=16, col='blue',cex=2)
    }
  }
  points(2.3,-58, pch=1, col='blue',cex=2,lwd=4)
  text(10, -58, "regular jobs",cex=1.5)
  points(2.3,-75, pch=16, col='blue',cex=2.5,lwd=4)
  text(12.9, -75, "shuffle-heavy jobs",cex=1.5)
}

fairness(20, 0.6, 4)
complTime(20, 0.6, 4)


