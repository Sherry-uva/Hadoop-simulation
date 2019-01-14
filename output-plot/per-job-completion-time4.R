rm(list=ls(all=TRUE)) 

jobInfo <- read.csv(file="/Users/sherry/Box Sync/Hadoop-simulation-hybrid/logs/duration1000sec_SHJRatio10%_arrivalRate0.05-cpuUtil1.csv", header=TRUE, sep=",")
jobInfo <- as.matrix(jobInfo)

jobId <- jobInfo[,1]
jobType <- jobInfo[,2]
complTime_hybrid_25 <- jobInfo[,3]
cpu_hybrid_25 <- jobInfo[,4]

jobInfo <- read.csv(file="/Users/sherry/Box Sync/Hadoop-simulation-hybrid/logs-0%/duration1000sec_SHJRatio10%_arrivalRate0.05-cpuUtil1.csv", header=TRUE, sep=",")
jobInfo <- as.matrix(jobInfo)

complTime_hybrid_0 <- jobInfo[,3]
cpu_hybrid_0 <- jobInfo[,4]

jobInfo <- read.csv(file="/Users/sherry/Box Sync/Hadoop-simulation-electrical/logs/duration1000sec_SHJRatio10%_arrivalRate0.05-cpuUtil1.csv", header=TRUE, sep=",")
jobInfo <- as.matrix(jobInfo)

complTime_electrical <- jobInfo[,3]
cpu_electrical <- jobInfo[,4]

plot(range(c(0,length(jobId))), range(c(0, 410)), axes=FALSE, type="n",xlab='job indices',ylab='job completion time (s)',cex.lab=1.2)
box()
axis(2, at=(0:4)*100,cex.axis=1.5)
axis(1, at=(0:10)*10,cex.axis=1)

lines(jobId, complTime_hybrid_25, col='red', type = "b", pch=21)
lines(jobId, complTime_hybrid_0, col='blue', type = "b", pch=23)
lines(jobId, complTime_electrical, col='green', type = "b", pch=24)

for (id in 1:length(jobId)) {
  if (jobType[id] == "SHJ") {
    points(id-1, complTime_hybrid_25[id], pch=16, col='red')
    points(id-1, complTime_hybrid_0[id], pch=18, col='blue')
    points(id-1, complTime_electrical[id], pch=17, col='green')
  }
}

# CPU utilization
plot(range(c(0,length(jobId))), range(c(0, 1)), axes=FALSE, type="n",xlab='job indices',ylab='av. CPU utilization of reduce cntrs',cex.lab=1.2)
box()
axis(2, at=(0:5)*0.2,cex.axis=1.5)
axis(1, at=(0:10)*10,cex.axis=1)

lines(jobId, cpu_hybrid_25, col='red', type = "b", pch=21)
lines(jobId, cpu_hybrid_0, col='blue', type = "b", pch=23)
lines(jobId, cpu_electrical, col='green', type = "b", pch=24)

for (id in 1:length(jobId)) {
  if (jobType[id] == "SHJ") {
    points(id-1, cpu_hybrid_25[id], pch=16, col='red')
    points(id-1, cpu_hybrid_0[id], pch=18, col='blue')
    points(id-1, cpu_electrical[id], pch=17, col='green')
  }
}



