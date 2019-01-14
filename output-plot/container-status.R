rm(list=ls(all=TRUE)) 

taskInfo <- read.csv(file="/Users/sherry/Box Sync/Hadoop-simulation-electrical/logs/duration500sec_SHJRatio5%_arrivalRate0.2-tasks1.csv", header=TRUE, sep=",")
taskInfo <- as.matrix(taskInfo)

plot(range(c(200,460)), range(c(0, 1279)),axes=FALSE, type="n",xlab='Time (in seconds)',ylab='cntrID',cex.lab=1.5)
box()
axis(2, at=(0:4)*300,cex.axis=1.5)
axis(1, at=(0:5)*50+200,cex.axis=1.5)

tmp1 <- c('deeppink', 'green', 'yellow', 'blue', 'cyan', 'red', 'palevioletred', 'violetred')
tmp2 <- rep('orange', 30)
colors <- c(tmp1, tmp2) 

for (index in 1:nrow(taskInfo)) {
  if (as.numeric(taskInfo[index,1])<30 || as.numeric(taskInfo[index,1])>58) {
    next
  }
  startTime <- as.numeric(taskInfo[index,5])
  cntr <- as.numeric(taskInfo[index,3])
  color = colors[as.numeric(taskInfo[index,1])-30+1]
  if (taskInfo[index,2] == 'm') {
    lineWidth = 0.5
    lineType = 1
    finishTime <- as.numeric(taskInfo[index,9])
    segments(startTime, cntr, finishTime, cntr, col=color, lwd=lineWidth, lty=lineType)
    segments(startTime, cntr, as.numeric(taskInfo[index,7]), cntr, col='black', lwd=2, lty=lineType)
  }
  else if (taskInfo[index,2] == 'r') {
    lineWidth = 0.5
    lineType = 1
    finishTime <- as.numeric(taskInfo[index,9])
    segments(startTime, cntr, finishTime, cntr, col=color, lwd=lineWidth, lty=lineType)
    segments(startTime, cntr, as.numeric(taskInfo[index,7]), cntr, col='black', lwd=3, lty=lineType)
  }
  else {
    lineWidth = 5
    lineType = 3
    finishTime <- as.numeric(taskInfo[index,7])
    segments(startTime, cntr, finishTime, cntr, col=color, lwd=lineWidth, lty=lineType)
  }
  points(finishTime, cntr, cex = 1.4)
}










