rm(list=ls(all=TRUE)) 

#cntrInfo <- read.csv(file="/Users/sherry/Box Sync/Hadoop-simulation-electrical-Arch-small/logs/numTasks.csv", header=TRUE, sep=",")
cntrInfo <- read.csv(file="/Users/sherry/Box Sync/Hadoop-simulation-small/logs/numTasks.csv", header=TRUE, sep=",")
cntrInfo <- as.matrix(cntrInfo)

par(mar=c(5, 4, 4, 4) + 0.1)
plot(range(c(0,215)), range(c(0, 16)), axes=FALSE, type="n",xlab='',ylab='',cex.lab=1.2)
mtext("Time (in seconds)", side=1, col="black",line=2, cex=1.2)
mtext("Number of containers allocated per job", side=2, col="red",line=2.5, cex=1.2)
box()
#axis(2, at=(0:3)*5,cex.axis=1.5)
axis(2, ylim=c(0,15), col="red",col.axis="red",las=1)
axis(1, at=(0:10)*50,cex.axis=1)

colors = c('deeppink', 'green', 'red', 'blue', 'cyan')

numJobs = ncol(cntrInfo)-2
for (job in 1:numJobs) {
  lines(cntrInfo[,job], col=colors[job])
  par(new=TRUE)
}

par(new=TRUE)
## Plot the second plot and put axis scale on right
plot(cntrInfo[,numJobs+2], xlab="", ylab="", ylim=c(0,0.9), axes=FALSE, type="b", col="black")
mtext("Fairness", side=4, col="black", line=2.5, cex=1.2) 
axis(4, ylim=c(0,0.9), col="black",col.axis="black",las=1)






