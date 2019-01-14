IFS=, read -r jobId numMaps numReduces startTime finishTime duration < <(tail -n1 jobCompletionTime-electrical.csv)
lastTime=${finishTime%%.*}
echo $lastTime
python log-parser.py $lastTime
