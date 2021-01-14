# Cloud_Computing-Assignment
Twitter Geoprocessing


### To Run the file:
#### Write .sh script and change node and cores accordingly
#!/bin/bash
#SBATCH --time=00:05:00
#SBATCH --nodes=2
#SBATCH --ntasks=8
#SBATCH --cpus-per-task=1
module load Python/3.4.3-goolf-2015a
#module load Java/1.8.0_71
#module load mpj/0.44
mpiexec -np 8 python3 geoProcessing.py
#javac -cp .:$MPJ_HOME/lib/mpj.jar HelloWorld.java
#mpjrun.sh -np 4 HelloWorld


##### Output with total number of tweets in the grid and top 5 hashtags will be displayed
