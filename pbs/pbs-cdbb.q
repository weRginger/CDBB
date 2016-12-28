#!/bin/bash -l
#PBS -N cdbb-40
#PBS -e cdbb-40.e
#PBS -o cdbb-40.o
#PBS -l walltime=00:02:00,nodes=5:ppn=8
#PBS -M ziqifan16@gmail.com
#PBS -m abe
#PBS -q batch

module load intel impi
cd /home/dudh/fanxx234/CDBB
mpirun -np 40 ./cdbb
