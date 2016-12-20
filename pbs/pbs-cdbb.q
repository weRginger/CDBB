#!/bin/bash -l
#PBS -N cdbb-1600
#PBS -e cdbb-1600.e
#PBS -o cdbb-1600.o
#PBS -l walltime=00:05:00,nodes=200:ppn=8
#PBS -M ziqifan16@gmail.com
#PBS -m abe
#PBS -q batch

module load intel impi
cd /home/dudh/fanxx234/CDBB
mpirun -np 1600 ./cdbb
