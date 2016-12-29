#!/bin/bash -l
#PBS -N cdbb-16
#PBS -e cdbb-16.e
#PBS -o cdbb-16.o
#PBS -l walltime=00:02:00,nodes=2:ppn=8
#PBS -M ziqifan16@gmail.com
#PBS -m abe
#PBS -q batch

module load intel impi
cd /home/dudh/fanxx234/CDBB
mpirun -np 16 ./cdbb
