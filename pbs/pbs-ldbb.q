#!/bin/bash -l
#PBS -N ldbb-40
#PBS -e ldbb-40.e
#PBS -o ldbb-40.o
#PBS -l walltime=00:05:00,nodes=5:ppn=8
#PBS -M ziqifan16@gmail.com
#PBS -m abe
#PBS -q batch

module load intel impi
cd /home/dudh/fanxx234/CDBB
mpirun -np 40 ./ldbb
