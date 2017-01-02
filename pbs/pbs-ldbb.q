#!/bin/bash -l
#PBS -N ldbb-32
#PBS -e ldbb-32.e
#PBS -o ldbb-32.o
#PBS -l walltime=00:05:00,nodes=4:ppn=8
#PBS -M ziqifan16@gmail.com
#PBS -m abe
#PBS -q batch

module load intel impi
cd /home/dudh/fanxx234/CDBB
mpirun -np 32 ./ldbb
