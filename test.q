#!/bin/bash -l
#PBS -N test
#PBS -e test.e
#PBS -o test.o
#PBS -l walltime=00:01:00,nodes=2:ppn=8
#PBS -M ziqifan16@gmail.com
#PBS -m abe
#PBS -q batch

module load intel impi
cd /home/dudh/fanxx234/CDBB
mpirun -n 16 ./cdbb
