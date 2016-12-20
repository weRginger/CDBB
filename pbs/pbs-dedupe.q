#!/bin/bash -l
#PBS -N dedupe-40
#PBS -e dedupe-40.e
#PBS -o dedupe-40.o
#PBS -l walltime=00:05:00,nodes=5:ppn=8
#PBS -M ziqifan16@gmail.com
#PBS -m abe
#PBS -q batch

module load intel impi
cd /home/dudh/fanxx234/CDBB
mpirun -np 40 ./dedupe
