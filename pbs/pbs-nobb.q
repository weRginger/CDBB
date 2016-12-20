#!/bin/bash -l
#PBS -N nobb-400
#PBS -e nobb-400.e
#PBS -o nobb-400.o
#PBS -l walltime=00:15:00,nodes=50:ppn=8
#PBS -M ziqifan16@gmail.com
#PBS -m abe
#PBS -q batch

module load intel impi
cd /home/dudh/fanxx234/CDBB
mpirun -np 400 ./nobb
