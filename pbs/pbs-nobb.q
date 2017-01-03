#!/bin/bash -l
#PBS -N nobb-16
#PBS -e nobb-16.e
#PBS -o nobb-16.o
#PBS -l walltime=00:5:00,nodes=2:ppn=8
#PBS -M ziqifan16@gmail.com
#PBS -m abe
#PBS -q batch

module load intel impi
cd /home/dudh/fanxx234/CDBB
mpirun -np 16 ./nobb
