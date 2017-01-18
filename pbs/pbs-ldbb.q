#!/bin/bash -l
#PBS -N ldbb-512
#PBS -e ldbb-512.e
#PBS -o ldbb-512.o
#PBS -l walltime=00:10:00,nodes=64:ppn=8
#PBS -M ziqifan16@gmail.com
#PBS -m abe
#PBS -q batch

module load intel impi
cd /home/dudh/fanxx234/CDBB
mpirun -np 512 ./ldbb
