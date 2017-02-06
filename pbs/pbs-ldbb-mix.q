#!/bin/bash -l
#PBS -N ldbb-368-mix
#PBS -e ldbb-368-mix.e
#PBS -o ldbb-368-mix.o
#PBS -l walltime=00:60:00,nodes=46:ppn=8
#PBS -M ziqifan16@gmail.com
#PBS -m abe
#PBS -q batch

module load intel impi
cd /home/dudh/fanxx234/CDBB
mpirun -np 368 ./ldbb 167772160 285212672 654311424 1660944384 2147483646
