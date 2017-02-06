#!/bin/bash -l
#PBS -N cdbb-368-mix
#PBS -e cdbb-368-mix.e
#PBS -o cdbb-368-mix.o
#PBS -l walltime=00:60:00,nodes=46:ppn=8
#PBS -M ziqifan16@gmail.com
#PBS -m abe
#PBS -q batch

module load intel impi
cd /home/dudh/fanxx234/CDBB
mpirun -np 368 ./cdbb 167772160 285212672 654311424 1660944384 2147483646
