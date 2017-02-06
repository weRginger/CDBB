#!/bin/bash -l
#PBS -N ldbb-368-large
#PBS -e ldbb-368-large.e
#PBS -o ldbb-368-large.o
#PBS -l walltime=00:60:00,nodes=46:ppn=8
#PBS -M ziqifan16@gmail.com
#PBS -m abe
#PBS -q batch

module load intel impi
cd /home/dudh/fanxx234/CDBB
mpirun -np 368 ./ldbb 872415232 1258291200 1577058304 1660944384 2147483646
