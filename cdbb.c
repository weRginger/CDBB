// Author: Ziqi Fan
// Collaborative Distributed Burst Buffer
//
#include <mpi.h>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>

int main(int argc, char** argv) {
  // Initialize the MPI environment. The two arguments to MPI Init are not
  // currently used by MPI implementations, but are there in case future
  // implementations might need the arguments.
  MPI_Init(NULL, NULL);

  // Get the number of processes
  int size;
  MPI_Comm_size(MPI_COMM_WORLD, &size);

  // Get the rank of the process
  int rank;
  MPI_Comm_rank(MPI_COMM_WORLD, &rank);

  // Get the name of the processor
  char processor_name[MPI_MAX_PROCESSOR_NAME];
  int name_len;
  MPI_Get_processor_name(processor_name, &name_len);

  // Print off a hello world message
  printf("Hello world from processor %s, rank %d out of %d processors\n", processor_name, rank, size);
         
  FILE *fp;
  
  if(rank%8 == 0) {
    printf("This is a burst buffer process.\n");
  }
  else {
    printf("This is a writer process.\n");
    
    char filename[64];
    
    char *prefix="/scratch.global/fan/rank";
    strcpy(filename, prefix);
    
    char buf[sizeof(int)+1];
    snprintf(buf, sizeof buf, "%d", rank);
    
    strcat(filename, buf);
    strcat(filename, ".txt");
    
    
    fp = fopen(filename, "a+");
    if(fp == NULL) {
      printf("cannot open file. Exit!\n");
      return 1;
    }
    
    fprintf(fp, "This is testing for fprintf...\n");
    fputs("This is testing for fputs...\n", fp);
    fclose(fp);
  }

  // Finalize the MPI environment. No more MPI calls can be made after this
  MPI_Finalize();
}
