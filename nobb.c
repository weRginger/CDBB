// Author: Ziqi Fan
// nobb.c: no burst buffer, write to PFS directly
//
#include <mpi.h>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <time.h>

unsigned long fsize(char* file)
{
    FILE * f = fopen(file, "r");
    fseek(f, 0, SEEK_END);
    unsigned long len = (unsigned long)ftell(f);
    fclose(f);
    return len;
}

int find_max(double a[], int n) {
    int c, index;
    double max;

    max = a[0];
    index = 0;

    for (c = 1; c < n; c++) {
        if (a[c] > max) {
            index = c;
            max = a[c];
        }
    }

    return index;
}

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

    fp = fopen("/home/dudh/fanxx234/CDBB/sample.vmdk", "r");
    if(fp == NULL) {
        printf("cannot open file for read. Exit!\n");
        return 1;
    }

    // store all the processing time
    //double time[1000] = 0;

    // read file to buffer
    unsigned long fileSize = fsize("/home/dudh/fanxx234/CDBB/sample.vmdk");
    printf("fileSize = %u\n", fileSize);
    char *readBuffer;
    fseek(fp, 0, SEEK_END);
    rewind(fp);
    readBuffer = (char*) malloc(sizeof(char) * fileSize);
    fread(readBuffer, 1, fileSize, fp);

    fclose(fp);

    // using MPI timer to get the start and end walltime
    double timeStart, timeEnd;

    // using clock timer to get the clock ticks
    clock_t clockStart, clockEnd;

    // using gettimeofday to get the wall time
    struct timeval tvStart, tvEnd;

    gettimeofday(&tvStart, NULL);
    clockStart = clock();
    timeStart = MPI_Wtime();
    //printf( "timeStart %f\n", timeStart );
    if(rank % 8 == 0) {
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
        strcat(filename, ".out");

        fp = fopen(filename, "a+");
        if(fp == NULL) {
            printf("cannot open file for write. Exit!\n");
            return 1;
        }
        fwrite(readBuffer , 1 , fileSize , fp );
        fclose(fp);
    }
    MPI_Barrier(MPI_COMM_WORLD);

    timeEnd = MPI_Wtime();
    clockEnd= clock();
    gettimeofday(&tvEnd, NULL);
    printf( "Elapsed MPI_Wtime for rank %d is %f\n", rank, timeEnd - timeStart );
    printf( "Elapsed cpu time for rank %d is %f\n", rank, (clockEnd - clockStart) / CLOCKS_PER_SEC );

    long elapsed = (tvEnd.tv_sec-tvStart.tv_sec)*1000000 + (tvEnd.tv_usec-tvStart.tv_usec);
    printf( "Elapsed walltime for rank %d is %ld\n", rank, elapsed);
    //time[rank] = timeEnd - timeStart;
    //printf( "Longest elapsed time is %d\n", find_max(time,1000) );

    free(readBuffer);

    // Finalize the MPI environment. No more MPI calls can be made after this
    MPI_Finalize();

    //int maxIndex = find_max(time,1000);
    //printf( "maxIndex is %d\n", maxIndex );

    return 0;
}
