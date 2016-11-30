// Author: Ziqi Fan
// cdbb.c: Collaborative Distributed Burst Buffer
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

    //printf("sizeof(char) %d \n", sizeof(char));

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
    if (readBuffer == 0) {
        printf("ERROR: Out of memory when malloc readBuffer\n");
        return 1;
    }
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
    if( (rank % 8 == 0) && (rank < size/2)) {
        printf("This is a burst buffer on rank %d\n", rank);

        // mallc some space to receive data from writers
        char *recvBuffer;
        recvBuffer = (char*) malloc(sizeof(char) * fileSize);

        // malloc 3GB as the local burst buffer
        char *burstBuffer;
        burstBuffer = (char*) malloc(sizeof(char) * 3 *  1024 * 1024 *1024 );

        MPI_Status status;
        int i = 0;
        //unsigned long offset = 0;
        for(i = 0; i < 5; i++) {
            MPI_Recv(burstBuffer, fileSize, MPI_CHAR, MPI_ANY_SOURCE, 0, MPI_COMM_WORLD, &status);
            //MPI_Recv(burstBuffer+offset, fileSize, MPI_CHAR, MPI_ANY_SOURCE, 0, MPI_COMM_WORLD, &status);
            printf("burst buffer receive from rank %d\n", status.MPI_SOURCE);
            //offset += fileSize;
        }

        free(burstBuffer);
        //memcpy(burstBuffer+fileSize, recvBuffer, fileSize);
    }
    else if( (rank % 8 == 0) && (rank >= size/2)) {
        printf("This is a burst buffer on rank %d\n", rank);

        // mallc some space to receive data from writers
        char *recvBuffer;
        recvBuffer = (char*) malloc(sizeof(char) * fileSize);

        // malloc 3GB as the local burst buffer
        char *burstBuffer;
        burstBuffer = (char*) malloc(sizeof(char) * 3 *  1024 * 1024 *1024 );
        if (burstBuffer == 0) {
            printf("ERROR: Out of memory when malloc burstBuffer\n");
            return 1;
        }

        MPI_Status status;
        int i = 0;
        //unsigned long offset = 0;
        for(i = 0; i < 2; i++) {
            MPI_Recv(burstBuffer, fileSize, MPI_CHAR, MPI_ANY_SOURCE, 0, MPI_COMM_WORLD, &status);
            //MPI_Recv(burstBuffer+offset, fileSize, MPI_CHAR, MPI_ANY_SOURCE, 0, MPI_COMM_WORLD, &status);
            printf("burst buffer receive from rank %d\n", status.MPI_SOURCE);
            //offset += fileSize;
        }

        free(burstBuffer);
        //memcpy(burstBuffer+fileSize, recvBuffer, fileSize);
    }
    else if(rank > size/2) {
        printf("My rank=%d is larger than size/2=%d, so I do nothing.\n", rank, size/2);
    }
    else if( (rank%8 >= 1) && (rank%8 <= 5) ) {
        printf("This is a writer on rank %d, I will write to rank %d\n", rank, (rank/8)*8);
        MPI_Send(readBuffer, fileSize, MPI_CHAR, (rank/8)*8, 0, MPI_COMM_WORLD);
    }
    else {
        printf("This is a writer on rank %d, I will write to rank %d\n", rank, (rank/8)*8+size/2);
        MPI_Send(readBuffer, fileSize, MPI_CHAR, (rank/8)*8+size/2, 0, MPI_COMM_WORLD);
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
