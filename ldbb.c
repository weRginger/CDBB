// Author: Ziqi Fan
// ldbb.c: local distributed burst buffer
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

    FILE *fp;

    fp = fopen("/home/dudh/fanxx234/CDBB/sample.vmdk", "r");
    if(fp == NULL) {
        printf("cannot open file for read. Exit!\n");
        return 1;
    }

    // read file to buffer
    unsigned long fileSize = fsize("/home/dudh/fanxx234/CDBB/sample.vmdk");
    char *readBuffer;
    fseek(fp, 0, SEEK_END);
    rewind(fp);
    readBuffer = (char*) malloc(sizeof(char) * fileSize);
    fread(readBuffer, 1, fileSize, fp);

    fclose(fp);

    // using MPI timer to get the start and end time
    double timeStart, timeEnd;
    timeStart = MPI_Wtime();

    if(rank % 8 == 0) {
        // mallc some space to receive data from writers
        char *recvBuffer;
        recvBuffer = (char*) malloc(sizeof(char) * fileSize);

        // malloc 3GB as the local burst buffer
        char *burstBuffer;
        unsigned long burstBufferMaxSize = 3221225472; // = 3*1024*1024*1024
        burstBuffer = (char*) malloc(sizeof(char) * 3 *  1024 * 1024 *1024 );

        unsigned long burstBufferOffset = 0;

        MPI_Status status;
        int i = 0;
        //unsigned long offset = 0;
        for(i = 0; i < 7; i++) {
            // receive from writer how much data it wants to write
            unsigned long incomingDataSize;
            MPI_Recv(&incomingDataSize, 1, MPI_UNSIGNED_LONG, MPI_ANY_SOURCE, 0, MPI_COMM_WORLD, &status);
            int checkResult = 0;
            // BB has no space left
            if(burstBufferOffset + incomingDataSize > burstBufferMaxSize) {
                checkResult = 0;
                MPI_Send(&checkResult, 1, MPI_INT, status.MPI_SOURCE, 1, MPI_COMM_WORLD);
                printf("BB %d: No enough BB space left for rank %d\n", rank, status.MPI_SOURCE);
            }
            // BB has enough space; let writer send the real data
            else {
                checkResult = 1;
                MPI_Send(&checkResult, 1, MPI_INT, status.MPI_SOURCE, 1, MPI_COMM_WORLD);
                MPI_Recv(burstBuffer, incomingDataSize, MPI_CHAR, MPI_ANY_SOURCE, 2, MPI_COMM_WORLD, &status);
                burstBufferOffset += incomingDataSize;
                printf("BB %d: burst buffer receive %u data from rank %d. burstBufferOffset is %u\n", rank, incomingDataSize, status.MPI_SOURCE, burstBufferOffset);
            }
        }
        free(burstBuffer);
    }
    else {
        // before sending the real data, send fileSize to BB to check how much space left
        MPI_Send(&fileSize, 1, MPI_UNSIGNED_LONG, (rank/8)*8, 0, MPI_COMM_WORLD);
        int checkResult;
        MPI_Recv(&checkResult, 1, MPI_INT, MPI_ANY_SOURCE, 1, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
        // only there is enough space left in BB, we will send the real data to it.
        if(checkResult == 1) {
            MPI_Send(readBuffer, fileSize, MPI_CHAR, (rank/8)*8, 2, MPI_COMM_WORLD);
            printf("Writer %d: send %u amount of data to BB\n", rank, fileSize);
        }
        else {
            printf("Writer %d: Not enough space left in BB\n", rank);
        }
    }
    MPI_Barrier(MPI_COMM_WORLD);

    timeEnd = MPI_Wtime();
    printf( "Elapsed time for rank %d is %f\n", rank, timeEnd - timeStart );

    free(readBuffer);

    // Finalize the MPI environment. No more MPI calls can be made after this
    MPI_Finalize();
    return 0;
}
