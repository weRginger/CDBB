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
    fread(readBuffer, 1, fileSize, fp);

    fclose(fp);

    // using MPI timer to get the start and end time
    double timeStart, timeEnd;

    timeStart = MPI_Wtime();
    //printf( "timeStart %f\n", timeStart );
    if(rank % 8 == 0) {
        printf("This is a burst buffer process.\n");

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
            if(burstBufferOffset + incomingDataSize > burstBufferMaxSize) {
                checkResult = 0;
                MPI_Send(&checkResult, 1, MPI_INT, status.MPI_SOURCE, 1, MPI_COMM_WORLD);
                printf("burst buffer check receive from rank %d. No enough BB space left\n", status.MPI_SOURCE);
            }
            else {
                checkResult = 1;
                MPI_Send(&checkResult, 1, MPI_INT, status.MPI_SOURCE, 1, MPI_COMM_WORLD);

                MPI_Recv(burstBuffer, incomingDataSize, MPI_CHAR, MPI_ANY_SOURCE, 2, MPI_COMM_WORLD, &status);
                //MPI_Recv(burstBuffer+burstBufferOffset, fileSize, MPI_CHAR, MPI_ANY_SOURCE, 0, MPI_COMM_WORLD, &status);
                burstBufferOffset += incomingDataSize;
                printf("burst buffer receive from rank %d. burstBufferOffset is %u\n", status.MPI_SOURCE, burstBufferOffset);
            }
        }

        free(burstBuffer);
        //memcpy(burstBuffer+fileSize, recvBuffer, fileSize);
    }
    else {
        printf("This is a writer process.\n");

        // before sending the real data, send fileSize to BB to check how much space left
        MPI_Send(&fileSize, 1, MPI_UNSIGNED_LONG, (rank/8)*8, 0, MPI_COMM_WORLD);
        int checkResult;
        MPI_Recv(&checkResult, 1, MPI_INT, MPI_ANY_SOURCE, 1, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
        // only there is enough space left in BB, we will send the real data to it.
        if(checkResult == 1) {
            MPI_Send(readBuffer, fileSize, MPI_CHAR, (rank/8)*8, 2, MPI_COMM_WORLD);
            printf("Rank %d sends %u amount of data to BB\n", rank, fileSize);
        }
        else {
            printf("Not enough space left in BB for rank %d\n", rank);
        }
    }
    MPI_Barrier(MPI_COMM_WORLD);

    timeEnd = MPI_Wtime();
    printf( "Elapsed time for rank %d is %f\n", rank, timeEnd - timeStart );
    //time[rank] = timeEnd - timeStart;
    //printf( "Longest elapsed time is %d\n", find_max(time,1000) );

    free(readBuffer);

    // Finalize the MPI environment. No more MPI calls can be made after this
    MPI_Finalize();

    //int maxIndex = find_max(time,1000);
    //printf( "maxIndex is %d\n", maxIndex );

    return 0;
}
