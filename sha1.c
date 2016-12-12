// Author: Ziqi Fan
// sha1.c: added dedupe with sha1 and fixed chunking to local distributed burst buffer
//
#include <mpi.h>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <time.h>
#include <openssl/sha.h>

// dedupe use fixed chunking; size is 32 KB
#define CHUNK_SIZE 32*1024

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
        burstBuffer = (char*) malloc(sizeof(char) * 3 *  1024 * 1024 *1024 );

        MPI_Status status;
        int i = 0;
        //unsigned long offset = 0;
        for(i = 0; i < 7; i++) {
            MPI_Recv(burstBuffer, fileSize, MPI_CHAR, MPI_ANY_SOURCE, 0, MPI_COMM_WORLD, &status);
            //MPI_Recv(burstBuffer+offset, fileSize, MPI_CHAR, MPI_ANY_SOURCE, 0, MPI_COMM_WORLD, &status);
            printf("burst buffer receive from rank %d\n", status.MPI_SOURCE);
            //offset += fileSize;
        }

        free(burstBuffer);
        //memcpy(burstBuffer+fileSize, recvBuffer, fileSize);
    }
    else {
        printf("This is a writer process.\n");

        // sha1
        int i = 0;
        unsigned char temp[SHA_DIGEST_LENGTH];
        char buf[SHA_DIGEST_LENGTH*2];
        memset(buf, 0x0, SHA_DIGEST_LENGTH*2);
        memset(temp, 0x0, SHA_DIGEST_LENGTH);
        char chunk[CHUNK_SIZE];
        memset(chunk, 0x0, CHUNK_SIZE);
        int offset=0;
        int num=0;
        while(offset<fileSize) {
            SHA1((unsigned char *)readBuffer+offset, CHUNK_SIZE, temp);
            for (i=0; i < SHA_DIGEST_LENGTH; i++) {
                sprintf((char*)&(buf[i*2]), "%02x", temp[i]);
            }
            printf("num %d SHA1 is %s\n", num++, buf);
            offset += CHUNK_SIZE;
        }

        // send
        MPI_Send(readBuffer, fileSize, MPI_CHAR, (rank/8)*8, 0, MPI_COMM_WORLD);
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
