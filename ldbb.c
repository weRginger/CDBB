// Author: Ziqi Fan
// ldbb.c: local distributed burst buffer
//

#include <mpi.h>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <time.h>
#include <pthread.h>

#define debug 0

#if debug
#define dbg_print(format,args...)\
    do\
    {\
        printf("[%s][%d]"format,__FUNCTION__, __LINE__,## args);\
    }while(0)
#else
#define dbg_print(format,args...)
#endif

unsigned long burstBufferMaxSize = 3221225472; // 3GB = 3*1024*1024*1024

unsigned long burstBufferOffset = 0;

struct threadParams {
    int rank;
    char* burstBuffer;
    int size;
    int fileSize;
};

unsigned long fsize(char* file)
{
    FILE * f = fopen(file, "r");
    fseek(f, 0, SEEK_END);
    unsigned long len = (unsigned long)ftell(f);
    fclose(f);
    return len;
}

void* producer(void *ptr) {
    struct threadParams *tp = ptr;
    MPI_Status status;
    int i;

    while (1) {
        // receive from writer how much data it wants to write
        unsigned long incomingDataSize;
        MPI_Recv(&incomingDataSize, 1, MPI_UNSIGNED_LONG, MPI_ANY_SOURCE, 0, MPI_COMM_WORLD, &status);
        int checkResult = 0;
        // BB has no space left
        if(burstBufferOffset + incomingDataSize > burstBufferMaxSize) {
            checkResult = 0;
            MPI_Send(&checkResult, 1, MPI_INT, status.MPI_SOURCE, 1, MPI_COMM_WORLD);
            dbg_print("BB producer %d: No enough BB space left for rank %d\n", tp->rank, status.MPI_SOURCE);
        }
        // BB has enough space; let writer send the real data
        else {
            checkResult = 1;
            MPI_Send(&checkResult, 1, MPI_INT, status.MPI_SOURCE, 1, MPI_COMM_WORLD);
            MPI_Recv(tp->burstBuffer, incomingDataSize, MPI_CHAR, MPI_ANY_SOURCE, 2, MPI_COMM_WORLD, &status);
            burstBufferOffset += incomingDataSize;
            dbg_print("BB producer %d: burst buffer receive %u data from rank %d. burstBufferOffset is %u\n", tp->rank, incomingDataSize, status.MPI_SOURCE, burstBufferOffset);
        }
    }
    pthread_exit(0);
}

void* consumer(void *ptr) {
    struct threadParams *tp = ptr;
    //int i;

    dbg_print("BB consumer %d: just entered, nothing been done yet\n", tp->rank);
    while(1) {
        if(burstBufferOffset > 0) {
            char filename[64];
            char *prefix="/scratch.global/fan/rank";
            strcpy(filename, prefix);
            char buf[sizeof(int)+1];
            snprintf(buf, sizeof buf, "%d", tp->rank);
            strcat(filename, buf);
            strcat(filename, ".out");
            FILE *fp;
            fp = fopen(filename, "a+");
            if(fp == NULL) {
                printf("cannot open file for write. Exit!\n");
                return;
            }
            fwrite(tp->burstBuffer , 1 , tp->fileSize , fp );
            fclose(fp);

            burstBufferOffset -= tp->fileSize;
            dbg_print("BB consumer %d: drained %d amount of data to PFS, burstBufferOffset is %d\n", tp->rank, tp->fileSize, burstBufferOffset);
        }
    }
    pthread_exit(0);
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
    if (readBuffer == 0) {
        printf("ERROR: Out of memory when malloc readBuffer\n");
        return 1;
    }
    fread(readBuffer, 1, fileSize, fp);
    fclose(fp);

    // using MPI timer to get the start and end time
    double timeStart, timeEnd;

    MPI_Barrier(MPI_COMM_WORLD);

    timeStart = MPI_Wtime();

    // BB rank
    if(rank % 8 == 0) {
        char *burstBuffer;
        burstBuffer = (char*) malloc(sizeof(char) * 3 *  1024 * 1024 *1024 ); // malloc 3GB as the local burst buffer

        pthread_t pro, con;

        struct threadParams tp;
        tp.rank = rank;
        tp.burstBuffer = burstBuffer;
        tp.size = burstBufferMaxSize;
        tp.fileSize = fileSize;

        // Create the threads
        pthread_create(&con, NULL, consumer, &tp);
        pthread_create(&pro, NULL, producer, &tp);

        // Wait for the threads to finish
        // Otherwise main might run to the end
        // and kill the entire process when it exits.
        pthread_join(pro, NULL);
        pthread_join(con, NULL);

        free(burstBuffer);
    }
    // writer rank, the first half rank writes
    else if(rank < size/2) {
        // before sending the real data, send fileSize to BB to check how much space left
        MPI_Send(&fileSize, 1, MPI_UNSIGNED_LONG, (rank/8)*8, 0, MPI_COMM_WORLD);
        int checkResult;
        MPI_Recv(&checkResult, 1, MPI_INT, MPI_ANY_SOURCE, 1, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
        // only there is enough space left in BB, we will send the real data to it.
        if(checkResult == 1) {
            MPI_Send(readBuffer, fileSize, MPI_CHAR, (rank/8)*8, 2, MPI_COMM_WORLD);
            dbg_print("Writer %d: send %u amount of data to BB\n", rank, fileSize);
        }
        else {
            dbg_print("Writer %d: Not enough space left in BB -> write to PFS\n", rank);

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
    }
    // the rest half ranks do nothing
    else {
        // do nothing
    }

    timeEnd = MPI_Wtime();
    if(rank % 8 == 0) {
        printf( "$$ Elapsed time for BB rank %d is %f\n\n", rank, timeEnd - timeStart );
    }
    else {
        printf( "$$ Elapsed time for writer rank %d is %f\n\n", rank, timeEnd - timeStart );
    }

    free(readBuffer);

    // Finalize the MPI environment. No more MPI calls can be made after this
    MPI_Finalize();
    return 0;
}
