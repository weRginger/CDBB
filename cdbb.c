// Author: Ziqi Fan
// cdbb.c: Collaborative Distributed Burst Buffer
//

#include <mpi.h>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <time.h>
#include <pthread.h>
#include <limits.h>

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

unsigned long burstBufferMaxSize = 3145728; // 3MB = 3*1024*1024

struct threadParams {
    int rank; // the rank of current process
    int totalRank; // the total number of ranks (processes)
    char* burstBuffer;
    int size; // the size of one burst buffer
    int fileSize; // the size of incoming data
    unsigned long* burstBufferOffset; // utilization of all burst buffers
};

unsigned long fsize(char* file)
{
    FILE * f = fopen(file, "r");
    fseek(f, 0, SEEK_END);
    unsigned long len = (unsigned long)ftell(f);
    fclose(f);
    return len;
}

int findSmallest(unsigned long* array, int size) {
    int i = 0;
    int ans;
    unsigned long smallest = INT_MAX;
    for(i=0; i<size/8; i++) {
        if(smallest > array[i]) {
            smallest = array[i];
            ans = i;
        }
    }
    printf("Rank of smallest burst buffer offset is %d, offset is %u\n\n", ans, smallest);
    return ans;
}

void* producer(void *ptr) {
    struct threadParams *tp = ptr;

    printf("BB producer %d: just entered, nothing been done yet\n\n", tp->rank);

    MPI_Status status;
    int i;

    while(1) {
        unsigned long incomingDataSize; // receive from writer how much data it wants to write

        dbg_print("reach here rank %d!\n", tp->rank);
        MPI_Recv(&incomingDataSize, 1, MPI_UNSIGNED_LONG, MPI_ANY_SOURCE, 0, MPI_COMM_WORLD, &status);
        dbg_print("reach here rank %d!\n", tp->rank);
        int localBB = tp->rank / 8; // the rank number of local burst buffer

        int checkResult = 0; // denote whether any BB has space left; 1 means yes and 0 means no

        // local BB has enough space; let writer send the real data
        if(tp->burstBufferOffset[localBB] + incomingDataSize < burstBufferMaxSize) {
            dbg_print("reach here rank %d!\n", tp->rank);
            // broadcast the changed burst buffer offset to other ranks
            tp->burstBufferOffset[localBB] += incomingDataSize;
            MPI_Bcast(tp->burstBufferOffset, tp->totalRank / 8, MPI_UNSIGNED_LONG, tp->rank, MPI_COMM_WORLD);
            dbg_print("reach here rank %d!\n", tp->rank);

            checkResult = 1;
            MPI_Send(&checkResult, 1, MPI_INT, status.MPI_SOURCE, 1, MPI_COMM_WORLD);
            dbg_print("reach here rank %d!\n", tp->rank);
            int BBrank2send = tp->rank / 8 * 8;
            MPI_Send(&BBrank2send, 1, MPI_INT, status.MPI_SOURCE, 2, MPI_COMM_WORLD);
            dbg_print("reach here rank %d!\n", tp->rank);
            MPI_Recv(tp->burstBuffer, incomingDataSize, MPI_CHAR, MPI_ANY_SOURCE, 3, MPI_COMM_WORLD, &status);
            dbg_print("reach here rank %d!\n", tp->rank);

            printf("BB producer %d: burst buffer receive %u data from rank %d. burstBufferOffset is %u\n\n", tp->rank, incomingDataSize, status.MPI_SOURCE, tp->burstBufferOffset[localBB]);

            continue;
        }
        dbg_print("reach here rank %d!\n", tp->rank);
        int rankOfSmallestBurstBufferOffset = findSmallest(tp->burstBufferOffset, tp->totalRank);
        // remote BB has enough space; let writer send the read data
        if(tp->burstBufferOffset[rankOfSmallestBurstBufferOffset] + incomingDataSize < burstBufferMaxSize) {
            dbg_print("reach here rank %d!\n", tp->rank);
            // broadcast the changed burst buffer offset to other ranks
            tp->burstBufferOffset[rankOfSmallestBurstBufferOffset] += incomingDataSize;
            MPI_Bcast(tp->burstBufferOffset, tp->totalRank / 8, MPI_UNSIGNED_LONG, tp->rank, MPI_COMM_WORLD);

            checkResult = 1;
            MPI_Send(&checkResult, 1, MPI_INT, status.MPI_SOURCE, 1, MPI_COMM_WORLD);
            int BBrank2send  = rankOfSmallestBurstBufferOffset * 8;
            MPI_Send(&BBrank2send, 1, MPI_INT, status.MPI_SOURCE, 2, MPI_COMM_WORLD);
            MPI_Recv(tp->burstBuffer, incomingDataSize, MPI_CHAR, MPI_ANY_SOURCE, 3, MPI_COMM_WORLD, &status);

            printf("BB producer %d: burst buffer receive %u data from rank %d. burstBufferOffset is %u\n\n", tp->rank, incomingDataSize, status.MPI_SOURCE, tp->burstBufferOffset[rankOfSmallestBurstBufferOffset]);
        }
        // all BBs do not have enough space; writer has to bypass BB and write to PFS
        else {
            dbg_print("reach here rank %d!\n", tp->rank);
            checkResult = 0;
            MPI_Send(&checkResult, 1, MPI_INT, status.MPI_SOURCE, 1, MPI_COMM_WORLD);
            int BBrank2send  = 233333;
            MPI_Send(&BBrank2send, 1, MPI_INT, status.MPI_SOURCE, 2, MPI_COMM_WORLD);
            printf("BB producer %d: No enough BB space left for rank %d\n\n", tp->rank, status.MPI_SOURCE);
        }
        dbg_print("reach here rank %d!\n", tp->rank);
    }
    pthread_exit(0);
}

void* consumer(void *ptr) {
    struct threadParams *tp = ptr;

    printf("BB consumer %d: just entered, nothing been done yet\n\n", tp->rank);

    while(1) {
        if(tp->burstBufferOffset[tp->rank / 8] > 0) {
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
                printf("cannot open file for write. Exit!\n\n");
                return;
            }
            fwrite(tp->burstBuffer , 1 , tp->fileSize , fp );
            fclose(fp);

            tp->burstBufferOffset[tp->rank / 8] -= tp->fileSize;
            // broadcast the changed burst buffer offset to other ranks
            MPI_Bcast(tp->burstBufferOffset, tp->totalRank / 8, MPI_UNSIGNED_LONG, tp->rank, MPI_COMM_WORLD);
            printf("BB consumer %d: drained %d amount of data to PFS, burstBufferOffset is %d\n\n", tp->rank, tp->fileSize, tp->burstBufferOffset[tp->rank / 8]);
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

    // Print off a hello world message
    printf("Hello world from processor %s, rank %d out of %d processors\n", processor_name, rank, size);

    FILE *fp;
    fp = fopen("/home/dudh/fanxx234/CDBB/ICC2011.pdf", "r");
    if(fp == NULL) {
        printf("cannot open file for read. Exit!\n\n");
        return 1;
    }

    // read file to buffer
    unsigned long fileSize = fsize("/home/dudh/fanxx234/CDBB/ICC2011.pdf");
    char *readBuffer;
    fseek(fp, 0, SEEK_END);
    rewind(fp);
    readBuffer = (char*) malloc(sizeof(char) * fileSize);
    if (readBuffer == 0) {
        printf("ERROR: Out of memory when malloc readBuffer\n\n");
        return 1;
    }
    fread(readBuffer, 1, fileSize, fp);
    fclose(fp);

    // using MPI timer to get the start and end time
    double timeStart, timeEnd;
    timeStart = MPI_Wtime();

    // BB rank
    if(rank % 8 == 0) {
        // malloc 3MB as the local burst buffer
        char *burstBuffer;
        burstBuffer = (char*) malloc(sizeof(char) * 3 *  1024 * 1024);

        unsigned long *burstBufferOffset;
        burstBufferOffset = (unsigned long*) malloc(sizeof(unsigned long) * size / 8);
        int i;
        for(i=0; i<size/8; i++) {
            burstBufferOffset[i] = 0;
        }

        pthread_t pro, con;

        struct threadParams tp;
        tp.rank = rank;
        tp.totalRank = size;
        tp.burstBuffer = burstBuffer;
        tp.size = burstBufferMaxSize;
        tp.fileSize = fileSize;
        tp.burstBufferOffset = burstBufferOffset;

        // Create the threads
        pthread_create(&con, NULL, consumer, &tp);
        pthread_create(&pro, NULL, producer, &tp);

        // Wait for the threads to finish
        // Otherwise main might run to the end
        // and kill the entire process when it exits.
        pthread_join(pro, NULL);
        pthread_cancel(con);

        free(burstBuffer);
        free(burstBufferOffset);
    }
    // writer rank
    else {
        // before sending the real data, send fileSize to local BB to check global BB status
        // if local BB is not full, send real data to local BB
        // if local BB is full but remote BB is not, send to remote BB
        // else send to PFS directly
        MPI_Send(&fileSize, 1, MPI_UNSIGNED_LONG, (rank/8)*8, 0, MPI_COMM_WORLD);
        int checkResult;
        MPI_Recv(&checkResult, 1, MPI_INT, MPI_ANY_SOURCE, 1, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
        int BBrank2send;
        MPI_Recv(&BBrank2send, 1, MPI_INT, MPI_ANY_SOURCE, 2, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
        // only there is enough space left in BB, we will send the real data to it.
        if(checkResult == 1) {
            MPI_Send(readBuffer, fileSize, MPI_CHAR, BBrank2send, 3, MPI_COMM_WORLD);
            printf("Writer %d: send %u amount of data to BB on rank %d\n\n", rank, fileSize, BBrank2send);
        }
        else {
            printf("Writer %d: Not enough space left in BB -> write to PFS\n\n", rank);

            char filename[64];
            char *prefix="/scratch.global/fan/rank";
            strcpy(filename, prefix);
            char buf[sizeof(int)+1];
            snprintf(buf, sizeof buf, "%d", rank);
            strcat(filename, buf);
            strcat(filename, ".out");
            fp = fopen(filename, "a+");
            if(fp == NULL) {
                printf("cannot open file for write. Exit!\n\n");
                return 1;
            }
            fwrite(readBuffer , 1 , fileSize , fp );
            fclose(fp);
        }
    }
    //MPI_Barrier(MPI_COMM_WORLD);

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
