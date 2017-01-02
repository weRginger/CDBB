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

#define debug 1

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
//unsigned long burstBufferMaxSize = 3221225472; // 3GB = 3*1024*1024*1024

struct threadParams {
    int rank; // the rank of current process
    int totalRank; // the total number of ranks (processes)
    char* burstBuffer;
    int size; // the size of one burst buffer
    int fileSize; // the size of incoming data
    unsigned long* BBmonitor; // utilization of all burst buffers
    int BBmonitorSize; // total number of BB in BBmonitor
    MPI_Win* win; // window for locking purpose
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
    dbg_print("Rank of smallest burst buffer offset is %d, offset is %u\n\n", ans, smallest);
    return ans;
}

void* xMPI_Alloc_mem(size_t nbytes) {
    void* p;
    MPI_Alloc_mem(nbytes, MPI_INFO_NULL, &p);
    if (nbytes != 0 && !p) {
        fprintf(stderr, "MPI_Alloc_mem failed for size %zu\n", nbytes);
        abort();
    }
    return p;
}

void* producer(void *ptr) {
    struct threadParams *tp = ptr;

    dbg_print("BB producer %d: just entered, nothing been done yet\n\n", tp->rank);

    MPI_Status status;
    int i;

    while(1) {
        unsigned long incomingDataSize; // receive from writer how much data it wants to write

        MPI_Recv(&incomingDataSize, 1, MPI_UNSIGNED_LONG, MPI_ANY_SOURCE, 0, MPI_COMM_WORLD, &status);
        int localBB = tp->rank / 8; // the rank number of local burst buffer

        int checkResult = 0; // denote whether any BB has space left; 1 means yes and 0 means no

        MPI_Win_lock(MPI_LOCK_SHARED, 0, 0, *(tp->win));
        // local BB has enough space; let writer send the real data
        if(tp->BBmonitor[localBB] + incomingDataSize < burstBufferMaxSize) {
            tp->BBmonitor[localBB] += incomingDataSize;

            checkResult = 1;
            MPI_Send(&checkResult, 1, MPI_INT, status.MPI_SOURCE, 1, MPI_COMM_WORLD);
            int BBrank2send = tp->rank / 8 * 8;
            MPI_Send(&BBrank2send, 1, MPI_INT, status.MPI_SOURCE, 2, MPI_COMM_WORLD);
            MPI_Recv(tp->burstBuffer, incomingDataSize, MPI_CHAR, MPI_ANY_SOURCE, 3, MPI_COMM_WORLD, &status);

            dbg_print("BB producer %d: burst buffer receive %u data from rank %d. BBmonitor is %u\n\n", tp->rank, incomingDataSize, status.MPI_SOURCE, tp->BBmonitor[localBB]);

            goto UNLOCK;
        }

        int rankOfSmallestBurstBufferOffset = findSmallest(tp->BBmonitor, tp->totalRank);
        // local BB is full, but remote BB has enough space;
        // let writer know which remote BB to try
        if(tp->BBmonitor[rankOfSmallestBurstBufferOffset] + incomingDataSize < burstBufferMaxSize) {
            checkResult = 1;
            MPI_Send(&checkResult, 1, MPI_INT, status.MPI_SOURCE, 1, MPI_COMM_WORLD);
            int BBrank2send  = rankOfSmallestBurstBufferOffset * 8;
            MPI_Send(&BBrank2send, 1, MPI_INT, status.MPI_SOURCE, 2, MPI_COMM_WORLD);

            dbg_print("BB producer %d: local burst buffer is full, but wrtier %d can try remote BB on rank %d\n\n", tp->rank, status.MPI_SOURCE, BBrank2send);
        }
        // all BBs do not have enough space; writer has to bypass BB and write to PFS
        else {
            checkResult = 0;
            MPI_Send(&checkResult, 1, MPI_INT, status.MPI_SOURCE, 1, MPI_COMM_WORLD);
            int BBrank2send  = 233333;
            MPI_Send(&BBrank2send, 1, MPI_INT, status.MPI_SOURCE, 2, MPI_COMM_WORLD);
            dbg_print("BB producer %d: all BBs are full for writier %d\n\n", tp->rank, status.MPI_SOURCE);
        }
UNLOCK:
        // broadcast the changed BBmonitor to other ranks
        MPI_Bcast(&(tp->BBmonitor[localBB]), 1, MPI_UNSIGNED_LONG, tp->rank, MPI_COMM_WORLD);
        MPI_Win_unlock(0, *(tp->win));
    }
    pthread_exit(0);
}

void* consumer(void *ptr) {
    struct threadParams *tp = ptr;

    dbg_print("BB consumer %d: just entered, nothing been done yet\n\n", tp->rank);

    int localBB = tp->rank / 8; // the rank number of local burst buffer

    while(1) {
        if(tp->BBmonitor[localBB] > 0) {
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

            tp->BBmonitor[localBB] -= tp->fileSize;

            dbg_print("BB consumer %d: drained %d amount of data to PFS, burstBufferOffset is %d\n\n", tp->rank, tp->fileSize, tp->BBmonitor[localBB]);
        }
        // broadcast the changed BBmonitor to other ranks
        MPI_Bcast(&(tp->BBmonitor[localBB]), 1, MPI_UNSIGNED_LONG, tp->rank, MPI_COMM_WORLD);
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
    dbg_print("Hello world from processor %s, rank %d out of %d processors\n", processor_name, rank, size);

    FILE *fp;
    fp = fopen("/home/dudh/fanxx234/CDBB/ICC2011.pdf", "r");
    //fp = fopen("/home/dudh/fanxx234/CDBB/sample.vmdk", "r");
    if(fp == NULL) {
        printf("cannot open file for read. Exit!\n\n");
        return 1;
    }

    // read file to buffer
    unsigned long fileSize = fsize("/home/dudh/fanxx234/CDBB/ICC2011.pdf");
    //unsigned long fileSize = fsize("/home/dudh/fanxx234/CDBB/sample.vmdk");
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

    MPI_Win win; // window to manage BB monitor
    int BBmonitorSize = size / 8;
    unsigned long* BBmonitor = (unsigned long*)xMPI_Alloc_mem(BBmonitorSize * sizeof(unsigned long));

    int i;
    for(i=0; i<BBmonitorSize; i++) {
        BBmonitor[i] = 0;
    }

    MPI_Win_create(BBmonitor, sizeof(unsigned long) * BBmonitorSize, sizeof(unsigned long), MPI_INFO_NULL, MPI_COMM_WORLD, &win);

    // using MPI timer to get the start and end time
    double timeStart, timeEnd;
    timeStart = MPI_Wtime();

    // BB rank
    if(rank % 8 == 0) {
        char *burstBuffer;
        burstBuffer = (char*) malloc(sizeof(char) * 3 *  1024 * 1024); // malloc 3MB as the local burst buffer
        //burstBuffer = (char*) malloc(sizeof(char) * 3 *  1024 * 1024 *1024 ); // malloc 3GB as the local burst buffer

        /*
        int BBmonitorSize = size / 8;
        unsigned long* BBmonitor = (unsigned long*)xMPI_Alloc_mem(BBmonitorSize * sizeof(unsigned long));

        int i;
        for(i=0; i<BBmonitorSize; i++) {
            BBmonitor[i] = 0;
        }

        MPI_Win_create(BBmonitor, sizeof(unsigned long) * BBmonitorSize, sizeof(unsigned long), MPI_INFO_NULL, MPI_COMM_WORLD, &win);
        */

        pthread_t pro, con;

        struct threadParams tp;
        tp.rank = rank;
        tp.totalRank = size;
        tp.burstBuffer = burstBuffer;
        tp.size = burstBufferMaxSize;
        tp.fileSize = fileSize;
        tp.BBmonitor = BBmonitor;
        tp.BBmonitorSize = BBmonitorSize;
        tp.win = &win;

        // Create the threads
        pthread_create(&con, NULL, consumer, &tp);
        pthread_create(&pro, NULL, producer, &tp);

        // Wait for the threads to finish
        // [!!!] but currently pro and con threads are in infinite loop
        //       they will not exit naturally
        pthread_join(pro, NULL);
        pthread_join(con, NULL);

        free(burstBuffer);
        MPI_Win_free(&win);
        MPI_Free_mem(BBmonitor);
    }
    // writer rank, first half ranks will write
    else if (rank < size/2) {
        // writer processes do not expose BBmoniror memory in the window
        // [!!!] Note that if not do this, the whole program will hang
        //MPI_Win_create(NULL, 0, 1, MPI_INFO_NULL, MPI_COMM_WORLD, &win);

        // before sending the real data, send fileSize to local BB to check global BB status
        // if local BB is not full, send real data to local BB
        // if local BB is full but remote BB is not, send to remote BB
        // else send to PFS directly
        int localBBrank = (rank/8)*8; // try local BB first;

TRYREMOTEBB:
        MPI_Send(&fileSize, 1, MPI_UNSIGNED_LONG, localBBrank, 0, MPI_COMM_WORLD);
        int checkResult; // 1 means space left in at least one BB, may not be local BB
        MPI_Recv(&checkResult, 1, MPI_INT, MPI_ANY_SOURCE, 1, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
        int returnedBBrank2send; // the returned BB rank number could be local BB or a remote BB
        MPI_Recv(&returnedBBrank2send, 1, MPI_INT, MPI_ANY_SOURCE, 2, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

        if(checkResult == 1) { // there is enough space left in local BB or remote BB
            if(localBBrank == returnedBBrank2send) { // that BB is local BB so we can send to it
                MPI_Send(readBuffer, fileSize, MPI_CHAR, localBBrank, 3, MPI_COMM_WORLD);
                dbg_print("Writer %d: send %u amount of data to local BB on rank %d\n\n", rank, fileSize, localBBrank);
            }
            // that BB is a remote BB, let go back to re-try
            else {
                dbg_print("Writer %d: local BB on rank %d is full, let's try remote BB on rank %d\n\n", rank, localBBrank, returnedBBrank2send);
                localBBrank = returnedBBrank2send;
                goto TRYREMOTEBB;
            }
        }
        else {
            dbg_print("Writer %d: Not enough space left in BB -> write to PFS\n\n", rank);

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
    // the rest half of ranks do nothing
    else {
        // writer processes do not expose BBmoniror memory in the window
        // [!!!] Note that if not do this, the whole program will hang
        //MPI_Win_create(NULL, 0, 1, MPI_INFO_NULL, MPI_COMM_WORLD, &win);
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
