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

//unsigned long burstBufferMaxSize = 3145728; // 3MB = 3*1024*1024
unsigned long burstBufferMaxSize = 3221225472; // 3GB = 3*1024*1024*1024

struct threadParams {
    int rank; // the rank of current process
    int totalRank; // the total number of ranks (processes)
    char* burstBuffer;
    int size; // the size of one burst buffer
    unsigned long fileSize; // the size of incoming data
    char* readBuffer; // checkpointing data buffer to write
    unsigned long localBBmonitor;
    pthread_mutex_t* lock_localBBmonitor; // lock for localBBmonitor
    int ckptRun; // keep track how many ckpts have been performed
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
    for(i=0; i<size; i++) {
        if(smallest > array[i]) {
            smallest = array[i];
            ans = i;
        }
    }
    dbg_print("Rank of smallest burst buffer offset is %d, offset is %lu\n", ans, smallest);
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

    dbg_print("BB producer %d: just entered, nothing been done yet\n", tp->rank);

    MPI_Status status;
    int i;

    while(1) {
        pthread_mutex_lock(tp->lock_localBBmonitor);

        // receive from writer how much data it wants to write
        unsigned long incomingDataSize;
        MPI_Recv(&incomingDataSize, 1, MPI_UNSIGNED_LONG, MPI_ANY_SOURCE, 4, MPI_COMM_WORLD, &status);

        // receive the real data from writer
        MPI_Recv(tp->burstBuffer, incomingDataSize, MPI_CHAR, MPI_ANY_SOURCE, 5, MPI_COMM_WORLD, &status);

        tp->localBBmonitor += incomingDataSize;

        pthread_mutex_unlock(tp->lock_localBBmonitor);

        dbg_print("BB producer %d: receive %lu amount of data, localBBmonitor is %lu\n", tp->rank, incomingDataSize, tp->localBBmonitor);
    }
    pthread_exit(0);
}

void* consumer(void *ptr) {
    struct threadParams *tp = ptr;

    dbg_print("BB consumer %d: just entered, nothing been done yet\n", tp->rank);

    while(1) {
        if(tp->localBBmonitor > 0) {
            pthread_mutex_lock(tp->lock_localBBmonitor);

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

            tp->localBBmonitor -= tp->fileSize;

            pthread_mutex_unlock(tp->lock_localBBmonitor);

            int BBmonitorRank = 0;

            MPI_Send(&tp->localBBmonitor, 1, MPI_UNSIGNED_LONG, BBmonitorRank, 6, MPI_COMM_WORLD);

            dbg_print("BB consumer %d: drained %lu amount of data to PFS, localBBmonitor is %lu\n", tp->rank, tp->fileSize, tp->localBBmonitor);
        }
    }
    pthread_exit(0);
}

void* writer(void *ptr) {
    // using MPI timer to get the start and end time
    double timeStart, timeEnd;
    timeStart = MPI_Wtime();

    struct threadParams *tp = ptr;

    // before sending the real data, send fileSize to local BB to check global BB status
    // if local BB is not full, send real data to local BB
    // if local BB is full but remote BB is not, send to remote BB
    // else send to PFS directly
    int BBmonitorRank = 0; // BB monitor rank

    // tell BB monitor rank I am a writer
    int senderID = 1;
    MPI_Send(&senderID, 1, MPI_INT, BBmonitorRank, 0, MPI_COMM_WORLD);

    // tell BB monitor how much data I want to write
    MPI_Send(&tp->fileSize, 1, MPI_UNSIGNED_LONG, BBmonitorRank, 1, MPI_COMM_WORLD);

    // 1 means space left in at least one BB, may not be local BB
    int checkResult;
    MPI_Recv(&checkResult, 1, MPI_INT, MPI_ANY_SOURCE, 2, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

    // the returned BB rank number could be local BB or a remote BB
    int returnedBBrank2send;
    MPI_Recv(&returnedBBrank2send, 1, MPI_INT, MPI_ANY_SOURCE, 3, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

    // there is enough space left in local BB or remote BB
    if(checkResult == 1) {
        // tell BB how much data I want to write
        MPI_Send(&tp->fileSize, 1, MPI_UNSIGNED_LONG, returnedBBrank2send, 4, MPI_COMM_WORLD);

        // send real data
        MPI_Send(tp->readBuffer, tp->fileSize, MPI_CHAR, returnedBBrank2send, 5, MPI_COMM_WORLD);
        dbg_print("Writer %d: send %lu amount of data to BB on rank %d\n", tp->rank, tp->fileSize, returnedBBrank2send);
    }
    else {
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
        fwrite(tp->readBuffer , 1 , tp->fileSize , fp );
        fclose(fp);

        dbg_print("Writer %d: Not enough space left in any BBs -> write %u to PFS\n", tp->rank, tp->fileSize);
    }

    timeEnd = MPI_Wtime();
    printf( "$$ CKPT Run %d: Elapsed time for writer rank %d is %f, timeStart %f, timeEnd %f\n", tp->ckptRun, tp->rank, timeEnd - timeStart, timeStart, timeEnd);
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

    MPI_Status status;

    // create window to manage BB monitor
    MPI_Win win;
    int BBmonitorSize = size / 8;
    unsigned long* BBmonitor = (unsigned long*)xMPI_Alloc_mem(BBmonitorSize * sizeof(unsigned long));
    int i;
    for(i=0; i<BBmonitorSize; i++) {
        BBmonitor[i] = 0;
    }
    MPI_Win_create(BBmonitor, sizeof(unsigned long) * BBmonitorSize, sizeof(unsigned long), MPI_INFO_NULL, MPI_COMM_WORLD, &win);

    // Print off a hello world message
    dbg_print("Hello world from processor %s, rank %d out of %d processors\n", processor_name, rank, size);

    FILE *fp;
    //fp = fopen("/home/dudh/fanxx234/CDBB/ICC2011.pdf", "r");
    fp = fopen("/home/dudh/fanxx234/CDBB/sample.vmdk", "r");
    if(fp == NULL) {
        printf("cannot open file for read. Exit!\n");
        return 1;
    }

    // read file to buffer
    //unsigned long fileSize = fsize("/home/dudh/fanxx234/CDBB/ICC2011.pdf");
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

    MPI_Barrier(MPI_COMM_WORLD);

    // BB monitor rank
    if(rank == 0) {
        while(1) {
            int senderID; // who is sending me information? 0 means from BB; 1 means from writer
            MPI_Recv(&senderID, 1, MPI_INT, MPI_ANY_SOURCE, 0, MPI_COMM_WORLD, &status);

            // talking with BB
            if(senderID == 0) {
                unsigned long newBBmonitor;
                MPI_Recv(&newBBmonitor, 1, MPI_UNSIGNED_LONG, MPI_ANY_SOURCE, 6, MPI_COMM_WORLD, &status);
                BBmonitor[status.MPI_SOURCE / 8 + 7] = newBBmonitor;
            }
            // talking with writer
            if(senderID == 1) {
                // receive from writer how much data it wants to write
                unsigned long incomingDataSize;
                MPI_Recv(&incomingDataSize, 1, MPI_UNSIGNED_LONG, MPI_ANY_SOURCE, 1, MPI_COMM_WORLD, &status);

                // calculate localBB offset in BBmonitor
                int localBB = status.MPI_SOURCE / 8;

                int checkResult = 0; // denote whether any BB has space left; 1 means yes and 0 means no

                MPI_Win_lock(MPI_LOCK_SHARED, 0, 0, win);

                int rankOfSmallestBurstBufferOffset = findSmallest(BBmonitor, BBmonitorSize);

                // local BB has enough space; let writer send the real data
                if(BBmonitor[localBB] + incomingDataSize < burstBufferMaxSize) {
                    BBmonitor[localBB] += incomingDataSize;

                    checkResult = 1;
                    MPI_Send(&checkResult, 1, MPI_INT, status.MPI_SOURCE, 2, MPI_COMM_WORLD);
                    int BBrank2send = localBB * 8 + 7;
                    MPI_Send(&BBrank2send, 1, MPI_INT, status.MPI_SOURCE, 3, MPI_COMM_WORLD);

                    dbg_print("BB monitor: let writer %d send its data to it local BB on rank %d\n", status.MPI_SOURCE, BBrank2send);
                }
                // local BB is full, but remote BB has enough space;
                // let writer know which remote BB to try
                else if(BBmonitor[rankOfSmallestBurstBufferOffset] + incomingDataSize < burstBufferMaxSize) {
                    BBmonitor[rankOfSmallestBurstBufferOffset] += incomingDataSize;

                    checkResult = 1;
                    MPI_Send(&checkResult, 1, MPI_INT, status.MPI_SOURCE, 2, MPI_COMM_WORLD);
                    int BBrank2send  = rankOfSmallestBurstBufferOffset * 8 + 7;
                    MPI_Send(&BBrank2send, 1, MPI_INT, status.MPI_SOURCE, 3, MPI_COMM_WORLD);

                    dbg_print("BB monitor: local BB is full, let writer %d send its data to it remote BB on rank %d\n", status.MPI_SOURCE, BBrank2send);
                }
                // all BBs do not have enough space; writer has to bypass BB and write to PFS
                else {
                    checkResult = 0;
                    MPI_Send(&checkResult, 1, MPI_INT, status.MPI_SOURCE, 2, MPI_COMM_WORLD);
                    int BBrank2send  = 666;
                    MPI_Send(&BBrank2send, 1, MPI_INT, status.MPI_SOURCE, 3, MPI_COMM_WORLD);
                    dbg_print("BB monitor: all BBs are full for writer %d\n", status.MPI_SOURCE);
                }
                MPI_Win_unlock(0, win);
            }
        }
        MPI_Win_free(&win);
        MPI_Free_mem(BBmonitor);
    }
    // BB rank
    if(rank % 8 == 7) {
        // writer processes do not expose BBmoniror memory in the window
        // [!!!] Note that if not do this, the whole program will hang
        //MPI_Win_create(NULL, 0, 1, MPI_INFO_NULL, MPI_COMM_WORLD, &win);

        char *burstBuffer;
        //burstBuffer = (char*) malloc(sizeof(char) * 3 *  1024 * 1024); // malloc 3MB as the local burst buffer
        burstBuffer = (char*) malloc(sizeof(char) * 3 *  1024 * 1024 *1024 ); // malloc 3GB as the local burst buffer

        pthread_t pro, con;

        pthread_mutex_t lock_localBBmonitor;

        struct threadParams tp;
        tp.rank = rank;
        tp.totalRank = size;
        tp.burstBuffer = burstBuffer;
        tp.size = burstBufferMaxSize;
        tp.fileSize = fileSize;
        tp.localBBmonitor = 0;
        tp.lock_localBBmonitor = &lock_localBBmonitor;
        tp.readBuffer = readBuffer;

        // Create the threads
        pthread_create(&con, NULL, consumer, &tp);
        pthread_create(&pro, NULL, producer, &tp);

        // Wait for the threads to finish
        // [!!!] but currently pro and con threads are in infinite loop
        //       they will not exit naturally
        pthread_join(pro, NULL);
        pthread_join(con, NULL);

        free(burstBuffer);
    }
    // writer rank, first half ranks will write
    // every 60 seconds with data size of 1.3GB (i.e. fileSize)
    else if (rank < size/2) {
        int ckptRun = 0; // keep track how many ckpts have been performed

        while(1) {
            sleep(60); // checkpointing frequency

            pthread_t wrtr;

            struct threadParams tp;
            tp.rank = rank;
            tp.totalRank = size;
            tp.burstBuffer = NULL;
            tp.size = burstBufferMaxSize;
            tp.fileSize = fileSize; // checkpointing data size
            tp.localBBmonitor = 0;
            tp.lock_localBBmonitor = 0;
            tp.readBuffer = readBuffer;
            tp.ckptRun = ckptRun;

            // Create the threads
            pthread_create(&wrtr, NULL, writer, &tp);

            ckptRun++;
        }
    }
    // writer rank, the second half ranks will write
    // every 100 seconds with data size of 650MB (i.e. fileSize/2)
    else {
        int ckptRun = 0; // keep track how many ckpts have been performed

        while(1) {
            sleep(100); // checkpointing frequency

            pthread_t wrtr;

            struct threadParams tp;
            tp.rank = rank;
            tp.totalRank = size;
            tp.burstBuffer = NULL;
            tp.size = burstBufferMaxSize;
            tp.fileSize = fileSize / 2; // checkpointing data size
            tp.localBBmonitor = 0;
            tp.lock_localBBmonitor = 0;
            tp.readBuffer = readBuffer;
            tp.ckptRun = ckptRun;

            // Create the threads
            pthread_create(&wrtr, NULL, writer, &tp);

            ckptRun++;
        }
    }

    free(readBuffer);

    // Finalize the MPI environment. No more MPI calls can be made after this
    MPI_Finalize();
    return 0;
}
