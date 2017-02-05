// Author: Ziqi Fan
// ldbb.c: local distributed burst buffer
//

#include <mpi.h>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <time.h>
#include <pthread.h>
#include <limits.h>
#include <stdbool.h>
#include <assert.h>

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

unsigned long burstBufferMaxSize = 4294967296; // 4GB = 4*1024*1024*1024

unsigned long burstBufferOffset = 0;

pthread_mutex_t lock_burstBufferOffset; // lock for burstBufferOffset

struct threadParams {
    int rank; // the rank of current process
    char* burstBuffer;
    int size; // the size of one burst buffer
    int fileSize; // the size of incoming data
    char* readBuffer; // checkpointing data buffer to write
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

// a FIFO queue for producer and consumer to manage accepting and draining data
// start

#define MAX 2000

unsigned long queue[MAX];
int front = 0;
int rear = -1;
int itemCount = 0;

unsigned long peek() {
    return queue[front];
}

bool isEmpty() {
    return itemCount == 0;
}

bool isFull() {
    return itemCount == MAX;
}

int size() {
    return itemCount;
}

void insert(unsigned long data) {
    if(!isFull()) {
        if(rear == MAX-1) {
            rear = -1;
        }
        queue[++rear] = data;
        itemCount++;
    }
    else {
        printf("FIFO queue is full. Enlarge it!!!\n");
    }
}

unsigned long removeData() {
    unsigned long data = queue[front++];

    if(front == MAX) {
        front = 0;
    }

    itemCount--;
    return data;
}
// end
// FIFO queue implementation

void* producer(void *ptr) {
    struct threadParams *tp = ptr;
    MPI_Status status;
    int i;

    while (1) {
        // receive from writer how much data it wants to write
        int incomingDataSize;
        MPI_Recv(&incomingDataSize, 1, MPI_INT, MPI_ANY_SOURCE, 0, MPI_COMM_WORLD, &status);
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

            pthread_mutex_lock(&lock_burstBufferOffset);

            MPI_Send(&checkResult, 1, MPI_INT, status.MPI_SOURCE, 1, MPI_COMM_WORLD);
            MPI_Recv(tp->burstBuffer, incomingDataSize, MPI_CHAR, MPI_ANY_SOURCE, 2, MPI_COMM_WORLD, &status);
            burstBufferOffset += incomingDataSize;

            insert(incomingDataSize);

            pthread_mutex_unlock(&lock_burstBufferOffset);

            dbg_print("BB producer %d: burst buffer receive %lu data from rank %d. burstBufferOffset is %lu\n", tp->rank, incomingDataSize, status.MPI_SOURCE, burstBufferOffset);
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
            pthread_mutex_lock(&lock_burstBufferOffset);

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

            assert(!isEmpty());
            unsigned long drainSize = removeData();

            fwrite(tp->burstBuffer, 1, drainSize, fp);
            fclose(fp);

            burstBufferOffset -= drainSize;

            pthread_mutex_unlock(&lock_burstBufferOffset);

            dbg_print("BB consumer %d: drained %lu amount of data to PFS, burstBufferOffset is %lu\n", tp->rank, drainSize, burstBufferOffset);
        }
    }
    pthread_exit(0);
}

void* writer(void *ptr) {
    // using MPI timer to get the start and end time
    double timeStart, timeEnd;
    timeStart = MPI_Wtime();

    struct threadParams *tp = ptr;

    // before sending the real data, send fileSize to BB to check how much space left
    MPI_Send(&tp->fileSize, 1, MPI_INT, (tp->rank/8)*8 + 7, 0, MPI_COMM_WORLD);
    int checkResult;
    MPI_Recv(&checkResult, 1, MPI_INT, MPI_ANY_SOURCE, 1, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
    // only there is enough space left in BB, we will send the real data to it.
    if(checkResult == 1) {
        MPI_Send(tp->readBuffer, tp->fileSize, MPI_CHAR, (tp->rank/8)*8 + 7, 2, MPI_COMM_WORLD);
        dbg_print("Writer %d: send %lld amount of data to BB\n", tp->rank, tp->fileSize);
    }
    else {
        dbg_print("Writer %d: Not enough space left in BB -> write to PFS\n", tp->rank);

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
        fwrite(tp->readBuffer, 1, tp->fileSize, fp);
        fclose(fp);
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

    FILE *fp;

    fp = fopen("/home/dudh/fanxx234/CDBB/ddFile.input", "r");
    if(fp == NULL) {
        printf("cannot open file for read. Exit!\n");
        return 1;
    }

    // read file to buffer
    unsigned long fileSize = fsize("/home/dudh/fanxx234/CDBB/ddFile.input");
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

    // Print off a hello world message
    dbg_print("Hello world from processor %s, rank %d out of %d processors\n", processor_name, rank, size);

    MPI_Barrier(MPI_COMM_WORLD);

    // for CDBB, rank 0 is BB coordinator rank
    // however for LDBB, there is no BB coordinator rank, so we just leave it here
    if(rank == 0) {
        while(1) {}
    }
    // BB rank
    else if(rank % 8 == 7) {
        char *burstBuffer;
        burstBuffer = (char*) malloc(sizeof(char) * 4 *  1024 * 1024 *1024 ); // malloc 4GB as the local burst buffer

        pthread_t pro, con;

        struct threadParams tp;
        tp.rank = rank;
        tp.burstBuffer = burstBuffer;
        tp.size = burstBufferMaxSize;
        tp.fileSize = fileSize;
        tp.readBuffer = readBuffer;
        tp.ckptRun = 0;

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
    // 1st application from with 64 ranks from 1 to 73
    else if (rank >= 1 && rank <= 73) {
        int ckptRun = 0; // keep track how many ckpts have been performed

        sleep(0);
        dbg_print("1st application start after sleep for 00 seconds\n");

        while(1) {
            pthread_t wrtr;

            struct threadParams tp;
            tp.rank = rank;
            tp.burstBuffer = NULL;
            tp.size = burstBufferMaxSize;
            tp.fileSize = 167772160; // checkpointing data size
            tp.readBuffer = readBuffer;
            tp.ckptRun = ckptRun;

            // Create the threads
            pthread_create(&wrtr, NULL, writer, &tp);

            ckptRun++;

            sleep(600); // checkpointing frequency
        }
    }
    // 2nd application from with 64 ranks from 74 to 146
    else if (rank >= 74 && rank <= 146) {
        int ckptRun = 0; // keep track how many ckpts have been performed

        sleep(120);
        dbg_print("2nd application start after sleep for 120 seconds\n");

        while(1) {
            pthread_t wrtr;

            struct threadParams tp;
            tp.rank = rank;
            tp.burstBuffer = NULL;
            tp.size = burstBufferMaxSize;
            tp.fileSize = 285212672; // checkpointing data size
            tp.readBuffer = readBuffer;
            tp.ckptRun = ckptRun;

            // Create the threads
            pthread_create(&wrtr, NULL, writer, &tp);

            ckptRun++;

            sleep(600); // checkpointing frequency
        }
    }
    // 3rd application from with 64 ranks from 147 to 219
    else if (rank >= 147 && rank <= 219) {
        int ckptRun = 0; // keep track how many ckpts have been performed

        sleep(240);
        dbg_print("3rd application start after sleep for 240 seconds\n");

        while(1) {
            pthread_t wrtr;

            struct threadParams tp;
            tp.rank = rank;
            tp.burstBuffer = NULL;
            tp.size = burstBufferMaxSize;
            tp.fileSize = 654311424; // checkpointing data size
            tp.readBuffer = readBuffer;
            tp.ckptRun = ckptRun;

            // Create the threads
            pthread_create(&wrtr, NULL, writer, &tp);

            ckptRun++;

            sleep(600); // checkpointing frequency
        }
    }
    // 4th application from with 64 ranks from 220 to 292
    else if (rank >= 220 && rank <= 292) {
        int ckptRun = 0; // keep track how many ckpts have been performed

        sleep(360);
        dbg_print("4th application start after sleep for 360 seconds\n");

        while(1) {
            pthread_t wrtr;

            struct threadParams tp;
            tp.rank = rank;
            tp.burstBuffer = NULL;
            tp.size = burstBufferMaxSize;
            tp.fileSize = 1660944384; // checkpointing data size
            tp.readBuffer = readBuffer;
            tp.ckptRun = ckptRun;

            // Create the threads
            pthread_create(&wrtr, NULL, writer, &tp);

            ckptRun++;

            sleep(600); // checkpointing frequency
        }
    }
    // 5th application from with 64 ranks from 293 to 365
    else if (rank >= 293 && rank <= 365) {
        int ckptRun = 0; // keep track how many ckpts have been performed

        sleep(480);
        dbg_print("5th application start after sleep for 480 seconds\n");

        while(1) {
            pthread_t wrtr;

            struct threadParams tp;
            tp.rank = rank;
            tp.burstBuffer = NULL;
            tp.size = burstBufferMaxSize;
            tp.fileSize = 2147483646; // checkpointing data size
            tp.readBuffer = readBuffer;
            tp.ckptRun = ckptRun;

            // Create the threads
            pthread_create(&wrtr, NULL, writer, &tp);

            ckptRun++;

            sleep(600); // checkpointing frequency
        }
    }
    // rest ranks do nothing
    else {
        dbg_print("Rank %d does nothing\n", rank);
    }

    free(readBuffer);

    // Finalize the MPI environment. No more MPI calls can be made after this
    MPI_Finalize();
    return 0;
}
