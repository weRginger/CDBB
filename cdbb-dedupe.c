// Author: Ziqi Fan
// cdbb-dedupe.c: Collaborative Distributed Burst Buffer with Data Deduplication
//

#define _XOPEN_SOURCE 500 /* Enable certain library functions (strdup) on linux.  See feature_test_macros(7) */

#include <mpi.h>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <time.h>
#include <pthread.h>
#include <limits.h>
#include <openssl/sha.h>

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

#define CHUNK_SIZE 32*1024 // dedupe use fixed chunking; size is 32 KB

//unsigned long burstBufferMaxSize = 3145728; // 3MB = 3*1024*1024
unsigned long burstBufferMaxSize = 3221225472; // 3GB = 3*1024*1024*1024

struct threadParams {
    int rank; // the rank of current process
    int totalRank; // the total number of ranks (processes)
    char* burstBuffer;
    int size; // the size of one burst buffer
    unsigned long fileSize; // the size of incoming data;
    unsigned long localBBmonitor;
    pthread_mutex_t* lock_localBBmonitor; // lock for localBBmonitor
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

pthread_mutex_t lock;

// start: hash table implementation
//
//
struct entry_s {
    char *key;
    char *value;
    struct entry_s *next;
};

typedef struct entry_s entry_t;

struct hashtable_s {
    int size;
    struct entry_s **table;
};

typedef struct hashtable_s hashtable_t;


/* Create a new hashtable. */
hashtable_t *ht_create( int size ) {

    hashtable_t *hashtable = NULL;
    int i;

    if( size < 1 ) return NULL;

    /* Allocate the table itself. */
    if( ( hashtable = malloc( sizeof( hashtable_t ) ) ) == NULL ) {
        return NULL;
    }

    /* Allocate pointers to the head nodes. */
    if( ( hashtable->table = malloc( sizeof( entry_t * ) * size ) ) == NULL ) {
        return NULL;
    }
    for( i = 0; i < size; i++ ) {
        hashtable->table[i] = NULL;
    }

    hashtable->size = size;

    return hashtable;
}

/* Hash a string for a particular hash table. */
int ht_hash( hashtable_t *hashtable, char *key ) {

    unsigned long int hashval;
    int i = 0;

    /* Convert our string to an integer */
    while( hashval < ULONG_MAX && i < strlen( key ) ) {
        hashval = hashval << 8;
        hashval += key[ i ];
        i++;
    }

    return hashval % hashtable->size;
}

/* Create a key-value pair. */
entry_t *ht_newpair( char *key, char *value ) {
    entry_t *newpair;

    if( ( newpair = malloc( sizeof( entry_t ) ) ) == NULL ) {
        return NULL;
    }

    if( ( newpair->key = strdup( key ) ) == NULL ) {
        return NULL;
    }

    if( ( newpair->value = strdup( value ) ) == NULL ) {
        return NULL;
    }

    newpair->next = NULL;

    return newpair;
}

/* Insert a key-value pair into a hash table. */
void ht_set( hashtable_t *hashtable, char *key, char *value ) {
    int bin = 0;
    entry_t *newpair = NULL;
    entry_t *next = NULL;
    entry_t *last = NULL;

    bin = ht_hash( hashtable, key );

    next = hashtable->table[ bin ];

    while( next != NULL && next->key != NULL && strcmp( key, next->key ) > 0 ) {
        last = next;
        next = next->next;
    }

    /* There's already a pair.  Let's replace that string. */
    if( next != NULL && next->key != NULL && strcmp( key, next->key ) == 0 ) {

        free( next->value );
        next->value = strdup( value );

        /* Nope, could't find it.  Time to grow a pair. */
    } else {
        newpair = ht_newpair( key, value );

        /* We're at the start of the linked list in this bin. */
        if( next == hashtable->table[ bin ] ) {
            newpair->next = next;
            hashtable->table[ bin ] = newpair;

            /* We're at the end of the linked list in this bin. */
        } else if ( next == NULL ) {
            last->next = newpair;

            /* We're in the middle of the list. */
        } else  {
            newpair->next = next;
            last->next = newpair;
        }
    }
}

/* Retrieve a key-value pair from a hash table. */
char *ht_get( hashtable_t *hashtable, char *key ) {
    int bin = 0;
    entry_t *pair;

    bin = ht_hash( hashtable, key );

    /* Step through the bin, looking for our value. */
    pair = hashtable->table[ bin ];
    while( pair != NULL && pair->key != NULL && strcmp( key, pair->key ) > 0 ) {
        pair = pair->next;
    }

    char *notFound = "Key not found!";

    /* Did we actually find anything? */
    if( pair == NULL || pair->key == NULL || strcmp( key, pair->key ) != 0 ) {
        return notFound;

    } else {
        return pair->value;
    }

}
//
//
// end: hash table implementation

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

        dbg_print("BB producer %d: receive %lu amount of data, localBBmonitor is %lu\n", tp->rank, tp->fileSize, tp->localBBmonitor);
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
            fwrite(tp->burstBuffer, 1, CHUNK_SIZE, fp); // each time write one chunk to PFS
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

    // initiate hash table for dedupe
    hashtable_t *hashtable = ht_create(65536);

    // using MPI timer to get the start and end time
    double timeStart, timeEnd;

    MPI_Barrier(MPI_COMM_WORLD);

    timeStart = MPI_Wtime();

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
// [IMPORTANT] set comsumer drain rate, corresponding to dedupe
// [IMPORTANT] set comsumer drain rate, corresponding to dedupe
// [IMPORTANT] set comsumer drain rate, corresponding to dedupe
        tp.fileSize = fileSize/10;
        tp.localBBmonitor = 0;
        tp.lock_localBBmonitor = &lock_localBBmonitor;

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
    else if (rank < size/2) {
        // start dedupe process
        //
        //
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
            //printf("num %d SHA1 is %s\n", num++, buf);

            pthread_mutex_lock(&lock);
            char* val = ht_get(hashtable, buf);
            //printf( "ht_get(%s): %s\n", buf, val);
            if(strcmp(val, "Key not found!") == 0) {
                ht_set(hashtable, buf, "duplicate");
            }
            pthread_mutex_unlock(&lock);

            offset += CHUNK_SIZE;
        }

// [IMPORTANT] set duplication rate
// [IMPORTANT] set duplication rate
// [IMPORTANT] set duplication rate
        fileSize = fileSize/10;

        //
        //
        // end of dedupe process

        int BBmonitorRank = 0; // BB monitor rank

        // tell BB monitor rank I am a writer
        int senderID = 1;
        MPI_Send(&senderID, 1, MPI_INT, BBmonitorRank, 0, MPI_COMM_WORLD);

        // tell BB monitor how much data I want to write
        MPI_Send(&fileSize, 1, MPI_UNSIGNED_LONG, BBmonitorRank, 1, MPI_COMM_WORLD);

        // 1 means space left in at least one BB, may not be local BB
        int checkResult;
        MPI_Recv(&checkResult, 1, MPI_INT, MPI_ANY_SOURCE, 2, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

        // the returned BB rank number could be local BB or a remote BB
        int returnedBBrank2send;
        MPI_Recv(&returnedBBrank2send, 1, MPI_INT, MPI_ANY_SOURCE, 3, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

        // there is enough space left in local BB or remote BB
        if(checkResult == 1) {
            // tell BB how much data I want to write
            MPI_Send(&fileSize, 1, MPI_UNSIGNED_LONG, returnedBBrank2send, 4, MPI_COMM_WORLD);

            // send to real data
            MPI_Send(readBuffer, fileSize, MPI_CHAR, returnedBBrank2send, 5, MPI_COMM_WORLD);
            dbg_print("Writer %d: send %lu amount of data to BB on rank %d\n", rank, fileSize, returnedBBrank2send);
        }
        else {
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

            dbg_print("Writer %d: Not enough space left in any BBs -> write %lu to PFS\n", rank, fileSize);
        }
    }
    // the rest half of ranks do nothing
    else {
        // writer processes do not expose BBmoniror memory in the window
        // [!!!] Note that if not do this, the whole program will hang
        //MPI_Win_create(NULL, 0, 1, MPI_INFO_NULL, MPI_COMM_WORLD, &win);
    }

    timeEnd = MPI_Wtime();
    if(rank == 0) {
        printf( "$$ Elapsed time for BB monitor rank %d is %f\n", rank, timeEnd - timeStart );
    }
    else if(rank % 8 == 7) {
        printf( "$$ Elapsed time for BB rank %d is %f\n", rank, timeEnd - timeStart );
    }
    else {
        printf( "$$ Elapsed time for writer rank %d is %f\n", rank, timeEnd - timeStart );
    }

    free(readBuffer);

    // Finalize the MPI environment. No more MPI calls can be made after this
    MPI_Finalize();
    return 0;
}
