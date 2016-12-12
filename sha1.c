// Author: Ziqi Fan
// sha1.c: added dedupe with sha1 and fixed chunking to local distributed burst buffer
//

#define _XOPEN_SOURCE 500 /* Enable certain library functions (strdup) on linux.  See feature_test_macros(7) */

#include <mpi.h>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <time.h>
#include <limits.h>
#include <openssl/sha.h>
#include <pthread.h>

// dedupe use fixed chunking; size is 32 KB
#define CHUNK_SIZE 32*1024

pthread_mutex_t lock;

// start: hash table implementation
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
// end: hash table implementation


unsigned long fsize(char* file)
{
    FILE * f = fopen(file, "r");
    fseek(f, 0, SEEK_END);
    unsigned long len = (unsigned long)ftell(f);
    fclose(f);
    return len;
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

    // initiate hash table
    hashtable_t *hashtable = ht_create( 65536 );

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
        printf("$This is a writer process.\n");

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
            //printf("num %d SHA1 is %s\n", num++, buf);

            pthread_mutex_lock(&lock);
            char* val = ht_get(hashtable, buf);
            //printf( "ht_get(%s): %s\n", buf, val);
            if(strcmp(val, "Key not found!") == 0) {
                //printf( "debug: line %d\n", __LINE__ );
                ht_set(hashtable, buf, "duplicate");
            }
            pthread_mutex_unlock(&lock);

            offset += CHUNK_SIZE;
        }

        // send
        MPI_Send(readBuffer, fileSize, MPI_CHAR, (rank/8)*8, 0, MPI_COMM_WORLD);
    }
    MPI_Barrier(MPI_COMM_WORLD);

    timeEnd = MPI_Wtime();
    printf( "Elapsed time for rank %d is %f\n", rank, timeEnd - timeStart );

    free(readBuffer);

    // Finalize the MPI environment. No more MPI calls can be made after this
    MPI_Finalize();

    return 0;
}
