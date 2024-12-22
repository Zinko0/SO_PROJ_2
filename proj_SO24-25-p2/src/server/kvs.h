#ifndef KEY_VALUE_STORE_H
#define KEY_VALUE_STORE_H
#define TABLE_SIZE 26

#include <stddef.h>
#include <pthread.h>

#include "constants.h"

typedef struct KeyNode {
    char *key;
    char *value;
    int subscribers_fds[MAX_CLIENTS];
    struct KeyNode *next;
} KeyNode;

typedef struct HashTable {
    KeyNode *table[TABLE_SIZE];
    pthread_rwlock_t tablelock;
} HashTable;

/// Creates a new KVS hash table.
/// @return Newly created hash table, NULL on failure
struct HashTable *create_hash_table();

int hash(const char *key); 

// Writes a key value pair in the hash table.
// @param ht The hash table.
// @param key The key.
// @param value The value.
// @return 0 if successful.
int write_pair(HashTable *ht, const char *key, const char *value);

// Reads the value of a given key.
// @param ht The hash table.
// @param key The key.
// return the value if found, NULL otherwise.
char* read_pair(HashTable *ht, const char *key);

/// Deletes a pair from the table.
/// @param ht Hash table to read from.
/// @param key Key of the pair to be deleted.
/// @return 0 if the node was deleted successfully, 1 otherwise.
int delete_pair(HashTable *ht, const char *key);

/// Frees the hashtable.
/// @param ht Hash table to be deleted.
void free_table(HashTable *ht);

/// @brief 
/// @param ht 
/// @param key 
/// @param fd 
/// @return 0 if the key was not found, 1 if the subscription was added successfully, -1 if there is no space for more subscribers 
int write_subscription(HashTable *ht, const char *key, int fd);

/// @brief 
/// @param ht 
/// @param key 
/// @param fd 
/// @return 1 if the key was not found, 0 if the subscription was deleted successfully 
int delete_subscription(HashTable *ht, const char *key, int fd);


#endif  // KVS_H
