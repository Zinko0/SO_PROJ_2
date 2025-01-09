#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

#include "constants.h"
#include "io.h"
#include "kvs.h"
#include "operations.h"
#include "lock.h"

static struct HashTable *kvs_table = NULL;

/// Calculates a timespec from a delay in milliseconds.
/// @param delay_ms Delay in milliseconds.
/// @return Timespec with the given delay.
static struct timespec delay_to_timespec(unsigned int delay_ms) {
  return (struct timespec){delay_ms / 1000, (delay_ms % 1000) * 1000000};
}

int hash_key_sort(const void *a, const void *b) {
  return *(int *)a - *(int *)b;
}

int *set_of_keys(int *hash_keys, size_t num_pairs,
                 size_t *individual_keys_length) {
  int *individual_hash_keys = malloc(sizeof(int) * num_pairs);
  if (individual_hash_keys == NULL) {
    fprintf(stderr, "Failed to allocate memory\n");
    return NULL;
  }
  size_t individual_key_indexes = 1;
  individual_hash_keys[0] = hash_keys[0];

  for (size_t i = 1; i < num_pairs; i++) {
    if (hash_keys[i] != hash_keys[i - 1]) {
      individual_hash_keys[individual_key_indexes] = hash_keys[i];
      individual_key_indexes++;
    }
  }

  *individual_keys_length = individual_key_indexes;
  return individual_hash_keys;
}

int kvs_init() {
  if (kvs_table != NULL) {
    fprintf(stderr, "KVS state has already been initialized\n");
    return 1;
  }

  kvs_table = create_hash_table();
  return kvs_table == NULL;
}

int kvs_terminate() {
  if (kvs_table == NULL) {
    fprintf(stderr, "KVS state must be initialized\n");
    return 1;
  }

  free_table(kvs_table);
  kvs_table = NULL;
  return 0;
}

int kvs_write(size_t num_pairs, char keys[][MAX_STRING_SIZE],
              char values[][MAX_STRING_SIZE]) {
  if (kvs_table == NULL) {
    fprintf(stderr, "KVS state must be initialized\n");
    return 1;
  }

  int hash_keys[TABLE_SIZE]; 
  int* individual_hash_keys;
  size_t individual_keys_length;

  for (size_t i = 0; i < num_pairs; i++) {
    hash_keys[i] = hash(keys[i]);
  }
  // Sorting the hash keys
  qsort(hash_keys, num_pairs, sizeof(int), hash_key_sort);

  individual_hash_keys = set_of_keys(hash_keys, num_pairs, &individual_keys_length);

  if (write_lock_table(individual_keys_length, individual_hash_keys,
                        kvs_table->tablelock) != 0) {
    fprintf(stderr, "Failed to writelock hashtable\n");
    return 1;
  };

  for (size_t i = 0; i < num_pairs; i++) {
    if (write_pair(kvs_table, keys[i], values[i]) != 0) {
      fprintf(stderr, "Failed to write key pair (%s,%s)\n", keys[i], values[i]);
    }
  }

  if (rw_unlock_table(individual_keys_length, individual_hash_keys,
                      kvs_table->tablelock) == -1) {
    fprintf(stderr, "Failed to unlock hashtable\n");
    return 1;
  }

  free(individual_hash_keys);
  return 0;
}

int kvs_read(size_t num_pairs, char keys[][MAX_STRING_SIZE], int fd) {
  if (kvs_table == NULL) {
    fprintf(stderr, "KVS state must be initialized\n");
    return 1;
  }
  int hash_keys[TABLE_SIZE]; 
  int* individual_hash_keys;
  size_t individual_keys_length;

  for (size_t i = 0; i < num_pairs; i++) {
    hash_keys[i] = hash(keys[i]);
  }
  // Sorting the hash keys
  qsort(hash_keys, num_pairs, sizeof(int), hash_key_sort);

  individual_hash_keys = set_of_keys(hash_keys, num_pairs, &individual_keys_length);

  if (read_lock_table(individual_keys_length, individual_hash_keys,
                        kvs_table->tablelock) != 0) {
    fprintf(stderr, "Failed to writelock hashtable\n");
    return 1;
  };

  write_str(fd, "[");
  for (size_t i = 0; i < num_pairs; i++) {
    char *result = read_pair(kvs_table, keys[i]);
    char aux[MAX_STRING_SIZE];
    if (result == NULL) {
      snprintf(aux, MAX_STRING_SIZE, "(%s,KVSERROR)", keys[i]);
    } else {
      snprintf(aux, MAX_STRING_SIZE, "(%s,%s)", keys[i], result);
    }
    write_str(fd, aux);
    free(result);
  }
  write_str(fd, "]\n");

  if (rw_unlock_table(individual_keys_length, individual_hash_keys,
                      kvs_table->tablelock) == -1) {
    fprintf(stderr, "Failed to unlock hashtable\n");
    return 1;
  }

  free(individual_hash_keys);
  return 0;
}

int kvs_delete(size_t num_pairs, char keys[][MAX_STRING_SIZE], int fd) {
  if (kvs_table == NULL) {
    fprintf(stderr, "KVS state must be initialized\n");
    return 1;
  }
  
  int hash_keys[TABLE_SIZE]; 
  int* individual_hash_keys;
  size_t individual_keys_length;

  for (size_t i = 0; i < num_pairs; i++) {
    hash_keys[i] = hash(keys[i]);
  }
  // Sorting the hash keys
  qsort(hash_keys, num_pairs, sizeof(int), hash_key_sort);

  individual_hash_keys = set_of_keys(hash_keys, num_pairs, &individual_keys_length);

  if (write_lock_table(individual_keys_length, individual_hash_keys,
                        kvs_table->tablelock) != 0) {
    fprintf(stderr, "Failed to writelock hashtable\n");
    return 1;
  };

  int aux = 0;
  for (size_t i = 0; i < num_pairs; i++) {
    if (delete_pair(kvs_table, keys[i]) != 0) {
      if (!aux) {
        write_str(fd, "[");
        aux = 1;
      }
      char str[MAX_STRING_SIZE];
      snprintf(str, MAX_STRING_SIZE, "(%s,KVSMISSING)", keys[i]);
      write_str(fd, str);
    }
  }
  if (aux) {
    write_str(fd, "]\n");
  }

  if (rw_unlock_table(individual_keys_length, individual_hash_keys,
                      kvs_table->tablelock) == -1) {
    fprintf(stderr, "Failed to unlock hashtable\n");
    return 1;
  }

  free(individual_hash_keys);
  return 0;
}

void kvs_show(int fd) {
  if (kvs_table == NULL) {
    fprintf(stderr, "KVS state must be initialized\n");
    return;
  }
  int hash_keys[TABLE_SIZE]; 
  
  for (int i = 0; i < TABLE_SIZE; i++) {
    hash_keys[i] = i;
  }
  if (read_lock_table(TABLE_SIZE, hash_keys, kvs_table->tablelock) == -1) {
    fprintf(stderr, "Failed to readlock hashtable\n");
    return;
  }

  char aux[MAX_STRING_SIZE];
  
  for (int i = 0; i < TABLE_SIZE; i++) {
    KeyNode *keyNode = kvs_table->table[i]; // Get the next list head
    while (keyNode != NULL) {
      snprintf(aux, MAX_STRING_SIZE, "(%s, %s)\n", keyNode->key, keyNode->value);
      write_str(fd, aux);
      keyNode = keyNode->next; // Move to the next node of the list
    }
  }

  if (rw_unlock_table(TABLE_SIZE, hash_keys, kvs_table->tablelock) == -1) {
    fprintf(stderr, "Failed to unlock hashtable\n");
    return;
  }
}

int kvs_backup(size_t num_backup,char* job_filename , char* directory) {
  pid_t pid;
  char bck_name[50];
  snprintf(bck_name, sizeof(bck_name), "%s/%s-%ld.bck", directory, strtok(job_filename, "."),
           num_backup);

  int hash_keys[TABLE_SIZE]; 
  
  for (int i = 0; i < TABLE_SIZE; i++) {
    hash_keys[i] = i;
  }
  
  if (read_lock_table(TABLE_SIZE, hash_keys, kvs_table->tablelock) == -1) {
    fprintf(stderr, "Failed to readlock hashtable\n");
    return 1;
  }
  pid = fork();
  if (rw_unlock_table(TABLE_SIZE, hash_keys, kvs_table->tablelock) == -1) {
    fprintf(stderr, "Failed to unlock hashtable\n");
    return 1;
  }
  if (pid == 0) {
    // functions used here have to be async signal safe, since this
    // fork happens in a multi thread context (see man fork)
    int fd = open(bck_name, O_WRONLY | O_CREAT | O_TRUNC, 0666);
    for (int i = 0; i < TABLE_SIZE; i++) {
      KeyNode *keyNode = kvs_table->table[i]; // Get the next list head
      while (keyNode != NULL) {
        char aux[MAX_STRING_SIZE];
        aux[0] = '(';
        size_t num_bytes_copied = 1; // the "("
        // the - 1 are all to leave space for the '/0'
        num_bytes_copied += strn_memcpy(aux + num_bytes_copied,
                                        keyNode->key, MAX_STRING_SIZE - num_bytes_copied - 1);
        num_bytes_copied += strn_memcpy(aux + num_bytes_copied,
                                        ", ", MAX_STRING_SIZE - num_bytes_copied - 1);
        num_bytes_copied += strn_memcpy(aux + num_bytes_copied,
                                        keyNode->value, MAX_STRING_SIZE - num_bytes_copied - 1);
        num_bytes_copied += strn_memcpy(aux + num_bytes_copied,
                                        ")\n", MAX_STRING_SIZE - num_bytes_copied - 1);
        aux[num_bytes_copied] = '\0';
        write_str(fd, aux);
        keyNode = keyNode->next; // Move to the next node of the list
      }
    }
    exit(1);
  } else if (pid < 0) {
    return -1;
  }
  return 0;
}

void kvs_wait(unsigned int delay_ms) {
  struct timespec delay = delay_to_timespec(delay_ms);
  nanosleep(&delay, NULL);
}

char subscribe(char *key, int fd) {
  if (kvs_table == NULL) {
    fprintf(stderr, "KVS state must be initialized\n");
    return '1';
  }
  
  int hash_key = hash(key);

  pthread_rwlock_wrlock(&kvs_table->tablelock[hash_key]);

  if (write_subscription(kvs_table, key, fd) != 0) {
    pthread_rwlock_unlock(&kvs_table->tablelock[hash_key]);
    return '0';
  }
  
  pthread_rwlock_unlock(&kvs_table->tablelock[hash_key]);

  return '1';
}

char unsubscribe(char *key, int fd) {
  if (kvs_table == NULL) {
    fprintf(stderr, "KVS state must be initialized\n");
    return '1';
  }

  int hash_key = hash(key);
  
  pthread_rwlock_wrlock(&kvs_table->tablelock[hash_key]);

  if (delete_subscription(kvs_table, key, fd) != 0) {
    pthread_rwlock_unlock(&kvs_table->tablelock[hash_key]);
    return '1';
  }
  
  pthread_rwlock_unlock(&kvs_table->tablelock[hash_key]);
  return '0';
}

char disconnect(int fd) {
  //percorrer a lista de keys e dar delete_subscription
  KeyNode *keyNode;

  int* hash_keys = malloc(sizeof(int) * TABLE_SIZE);
  
  for (int i = 0; i < TABLE_SIZE; i++) {
    hash_keys[i] = i;
  }
  if (write_lock_table(TABLE_SIZE, hash_keys, kvs_table->tablelock) == -1) {
    fprintf(stderr, "Failed to readlock hashtable\n");
    return '1';
  }

  for(int i = 0; i < TABLE_SIZE; i++){
    keyNode = kvs_table->table[i];
    while(keyNode != NULL){
      for(int j = 0; j < MAX_CLIENTS; j++){
        if(keyNode->subscribers_fds[j] == fd){
          keyNode->subscribers_fds[j] = 0;
        }
      }
      keyNode = keyNode->next;
    }
  }

  if (rw_unlock_table(TABLE_SIZE, hash_keys, kvs_table->tablelock) == -1) {
    fprintf(stderr, "Failed to unlock hashtable\n");
    return '1';
  }

  free(hash_keys);
  
  return '0';
}

int disconnect_all() {
  //percorrer a lista de keys e dar delete_subscription
  KeyNode *keyNode;

  int* hash_keys = malloc(sizeof(int) * TABLE_SIZE);
  
  for (int i = 0; i < TABLE_SIZE; i++) {
    hash_keys[i] = i;
  }
  if (write_lock_table(TABLE_SIZE, hash_keys, kvs_table->tablelock) == -1) {
    fprintf(stderr, "Failed to readlock hashtable\n");
    return 1;
  }

  for(int i = 0; i < TABLE_SIZE; i++){
    keyNode = kvs_table->table[i];
    while(keyNode != NULL){
      for(int j = 0; j < MAX_CLIENTS; j++){
        keyNode->subscribers_fds[j] = 0;
      }
      keyNode = keyNode->next;
    }
  }

  if (rw_unlock_table(TABLE_SIZE, hash_keys, kvs_table->tablelock) == -1) {
    fprintf(stderr, "Failed to unlock hashtable\n");
    return 1;
  }

  free(hash_keys);
    
  return 0;
}