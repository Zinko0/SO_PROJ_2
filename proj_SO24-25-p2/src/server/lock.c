#include "lock.h"
#include <pthread.h>

int write_lock_table(size_t list_size, int *index_list,
                     pthread_rwlock_t *rwlock) {
  int tries = 0;
  for (size_t i = 0; i < list_size; i++) {
    if (pthread_rwlock_wrlock(&(rwlock[index_list[i]])) == -1) {
      return -1;  
    }
  }
  return 0;
}

int read_lock_table(size_t list_size, int *index_list,
                    pthread_rwlock_t *rwlock) {
  int tries = 0;
  for (size_t i = 0; i < list_size; i++) {
    if (pthread_rwlock_rdlock(&(rwlock[index_list[i]])) == -1) {
      return -1;
    }
  }
  return 0;
}

int rw_unlock_table(size_t list_size, int *index_list,
                    pthread_rwlock_t *rwlock) {
  int tries = 0;
  for (size_t i = 0; i < list_size; i++) {
    if (pthread_rwlock_unlock(&(rwlock[index_list[i]])) == -1) {
      return -1;
    }
  }
  return 0;
}