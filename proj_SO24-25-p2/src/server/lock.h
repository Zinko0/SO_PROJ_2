#include <pthread.h>
#include <stddef.h>

#define MAX_TRIES 10

/// Locks the indexes of the hash with a write lock.
/// @param list_size size of the list of indexes
/// @param index_list list containing the indexes of the hash that are supposed
/// to be locked
/// @param rwlock the array of locks for each letter of the alphabet
/// @return 0 if the locks were locked successfully, 1 otherwise
int write_lock_table(size_t list_size, int *index_list, pthread_rwlock_t *rwlock);

/// Locks the indexes of the hash with a read lock.
/// @param list_size size of the list of indexes
/// @param index_list list containing the indexes of the hash that are supposed
/// to be locked
/// @param rwlock the array of locks for each letter of the alphabet
/// @return 0 if the locks were locked successfully, 1 otherwise
int read_lock_table(size_t list_size, int *index_list, pthread_rwlock_t *rwlock);

/// Unlocks the indexes of the hash.
/// @param list_size size of the list of indexes
/// @param index_list list containing the indexes of the hash that are supposed
/// to be unlocked
/// @param rwlock the array of locks for each letter of the alphabet
/// @return 0 if the locks were unlocked successfully, 1 otherwise
int rw_unlock_table(size_t list_size, int *index_list, pthread_rwlock_t *rwlock);