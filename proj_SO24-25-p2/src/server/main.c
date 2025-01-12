#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <semaphore.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

#include "constants.h"
#include "io.h"
#include "operations.h"
#include "parser.h"
#include "pthread.h"
#include "src/common/constants.h"
#include "src/common/io.h"
#include "src/common/protocol.h"

struct SharedData {
  DIR* dir;
  char* dir_name;
  pthread_mutex_t directory_mutex;
};

struct PipeData {
  char req_pipe_path[MAX_PIPE_PATH_LENGTH];
  char resp_pipe_path[MAX_PIPE_PATH_LENGTH];
  char notif_pipe_path[MAX_PIPE_PATH_LENGTH];
};

struct ActiveClients {
  int req_fd;
  char req_pipe_path[MAX_PIPE_PATH_LENGTH];
  int resp_fd;
  char resp_pipe_path[MAX_PIPE_PATH_LENGTH];
  int notif_fd;
  char notif_pipe_path[MAX_PIPE_PATH_LENGTH];
};

struct ManagingClients {
  // the buffer can have whatever size.
  struct PipeData buffer[MAX_CLIENTS];
  size_t* read_index;
  int fifo_fd;
};

struct ActiveClients active_clients[MAX_CLIENTS];

sigset_t set_with_sigusr1;
int signal_received = 0;

pthread_mutex_t n_current_backups_lock = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t semExMut = PTHREAD_MUTEX_INITIALIZER;
sem_t productor_buffer;
sem_t consumer_buffer;

size_t active_backups = 0;  // Number of active backups
size_t max_backups;         // Maximum allowed simultaneous backups
size_t max_threads;         // Maximum allowed simultaneous threads

char* jobs_directory = NULL;

void initialize_global_sigset() {
  // Initialize the signal set
  sigemptyset(&set_with_sigusr1);
  sigaddset(&set_with_sigusr1, SIGUSR1);
}

void close_all_clients() {
  for (int i = 0; i < MAX_CLIENTS; i++) {
    if (active_clients[i].req_fd != 0) {
      close(active_clients[i].req_fd);
      unlink(active_clients[i].req_pipe_path);
      close(active_clients[i].resp_fd);
      unlink(active_clients[i].resp_pipe_path);
      close(active_clients[i].notif_fd);
      unlink(active_clients[i].notif_pipe_path);
      active_clients[i].req_fd = 0;
      active_clients[i].resp_fd = 0;
      active_clients[i].notif_fd = 0;
    }
  }
}

static void sigusr1_handler(int signo) {
  if (signal(SIGTERM, sigusr1_handler) == SIG_ERR) {
    exit(EXIT_FAILURE);
  }
  if (signo == SIGUSR1) {
    signal_received = 1;
  }
  return;
}

int filter_job_files(const struct dirent* entry) {
  const char* dot = strrchr(entry->d_name, '.');
  if (dot != NULL && strcmp(dot, ".job") == 0) {
    return 1;  // Keep this file (it has the .job extension)
  }
  return 0;
}

static int entry_files(const char* dir, struct dirent* entry, char* in_path, char* out_path) {
  const char* dot = strrchr(entry->d_name, '.');
  if (dot == NULL || dot == entry->d_name || strlen(dot) != 4 || strcmp(dot, ".job")) {
    return 1;
  }

  if (strlen(entry->d_name) + strlen(dir) + 2 > MAX_JOB_FILE_NAME_SIZE) {
    fprintf(stderr, "%s/%s\n", dir, entry->d_name);
    return 1;
  }

  strcpy(in_path, dir);
  strcat(in_path, "/");
  strcat(in_path, entry->d_name);

  strcpy(out_path, in_path);
  strcpy(strrchr(out_path, '.'), ".out");

  return 0;
}

static int run_job(int in_fd, int out_fd, char* filename) {
  size_t file_backups = 0;
  while (1) {
    char keys[MAX_WRITE_SIZE][MAX_STRING_SIZE] = {0};
    char values[MAX_WRITE_SIZE][MAX_STRING_SIZE] = {0};
    unsigned int delay;
    size_t num_pairs;

    switch (get_next(in_fd)) {
      case CMD_WRITE:
        num_pairs = parse_write(in_fd, keys, values, MAX_WRITE_SIZE, MAX_STRING_SIZE);
        if (num_pairs == 0) {
          write_str(STDERR_FILENO, "Invalid command. See HELP for usage\n");
          continue;
        }

        if (kvs_write(num_pairs, keys, values)) {
          write_str(STDERR_FILENO, "Failed to write pair\n");
        }
        break;

      case CMD_READ:
        num_pairs = parse_read_delete(in_fd, keys, MAX_WRITE_SIZE, MAX_STRING_SIZE);

        if (num_pairs == 0) {
          write_str(STDERR_FILENO, "Invalid command. See HELP for usage\n");
          continue;
        }

        if (kvs_read(num_pairs, keys, out_fd)) {
          write_str(STDERR_FILENO, "Failed to read pair\n");
        }
        break;

      case CMD_DELETE:
        num_pairs = parse_read_delete(in_fd, keys, MAX_WRITE_SIZE, MAX_STRING_SIZE);

        if (num_pairs == 0) {
          write_str(STDERR_FILENO, "Invalid command. See HELP for usage\n");
          continue;
        }

        if (kvs_delete(num_pairs, keys, out_fd)) {
          write_str(STDERR_FILENO, "Failed to delete pair\n");
        }
        break;

      case CMD_SHOW:
        kvs_show(out_fd);
        break;

      case CMD_WAIT:
        if (parse_wait(in_fd, &delay, NULL) == -1) {
          write_str(STDERR_FILENO, "Invalid command. See HELP for usage\n");
          continue;
        }

        if (delay > 0) {
          printf("Waiting %d seconds\n", delay / 1000);
          kvs_wait(delay);
        }
        break;

      case CMD_BACKUP:
        pthread_mutex_lock(&n_current_backups_lock);
        if (active_backups >= max_backups) {
          wait(NULL);
        } else {
          active_backups++;
        }
        pthread_mutex_unlock(&n_current_backups_lock);
        int aux = kvs_backup(++file_backups, filename, jobs_directory);

        if (aux < 0) {
          write_str(STDERR_FILENO, "Failed to do backup\n");
        } else if (aux == 1) {
          return 1;
        }
        break;

      case CMD_INVALID:
        write_str(STDERR_FILENO, "Invalid command. See HELP for usage\n");
        break;

      case CMD_HELP:
        write_str(STDOUT_FILENO,
                  "Available commands:\n"
                  "  WRITE [(key,value)(key2,value2),...]\n"
                  "  READ [key,key2,...]\n"
                  "  DELETE [key,key2,...]\n"
                  "  SHOW\n"
                  "  WAIT <delay_ms>\n"
                  "  BACKUP\n"  // Not implemented
                  "  HELP\n");

        break;

      case CMD_EMPTY:
        break;

      case EOC:
        return 0;
    }
  }
}

// frees arguments
static void* get_file(void* arguments) {
  struct SharedData* thread_data = (struct SharedData*)arguments;
  DIR* dir = thread_data->dir;
  char* dir_name = thread_data->dir_name;

  if (pthread_mutex_lock(&thread_data->directory_mutex) != 0) {
    fprintf(stderr, "Thread failed to lock directory_mutex\n");
    return NULL;
  }

  struct dirent* entry;
  char in_path[MAX_JOB_FILE_NAME_SIZE], out_path[MAX_JOB_FILE_NAME_SIZE];
  while ((entry = readdir(dir)) != NULL) {
    if (entry_files(dir_name, entry, in_path, out_path)) {
      continue;
    }

    if (pthread_mutex_unlock(&thread_data->directory_mutex) != 0) {
      fprintf(stderr, "Thread failed to unlock directory_mutex\n");
      return NULL;
    }

    int in_fd = open(in_path, O_RDONLY);
    if (in_fd == -1) {
      write_str(STDERR_FILENO, "Failed to open input file: ");
      write_str(STDERR_FILENO, in_path);
      write_str(STDERR_FILENO, "\n");
      pthread_exit(NULL);
    }

    int out_fd = open(out_path, O_WRONLY | O_CREAT | O_TRUNC, 0666);
    if (out_fd == -1) {
      write_str(STDERR_FILENO, "Failed to open output file: ");
      write_str(STDERR_FILENO, out_path);
      write_str(STDERR_FILENO, "\n");
      pthread_exit(NULL);
    }

    int out = run_job(in_fd, out_fd, entry->d_name);

    close(in_fd);
    close(out_fd);

    if (out) {
      if (closedir(dir) == -1) {
        fprintf(stderr, "Failed to close directory\n");
        return 0;
      }

      exit(0);
    }

    if (pthread_mutex_lock(&thread_data->directory_mutex) != 0) {
      fprintf(stderr, "Thread failed to lock directory_mutex\n");
      return NULL;
    }
  }

  if (pthread_mutex_unlock(&thread_data->directory_mutex) != 0) {
    fprintf(stderr, "Thread failed to unlock directory_mutex\n");
    return NULL;
  }

  pthread_exit(NULL);
}

void assing_pipe_data(struct PipeData* buffer, size_t index, char* pipes_path) {
  strncpy(buffer[index].req_pipe_path, pipes_path, MAX_PIPE_PATH_LENGTH);
  strncpy(buffer[index].resp_pipe_path, pipes_path + MAX_PIPE_PATH_LENGTH, MAX_PIPE_PATH_LENGTH);
  strncpy(buffer[index].notif_pipe_path, pipes_path + (2 * MAX_PIPE_PATH_LENGTH), MAX_PIPE_PATH_LENGTH);
  return;
}

static void* managing_clients(void* arguments) {
  struct ManagingClients* buffer_data = (struct ManagingClients*)arguments;
  char buffer[1 + MAX_PIPE_PATH_LENGTH * 3];  // OP_CODE + 3 pipe paths
  size_t write_index = 0;

  // Creation of sigaction struct to repeatedly handle sigusr1
  struct sigaction sigaction_struct;
  memset(&sigaction_struct, 0, sizeof(sigaction_struct));

  sigaction_struct.sa_handler = sigusr1_handler;
  sigemptyset(&sigaction_struct.sa_mask);
  sigaction_struct.sa_flags = SA_RESTART; //SA_RESTART to handle multiple times

  // Unblock SIGUSR1, because it is blocked by default from main
  if (pthread_sigmask(SIG_UNBLOCK, &set_with_sigusr1, NULL) != 0) {
    perror("pthread_sigmask");
    return NULL;
  }
  // Sigaction to handle SIGUSR1 multiple times
  if (sigaction(SIGUSR1, &sigaction_struct, NULL) != 0) {
    perror("signal");
    return NULL;
  }

  // Is allways reading from the FIFO waiting for a client to connect
  while (1) {
    if (signal_received == 1) {
      close_all_clients();
      // disconnect all is not async signal safe, so we need to call it here
      disconnect_all();
      signal_received = 0;
    }
    if (read_all(buffer_data->fifo_fd, buffer, sizeof(buffer), NULL) == 1) {
      if (get_code(buffer[0]) == OP_CODE_CONNECT) {
        sem_wait(&productor_buffer);

        pthread_mutex_lock(&semExMut);

        assing_pipe_data(buffer_data->buffer, write_index, buffer + 1);

        write_index = (write_index + 1) % MAX_CLIENTS;

        pthread_mutex_unlock(&semExMut);

        sem_post(&consumer_buffer);
      }
    }
  }
  close(buffer_data->fifo_fd);
  pthread_exit(NULL);
}

void run_client_requests(int req_pipe_fd, int resp_pipe_fd, int notif_pipe_fd, int index) {
  char op_buffer[1];              // OP_CODE + space
  char key[MAX_STRING_SIZE + 1];  // key + \0
  char resp_buffer[2];            // OP_CODE + result
  char result = '0';
  int disconnect_flag = 0;

  while (!disconnect_flag) {
    // read from the request pipe until we get a valid operation

    if (read_all(req_pipe_fd, op_buffer, sizeof(op_buffer), NULL) != 1) {
      if (errno == EBADF) {
        disconnect_flag = 1;
        break;
      }
    } else {
      enum Code op_code = get_code(op_buffer[0]);
      switch (op_code) {
        case OP_CODE_SUBSCRIBE:
          // if read_all fails beacuse of no file descriptor it needs to break the loop
          if (read_all(req_pipe_fd, key, sizeof(key), NULL) == -1) {
            if (errno == EBADF) {
              disconnect_flag = 1;
              break;
            }
            pthread_exit(NULL);
          }

          result = subscribe(key, notif_pipe_fd);
          resp_buffer[0] = get_code_string(OP_CODE_SUBSCRIBE);
          resp_buffer[1] = result;
          // fazer condição para quando o errno nao é EBADF
          if (write_all(resp_pipe_fd, resp_buffer, sizeof(resp_buffer)) == -1) {
            if (errno == EBADF) {
              disconnect_flag = 1;
              break;
            }
            pthread_exit(NULL);
          }

          break;

        case OP_CODE_UNSUBSCRIBE:
          if (read_all(req_pipe_fd, key, sizeof(key), NULL) == -1) {
            if (errno == EBADF) {
              disconnect_flag = 1;
              break;
            }
            pthread_exit(NULL);
          }
          result = unsubscribe(key, notif_pipe_fd);
          resp_buffer[0] = get_code_string(OP_CODE_UNSUBSCRIBE);
          resp_buffer[1] = result;
          if (write_all(resp_pipe_fd, resp_buffer, sizeof(resp_buffer)) == -1) {
            if (errno == EBADF) {
              disconnect_flag = 1;
              break;
            }
            pthread_exit(NULL);
          }

          break;

        case OP_CODE_DISCONNECT:

          result = disconnect(notif_pipe_fd);

          active_clients[index].req_fd = 0;
          active_clients[index].resp_fd = 0;
          active_clients[index].notif_fd = 0;

          resp_buffer[0] = get_code_string(OP_CODE_DISCONNECT);
          resp_buffer[1] = result;

          if (write_all(resp_pipe_fd, resp_buffer, sizeof(resp_buffer)) == -1) {
            if (errno == EBADF) {
              disconnect_flag = 1;
              break;
            }
            pthread_exit(NULL);
          }

          close(req_pipe_fd);
          close(notif_pipe_fd);
          close(resp_pipe_fd);
          // go back to the main loop
          disconnect_flag = 1;
          break;
        case OP_CODE_INVALID:
          fprintf(stderr, "Invalid operation\n");
          break;
        case OP_CODE_CONNECT:
          fprintf(stderr, "Invalid operation\n");
          break;
      }
    }
  }
  return;
}

static void* client_thread(void* arguments) {
  struct ManagingClients* buffer_data = (struct ManagingClients*)arguments;
  char req_pipe_path[MAX_PIPE_PATH_LENGTH];
  char resp_pipe_path[MAX_PIPE_PATH_LENGTH];
  char notif_pipe_path[MAX_PIPE_PATH_LENGTH];
  int req_pipe_fd;
  int resp_pipe_fd;
  int notif_pipe_fd;

  char result = '0';    // it starts at 0 because of the connect
  char resp_buffer[2];  // OP_CODE + result

  while (1) {

    // Reading from the production buffer ---------------------------
    sem_wait(&consumer_buffer);

    pthread_mutex_lock(&semExMut);

    strncpy(req_pipe_path, buffer_data->buffer[*(buffer_data->read_index)].req_pipe_path, MAX_PIPE_PATH_LENGTH);
    strncpy(resp_pipe_path, buffer_data->buffer[*(buffer_data->read_index)].resp_pipe_path, MAX_PIPE_PATH_LENGTH);
    strncpy(notif_pipe_path, buffer_data->buffer[*(buffer_data->read_index)].notif_pipe_path, MAX_PIPE_PATH_LENGTH);

    *(buffer_data->read_index) = (*(buffer_data->read_index) + 1) % MAX_CLIENTS;
    pthread_mutex_unlock(&semExMut);

    sem_post(&productor_buffer);

    //--------------------------------------------

    // Connecting the server to the client--------------------------

    req_pipe_fd = open(req_pipe_path, O_RDONLY);
    resp_pipe_fd = open(resp_pipe_path, O_WRONLY);
    notif_pipe_fd = open(notif_pipe_path, O_WRONLY);

    resp_buffer[0] = get_code_string(OP_CODE_CONNECT);
    resp_buffer[1] = result;
    write_all(resp_pipe_fd, resp_buffer, sizeof(resp_buffer));

    int index;
    for (index = 0; index < MAX_CLIENTS; index++) {
      if (active_clients[index].req_fd == 0) {
        active_clients[index].req_fd = req_pipe_fd;
        strcpy(active_clients[index].req_pipe_path, req_pipe_path);
        active_clients[index].resp_fd = resp_pipe_fd;
        strcpy(active_clients[index].resp_pipe_path, resp_pipe_path);
        active_clients[index].notif_fd = notif_pipe_fd;
        strcpy(active_clients[index].notif_pipe_path, notif_pipe_path);
        break;
      }
    }

    //-------------------------------------------------------------------------------------

    // Reading the clients requests and responding to them
    run_client_requests(req_pipe_fd, resp_pipe_fd, notif_pipe_fd, index);

    // Deleting pipes when no longer necessary
    unlink(req_pipe_path);
    unlink(notif_pipe_path);
    unlink(resp_pipe_path);
  }
  pthread_exit(NULL);
}

static void dispatch_threads(DIR* dir, struct ManagingClients* buffer_data) {
  pthread_t* threads = malloc(max_threads * sizeof(pthread_t));
  // create the host thread
  pthread_t* host_thread = malloc(sizeof(pthread_t));

  pthread_t* client_threads = malloc(MAX_CLIENTS * sizeof(pthread_t));
  if (threads == NULL) {
    fprintf(stderr, "Failed to allocate memory for threads\n");
    return;
  }

  struct SharedData thread_data = {dir, jobs_directory, PTHREAD_MUTEX_INITIALIZER};

  //dispatching the threads that process job files
  for (size_t i = 0; i < max_threads; i++) {
    if (pthread_create(&threads[i], NULL, get_file, (void*)&thread_data) != 0) {
      fprintf(stderr, "Failed to create thread %zu\n", i);
      pthread_mutex_destroy(&thread_data.directory_mutex);
      free(threads);
      free(host_thread);
      free(client_threads);
      return;
    }
  }
  // dispatching the host thread
  if (pthread_create(host_thread, NULL, managing_clients, (void*)buffer_data) != 0) {
    fprintf(stderr, "Failed to create host thread\n");
    pthread_mutex_destroy(&thread_data.directory_mutex);
    free(threads);
    free(host_thread);
    free(client_threads);
    return;
  }

  //dispatching the threads that handles clients (one thread per client)
  for (size_t i = 0; i < MAX_CLIENTS; i++) {
    if (pthread_create(&client_threads[i], NULL, client_thread, (void*)buffer_data) != 0) {
      fprintf(stderr, "Failed to create client thread\n");
      pthread_mutex_destroy(&thread_data.directory_mutex);
      free(threads);
      free(host_thread);
      free(client_threads);
      return;
    }
  }

  for (unsigned int i = 0; i < max_threads; i++) {
    if (pthread_join(threads[i], NULL) != 0) {
      fprintf(stderr, "Failed to join thread %u\n", i);
      pthread_mutex_destroy(&thread_data.directory_mutex);
      free(threads);
      free(host_thread);
      free(client_threads);
      return;
    }
  }

  if (pthread_join(*host_thread, NULL) != 0) {
    fprintf(stderr, "Failed to join host thread\n");
    pthread_mutex_destroy(&thread_data.directory_mutex);
    free(threads);
    free(host_thread);
    free(client_threads);
    return;
  }

  for (unsigned int i = 0; i < MAX_CLIENTS; i++) {
    if (pthread_join(client_threads[i], NULL) != 0) {
      fprintf(stderr, "Failed to join client thread %u\n", i);
      pthread_mutex_destroy(&thread_data.directory_mutex);
      free(threads);
      free(host_thread);
      free(client_threads);
      return;
    }
  }

  if (pthread_mutex_destroy(&thread_data.directory_mutex) != 0) {
    fprintf(stderr, "Failed to destroy directory_mutex\n");
  }

  free(threads);
  free(host_thread);
  free(client_threads);
}

int requests_buffer_init(struct ManagingClients* buffer, char* fifo_name) {
  int fifo_fd;

  if (unlink(fifo_name) != 0 && errno != ENOENT) {
    return 1;
  }

  if (mkfifo(fifo_name, 0640) == -1) {
    unlink(fifo_name);
    return 1;
  }

  fifo_fd = open(fifo_name, O_RDONLY);
  if (fifo_fd == -1) {
    return 1;
  }

  buffer->fifo_fd = fifo_fd;
  buffer->read_index = malloc(sizeof(size_t));  // MALOC TEMOS DE DAR FREE
  *(buffer->read_index) = 0;

  for (int i = 0; i < MAX_CLIENTS; i++) {
    active_clients[i].req_fd = 0;
    active_clients[i].resp_fd = 0;
    active_clients[i].notif_fd = 0;
  }
  return 0;
}

int main(int argc, char** argv) {
  if (argc != 5) {
    write_str(STDERR_FILENO, "Usage: ");
    write_str(STDERR_FILENO, argv[0]);
    write_str(STDERR_FILENO, " <jobs_dir>");
    write_str(STDERR_FILENO, " <max_threads>");
    write_str(STDERR_FILENO, " <max_backups> \n");
    write_str(STDERR_FILENO, "  <register_FIFO_name>\n");
    return 1;
  }

  jobs_directory = argv[1];
  struct ManagingClients buffer_data;  // struct to create the write/reading buffer
  char* endptr;
  max_backups = strtoul(argv[3], &endptr, 10);

  // initialize the global sigset
  initialize_global_sigset();

  if (pthread_sigmask(SIG_BLOCK, &set_with_sigusr1, NULL) != 0) {
    perror("pthread_sigmask not successful");
    return 1;
  }
  if (*endptr != '\0') {
    fprintf(stderr, "Invalid max_proc value\n");
    return 1;
  }

  max_threads = strtoul(argv[2], &endptr, 10);

  if (*endptr != '\0') {
    fprintf(stderr, "Invalid max_threads value\n");
    return 1;
  }

  if (max_backups <= 0) {
    write_str(STDERR_FILENO, "Invalid number of backups\n");
    return 0;
  }

  if (max_threads <= 0) {
    write_str(STDERR_FILENO, "Invalid number of threads\n");
    return 0;
  }

  if ((requests_buffer_init(&buffer_data, argv[4])) != 0) {
    write_str(STDERR_FILENO, "Failed to initialize FIFO\n");
    return 1;
  }

  // initialize the semaphore
  sem_init(&productor_buffer, 0, MAX_CLIENTS);
  sem_init(&consumer_buffer, 0, 0);

  // initialize the global sigset
  initialize_global_sigset();

  if (kvs_init()) {
    write_str(STDERR_FILENO, "Failed to initialize KVS\n");
    return 1;
  }

  DIR* dir = opendir(argv[1]);
  if (dir == NULL) {
    fprintf(stderr, "Failed to open directory: %s\n", argv[1]);
    return 0;
  }
  // WARNING: NAO SEI SE PRECISO DE PASSAR MAIS DO QUE O NOME DO FIFO
  dispatch_threads(dir, &buffer_data);

  if (closedir(dir) == -1) {
    fprintf(stderr, "Failed to close directory\n");
    return 0;
  }

  while (active_backups > 0) {
    wait(NULL);
    active_backups--;
  }

  kvs_terminate();

  return 0;
}
