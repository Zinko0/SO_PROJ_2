#include <unistd.h>
#include <errno.h>
#include <dirent.h>
#include <fcntl.h>
#include <limits.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <sys/stat.h>
#include <stdio.h>
#include <semaphore.h>
#include <sys/types.h>
#include <signal.h>

#include "constants.h"
#include "parser.h"
#include "operations.h"
#include "io.h"
#include "pthread.h"
#include "src/common/protocol.h"
#include "src/common/io.h"
#include "src/common/constants.h"

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
  int resp_fd;
  int notif_fd;
};

struct ManagingClients {
  //the buffer can have whatever size. 
  struct PipeData buffer[MAX_CLIENTS];
  size_t *read_index;
  int fifo_fd;

  struct ActiveClients active_clients[MAX_CLIENTS];
};



sigset_t set_with_sigusr1;
int signal_received = 0;



pthread_mutex_t lock = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t n_current_backups_lock = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t semExMut = PTHREAD_MUTEX_INITIALIZER;
sem_t full_buffer;
sem_t empty_buffer;

size_t active_backups = 0;     // Number of active backups
size_t max_backups;            // Maximum allowed simultaneous backups
size_t max_threads;            // Maximum allowed simultaneous threads

char* jobs_directory = NULL;

void initialize_global_sigset() {
    // Initialize the signal set
    sigemptyset(&set_with_sigusr1);
    sigaddset(&set_with_sigusr1, SIGUSR1);
}

static void sigusr1_handler(int signo) {
  if(signal(SIGTERM, sigusr1_handler) == SIG_ERR){
    exit(EXIT_FAILURE);
  }
  if (signo == SIGUSR1) {
    signal_received = 1;
    exit(EXIT_SUCCESS);
  }
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
            "  BACKUP\n" // Not implemented
            "  HELP\n");

        break;

      case CMD_EMPTY:
        break;

      case EOC:
        printf("EOF\n");
        return 0;
    }
  }
}

//frees arguments
static void* get_file(void* arguments) {
  if (pthread_sigmask(SIG_BLOCK, &set_with_sigusr1, NULL) != 0) {
    perror("pthread_sigmask");
    return NULL;
  }

  struct SharedData* thread_data = (struct SharedData*) arguments;
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

void assing_pipe_data(struct PipeData* buffer,size_t index,char* pipes_path){
//está com um char a mais por alguma razao

  strncpy(buffer[index].req_pipe_path,pipes_path,MAX_PIPE_PATH_LENGTH);
  strncpy(buffer[index].resp_pipe_path,pipes_path + MAX_PIPE_PATH_LENGTH,MAX_PIPE_PATH_LENGTH);
  strncpy(buffer[index].notif_pipe_path,pipes_path + (2*MAX_PIPE_PATH_LENGTH),MAX_PIPE_PATH_LENGTH);
  return;
}

void close_all_clients(struct ActiveClients* active_clients){
  for(int i = 0; i < MAX_CLIENTS; i++){
    if(active_clients[i].req_fd != 0){
      close(active_clients[i].req_fd);
      close(active_clients[i].resp_fd);
      close(active_clients[i].notif_fd);
      active_clients[i].req_fd = 0;
      active_clients[i].resp_fd = 0;
      active_clients[i].notif_fd = 0;
    }
  }
}

static void *managing_clients(void* arguments) {
  struct ManagingClients* buffer_data = (struct ManagingClients*) arguments;
  char buffer[1 + MAX_PIPE_PATH_LENGTH * 3]; //OP_CODE + 3 pipe paths
  size_t write_index = 0;
  //Handle SIGUSR1
  if (signal(SIGUSR1, sigusr1_handler) == SIG_ERR) {
    perror("signal");
    return NULL;
  }

  //Is allways reading from the FIFO waiting for a client to connect
  while (1){
    while(read_all(buffer_data->fifo_fd, buffer, sizeof(buffer), NULL) != 1); 

    if(get_code(buffer[0]) == OP_CODE_CONNECT){
      sem_wait(&full_buffer);

      pthread_mutex_lock(&semExMut);

      assing_pipe_data(buffer_data->buffer,write_index,buffer + 1);

      write_index = (write_index + 1)% MAX_CLIENTS; 
      pthread_mutex_unlock(&semExMut);

      sem_post(&empty_buffer);
    }
    printf("MANAGER POV:\n");
    for(int i = 0; i < MAX_CLIENTS; i++){
      printf("index: %d ",i);
      printf("req_fd: %s ",buffer_data->buffer[i].req_pipe_path);
      printf("resp_fd: %s ",buffer_data->buffer[i].resp_pipe_path);
      printf("notif_fd: %s\n",buffer_data->buffer[i].notif_pipe_path);
    }
    if(signal_received == 1){
      close_all_clients(buffer_data->active_clients);
      //disconnect all is not async signal safe, so we need to call it here
      disconnect_all();
      break;
    }
  }
  close(buffer_data->fifo_fd);
  pthread_exit(NULL);
}

static void *client_thread(void *arguments){
  if (pthread_sigmask(SIG_BLOCK, &set_with_sigusr1, NULL) != 0) {
    perror("pthread_sigmask");
    return NULL;
  }
  struct ManagingClients* buffer_data = (struct ManagingClients*) arguments;
  char req_pipe_path[MAX_PIPE_PATH_LENGTH];
  char resp_pipe_path[MAX_PIPE_PATH_LENGTH];
  char notif_pipe_path[MAX_PIPE_PATH_LENGTH];
  int req_pipe_fd;
  int resp_pipe_fd;
  int notif_pipe_fd;

  char op_buffer[1]; //OP_CODE + space
  char key[MAX_STRING_SIZE + 1]; //key + \0
  char resp_buffer[2]; //OP_CODE + space + result + \0
  char result = '0';//it starts at 0 because of the connect
  int disconnect_flag;
  while(1){
  //readMsg function ---------------------------
  disconnect_flag = 0;
  sem_wait(&empty_buffer);
  
  pthread_mutex_lock(&semExMut);

  strncpy(req_pipe_path,buffer_data->buffer[*(buffer_data->read_index)].req_pipe_path,MAX_PIPE_PATH_LENGTH);
  strncpy(resp_pipe_path,buffer_data->buffer[*(buffer_data->read_index)].resp_pipe_path,MAX_PIPE_PATH_LENGTH);
  strncpy(notif_pipe_path,buffer_data->buffer[*(buffer_data->read_index)].notif_pipe_path,MAX_PIPE_PATH_LENGTH);

  printf("CLIENT POV:\n");
  for(int i = 0; i < MAX_CLIENTS; i++){
      printf("index: %d ",i);
      printf("req_fd: %s ",buffer_data->buffer[i].req_pipe_path);
      printf("resp_fd: %s ",buffer_data->buffer[i].resp_pipe_path);
      printf("notif_fd: %s\n",buffer_data->buffer[i].notif_pipe_path);
  }
  printf("index: %ld \n",*(buffer_data->read_index));
  *(buffer_data->read_index) = (*(buffer_data->read_index) + 1) % MAX_CLIENTS;

  pthread_mutex_unlock(&semExMut);

  sem_post(&full_buffer);

  //--------------------------------------------
  
  //------Connecting function--------------------------

  pthread_mutex_lock(&lock);

  req_pipe_fd = open(req_pipe_path, O_RDONLY);
  resp_pipe_fd = open(resp_pipe_path, O_WRONLY);
  notif_pipe_fd = open(notif_pipe_path, O_WRONLY); //falta testar se os opens correram bem
  
  resp_buffer[0] = get_code_string(OP_CODE_CONNECT);
  resp_buffer[1] = result; 
  write_all(resp_pipe_fd,resp_buffer,sizeof(resp_buffer));

  
  int index;
  for(index = 0; index < MAX_CLIENTS; index++){
    if(buffer_data->active_clients[index].req_fd == 0){ 
      buffer_data->active_clients[index].req_fd = req_pipe_fd;
      buffer_data->active_clients[index].resp_fd = resp_pipe_fd;
      buffer_data->active_clients[index].notif_fd = notif_pipe_fd;
      break;
    }
  }
  pthread_mutex_unlock(&lock);
  //-------------------------------------------------------------------------------------
    //while the client is connected
    while(!disconnect_flag){
      //read from the request pipe until we get a valid operation
      while(read_all(req_pipe_fd, op_buffer, sizeof(op_buffer), NULL) != 1){
        if(errno == EBADF){
          disconnect_flag = 1;
          break;
        }
      }
      //if a SIGUSR1 was sent to the server we break the loop

      enum Code op_code = get_code(op_buffer[0]);
      switch (op_code){
        case OP_CODE_SUBSCRIBE:
          //if read_all fails beacuse of no file descriptor it needs to break the loop
          if(read_all(req_pipe_fd,key,sizeof(key),NULL) == -1 && errno == EBADF){
            disconnect_flag = 1;
            break;
          };
          
          result = subscribe(key, notif_pipe_fd);
          resp_buffer[0] = get_code_string(OP_CODE_SUBSCRIBE);
          resp_buffer[1] = result;
          //fazer condição para quando o errno nao é EBADF
          if(write_all(resp_pipe_fd,resp_buffer,sizeof(resp_buffer)) == -1 && errno == EBADF){
            disconnect_flag = 1;
          }
          break;


        case OP_CODE_UNSUBSCRIBE:
          if(read_all(req_pipe_fd,key,sizeof(key),NULL) == -1 && errno == EBADF){
            disconnect_flag = 1;
            break;
          };
          result = unsubscribe(key, notif_pipe_fd);
          resp_buffer[0] = get_code_string(OP_CODE_UNSUBSCRIBE);
          resp_buffer[1] = result;
          if(write_all(resp_pipe_fd,resp_buffer,sizeof(resp_buffer)) == -1 && errno == EBADF){
            disconnect_flag = 1;
          }
          break;

        case OP_CODE_DISCONNECT:
          
          result = disconnect(notif_pipe_fd);

          buffer_data->active_clients[index].req_fd = 0;
          buffer_data->active_clients[index].resp_fd = 0;
          buffer_data->active_clients[index].notif_fd = 0;

          resp_buffer[0] = get_code_string(OP_CODE_DISCONNECT);
          resp_buffer[1] = result;

          if(write_all(resp_pipe_fd,resp_buffer,sizeof(resp_buffer)) == -1 && errno == EBADF){
            disconnect_flag = 1;
          }
          break;
          close(req_pipe_fd);
          close(notif_pipe_fd);
          close(resp_pipe_fd);
          //go back to the main loop
          disconnect_flag = 1;
          break;
        case OP_CODE_INVALID:
          fprintf(stderr,"Invalid operation\n");
          break;
        case OP_CODE_CONNECT:
          fprintf(stderr,"Invalid operation\n");
          break;
      }
    }          
  }
  pthread_exit(NULL);
}

static void dispatch_threads(DIR* dir,struct ManagingClients* buffer_data) {
  pthread_t* threads = malloc(max_threads * sizeof(pthread_t));
  //create the host thread
  pthread_t* host_thread = malloc(sizeof(pthread_t));

  pthread_t* client_threads = malloc(MAX_CLIENTS * sizeof(pthread_t));
  if (threads == NULL) {
    fprintf(stderr, "Failed to allocate memory for threads\n");
    return;
  }

  struct SharedData thread_data = {dir, jobs_directory, PTHREAD_MUTEX_INITIALIZER};


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
  //dispatching the host thread TODO IT IS NOT CORRECT
  if(pthread_create(host_thread, NULL, managing_clients, (void*)buffer_data) != 0) {
    fprintf(stderr, "Failed to create host thread\n");
    pthread_mutex_destroy(&thread_data.directory_mutex);
    free(threads);
    free(host_thread);
    free(client_threads);
    return;
  }

  for(size_t i = 0; i < MAX_CLIENTS; i++) {
    if(pthread_create(&client_threads[i], NULL, client_thread , (void*)buffer_data) != 0) {
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

int requests_buffer_init(struct ManagingClients* buffer,char* fifo_name) {
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
  buffer->read_index = malloc(sizeof(size_t)); //MALOC TEMOS DE DAR FREE
  *(buffer->read_index)= 0;
  return 0;
}


int main(int argc, char** argv) {
  if (argc < 4) {
    write_str(STDERR_FILENO, "Usage: ");
    write_str(STDERR_FILENO, argv[0]);
    write_str(STDERR_FILENO, " <jobs_dir>");
		write_str(STDERR_FILENO, " <max_threads>");
		write_str(STDERR_FILENO, " <max_backups> \n");
    write_str(STDERR_FILENO, "  <register_FIFO_name>\n");
    return 1;
  }

  jobs_directory = argv[1];
  struct ManagingClients buffer_data; //struct to create the write/reading buffer
  char* endptr;
  max_backups = strtoul(argv[3], &endptr, 10);

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

  if((requests_buffer_init(&buffer_data,argv[4])) != 0) {
    write_str(STDERR_FILENO, "Failed to initialize FIFO\n");
    return 1;
  }

  //initialize the semaphore
  sem_init(&full_buffer, 0, MAX_CLIENTS);
  sem_init(&empty_buffer, 0, 0);

  //initialize the global sigset
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
  //WARNING: NAO SEI SE PRECISO DE PASSAR MAIS DO QUE O NOME DO FIFO
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
