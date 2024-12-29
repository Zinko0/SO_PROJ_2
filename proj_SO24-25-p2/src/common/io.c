#include "io.h"
#include <errno.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>
#include <sys/types.h>
#include <fcntl.h>
#include <sys/stat.h>
#include "src/common/constants.h"
#include "src/common/protocol.h"
 
 
int read_all(int fd, void *buffer, size_t size, int *intr) {
  if (intr != NULL && *intr) {
    return -1;
  }
  size_t bytes_read = 0;
  while (bytes_read < size) {
    ssize_t result = read(fd, buffer + bytes_read, size - bytes_read);
    if (result == -1) {
      if (errno == EINTR) {
        if (intr != NULL) {
          *intr = 1;
          if (bytes_read == 0) {
            return -1;
          }
        }
        continue;
      }
      perror("Failed to read from pipe");
      return -1;
    } else if (result == 0) {
      return 0;
    }
    bytes_read += (size_t)result;
  }
  return 1;
}
 
int read_string(int fd, char *str) {
  ssize_t bytes_read = 0;
  char ch;
  while (bytes_read < MAX_STRING_SIZE) {
    if (read(fd, &ch, 1) != 1) {
      return -1;
    }
    if (ch == '\0' || ch == '\n') {
      break;
    }
    str[bytes_read++] = ch;
  }
  str[bytes_read] = '\0';
  return (int)bytes_read;
}
 
int write_all(int fd, const void *buffer, size_t size) {
  size_t bytes_written = 0;
  while (bytes_written < size) {
    ssize_t result = write(fd, buffer + bytes_written, size - bytes_written);
    if (result == -1) {
      if (errno == EINTR) {
        // error for broken PIPE (error associated with writting to the closed PIPE)
        continue;
      }
      perror("Failed to write to pipe");
      return -1;
    }
    bytes_written += (size_t)result;
  }
  return 1;
}

static struct timespec delay_to_timespec(unsigned int delay_ms) {
    return (struct timespec){delay_ms / 1000, (delay_ms % 1000) * 1000000};
}

void delay(unsigned int time_ms) {
    struct timespec delay = delay_to_timespec(time_ms);
    nanosleep(&delay, NULL);
}

int create_pipe(char const* pipe_path) {
  //unlink pipe
  if(unlink(pipe_path) != 0 && errno != ENOENT){
    return -1;
  }
  //create pipe
  if(mkfifo(pipe_path, 0640) != 0){
    unlink(pipe_path);
    return -1;
  }
  
  return 0;
}

enum Code get_code(char code) {
  switch (code) {
    case '1':
      return OP_CODE_CONNECT;
    case '2':
      return OP_CODE_DISCONNECT;
    case '3':
      return OP_CODE_SUBSCRIBE;
    case '4':
      return OP_CODE_UNSUBSCRIBE;
    default:
      return OP_CODE_INVALID;
  }
}

char get_code_string(enum Code code) {
  switch (code) {
    case OP_CODE_CONNECT:
      return '1';
    case OP_CODE_DISCONNECT:
      return '2';
    case OP_CODE_SUBSCRIBE:
      return '3';
    case OP_CODE_UNSUBSCRIBE:
      return '4';
    case OP_CODE_INVALID:
      return '5';
    default:
      return '5';
  }
}

void pipe_string_filling(const char* str, size_t length, char* aux) {
  strcpy(aux, str);
  for(size_t i = length; i < MAX_STRING_SIZE; i++){
    aux[i] = '\0';
  }
}

void key_string_filling(const char* str, size_t length, char* aux) {
  strcpy(aux, str);
  for(size_t i = length; i < MAX_STRING_SIZE; i++){
    aux[i] = '\0';
  }
} 

void key_value_string_filling(const char* key, const char* value, size_t key_length, size_t value_length, char* aux) {

  strcpy(aux, key);
  for(size_t i = key_length; i <= MAX_STRING_SIZE; i++){
    aux[i] = '\0';
  }
  strcpy(aux + MAX_STRING_SIZE + 1, value);  //aux[41]
  size_t total_length = MAX_STRING_SIZE + 1 + value_length;
  //goes from aux[41 + len] to aux[81]
  for(size_t i = total_length; i < ((MAX_STRING_SIZE + 1)*2); i++){
    aux[i] = '\0';
  }
}
