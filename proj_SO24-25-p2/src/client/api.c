#include "api.h"

#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>

#include "src/common/constants.h"
#include "src/common/io.h"
#include "src/common/protocol.h"

int filedesc[4];

int kvs_connect(char const* req_pipe_path, char const* resp_pipe_path, char const* server_pipe_path,
                char const* notif_pipe_path) {
  // abro o pipe (o server já deve estar aberto)
  filedesc[0] = open(server_pipe_path, O_WRONLY);
  //------------------------------------------

  char buffer[1 + MAX_PIPE_PATH_LENGTH * 3];

  // Fill buffer with \0
  memset(buffer, 0, sizeof(buffer));

  // Assemble buffer to write to server
  buffer[0] = get_code_string(OP_CODE_CONNECT);
  strncpy(buffer + 1, req_pipe_path, (strlen(req_pipe_path)) * sizeof(char));
  strncpy(buffer + 1 + MAX_PIPE_PATH_LENGTH, resp_pipe_path, (strlen(resp_pipe_path)) * sizeof(char));
  strncpy(buffer + 1 + (2 * MAX_PIPE_PATH_LENGTH), notif_pipe_path, (strlen(notif_pipe_path)) * sizeof(char));

  if (write_all(filedesc[0], buffer, sizeof(buffer)) == -1) {
    return 1;
  }
  // its the only time the client communicates with the server through the server pipe
  // so we close it
  close(filedesc[0]);
  //------------------------------------------

  if (create_pipe(req_pipe_path) == -1) {
    return 1;
  }
  if (create_pipe(resp_pipe_path) == -1) {
    return 1;
  }

  if (create_pipe(notif_pipe_path) == -1) {
    return 1;
  }
  filedesc[1] = open(req_pipe_path, O_WRONLY);
  filedesc[2] = open(resp_pipe_path, O_RDONLY);
  filedesc[3] = open(notif_pipe_path, O_RDONLY);
  for (int i = 1; i < 4; i++) {
    if (filedesc[i] == -1) {
      return 1;
    }
  }
  // Aguardar resposta do servidor
  char resp_buffer[2];

  if (read_all(filedesc[2], resp_buffer, sizeof(resp_buffer), NULL) != 1) {
    return 1;
  }

  printf("Server returned %c for operation: connect\n", resp_buffer[1]);
  return 0;
}

int kvs_disconnect(void) {
  // close pipes
  //-------------------------------------------
  char buffer[1];  // OP_CODE_DISCONNECT
  buffer[0] = get_code_string(OP_CODE_DISCONNECT);
  if (write_all(filedesc[1], buffer, sizeof(buffer)) == -1) {
    return 1;
  }
  //-------------------------------------------
  // Aguardar resposta do servidor
  char resp_buffer[2];
  if (read_all(filedesc[2], resp_buffer, sizeof(resp_buffer), NULL) != 1) {
    return 1;
  }
  printf("Server returned %c for operation: disconnect\n", resp_buffer[1]);
  return 0;
}

int kvs_subscribe(const char* key) {
  // send subscribe message to request pipe and wait for response in response pipe
  char buffer[1 + MAX_STRING_SIZE + 1];

  // Fill buffer with \0
  memset(buffer, 0, sizeof(buffer));

  buffer[0] = get_code_string(OP_CODE_SUBSCRIBE);
  strncpy(buffer + 1, key, (MAX_STRING_SIZE + 1) * sizeof(char));

  if (write_all(filedesc[1], buffer, sizeof(buffer)) == -1) {
    return 1;
  }
  char resp_buffer[2];

  if (read_all(filedesc[2], resp_buffer, sizeof(resp_buffer), NULL) != 1) {
    return 1;
  }

  printf("Server returned %c for operation: subscribe\n", resp_buffer[1]);
  return 0;
}

int kvs_unsubscribe(const char* key) {
  // send unsubscribe message to request pipe and wait for response in response pipe
  char buffer[1 + MAX_STRING_SIZE + 1];

  // Fill buffer with \0
  memset(buffer, 0, sizeof(buffer));
  
  buffer[0] = get_code_string(OP_CODE_SUBSCRIBE);
  strncpy(buffer + 1, key, (MAX_STRING_SIZE + 1) * sizeof(char));

  if (write_all(filedesc[1], buffer, sizeof(buffer)) == -1) {
    return 1;
  }
  // response of type: "%c %c\n" -> OP_CODE_UNSUBSCRIBE, result
  char resp_buffer[2];

  if (read_all(filedesc[2], resp_buffer, sizeof(resp_buffer), NULL) != 1) {
    return 1;
  }

  printf("Server returned %c for operation: unsubscribe\n", resp_buffer[1]);
  return 0;
}

void* kvs_get_notification(void* arg) {
  int* connected = (int*)arg;
  // read from notification pipe
  char buffer[(MAX_STRING_SIZE + 1) * 2];
  char key[MAX_STRING_SIZE + 1];
  char value[MAX_STRING_SIZE + 1];

  // we only want to read from the pipe while the client is connected
  // if the client disconnects(filedesc[3] closes),
  // the *connected condition prevents MOST read_all calls without a file descriptor
  // if the server terminates, the read_all will return 0 and the client will terminate
  while (*connected) {
    if (read_all(filedesc[3], buffer, sizeof(buffer), NULL) != 1) {
      break;
    }
    strncpy(key, buffer, MAX_STRING_SIZE + 1);
    strncpy(value, buffer + MAX_STRING_SIZE + 1, MAX_STRING_SIZE + 1);
    printf("(%s,%s)\n", key, value);
  }
  terminate();
  return NULL;
}

int terminate() {
  for (size_t i = 1; i < 4; i++) {
    if (close(filedesc[i]) !=  0){
      return 1;
    }
  }
  return 0;
}