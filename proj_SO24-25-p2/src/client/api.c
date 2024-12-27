#include <fcntl.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include "src/common/constants.h"
#include "src/common/protocol.h"
#include "src/common/io.h"
#include "api.h"

int filedesc[4];
int connected;

int kvs_connect(char const* req_pipe_path, char const* resp_pipe_path, char const* server_pipe_path,
                char const* notif_pipe_path) {
  
  //abro o pipe (o server já deve estar aberto)
  filedesc[0] = open(server_pipe_path,O_WRONLY);
  //------------------------------------------
  
  char buffer[1 + MAX_PIPE_PATH_LENGTH * 3 + 3 + 1];
  char filled_req_pipe[MAX_PIPE_PATH_LENGTH];
  char filled_resp_pipe[MAX_PIPE_PATH_LENGTH];
  char filled_notif_pipe[MAX_PIPE_PATH_LENGTH];
  pipe_string_filling(req_pipe_path,strlen(req_pipe_path),filled_req_pipe);
  pipe_string_filling(resp_pipe_path,strlen(resp_pipe_path),filled_resp_pipe);
  pipe_string_filling(notif_pipe_path,strlen(notif_pipe_path),filled_notif_pipe);
  snprintf(buffer, sizeof(buffer), "%d %s %s %s", OP_CODE_CONNECT,filled_req_pipe, filled_resp_pipe, filled_notif_pipe);

  
  if(write_all(filedesc[0], buffer, sizeof(buffer)) == -1){
    return 1;
  }
 //------------------------------------------
 
  if(create_pipe(req_pipe_path) == -1){
    return 1;
  }
  if(create_pipe(resp_pipe_path) == -1){
    return 1;
  }

  if(create_pipe(notif_pipe_path) == -1){
    return 1;
  }
  filedesc[1] = open(filled_req_pipe,O_WRONLY);
  filedesc[2] = open(filled_resp_pipe,O_RDONLY);
  filedesc[3] = open(filled_notif_pipe,O_RDONLY);
  for(int i = 1; i < 4; i++){
    if(filedesc[i] == -1){
      return 1;
    }
  }
  //Aguardar resposta do servidor
  char resp_buffer[4];
  while(read_all(filedesc[2],resp_buffer,sizeof(resp_buffer),NULL) != 1);
  connected = 1;
  printf("Server returned %c for operation: connect\n",resp_buffer[2]);
  return 0;
}
 
int kvs_disconnect(void) {
  // close pipes 
  //-------------------------------------------
  char buffer[2]; //OP_CODE_DISCONNECT
  sprintf(buffer, "%d", OP_CODE_DISCONNECT);
  if(write_all(filedesc[1],buffer, sizeof(buffer)) == -1){
    return 1;
  }
  //-------------------------------------------
  //Aguardar resposta do servidor
  char resp_buffer[4];
  while(read_all(filedesc[2],resp_buffer,sizeof(resp_buffer),NULL) != 1);
  connected = 0;
  for(int i = 0; i < 4; i++){
    if(close(filedesc[i]) == -1){
      return 1;
    }
  }
  printf("Server returned %c for operation: disconnect\n",resp_buffer[2]);
  return 0;
}

int kvs_subscribe(const char* key) {
  // send subscribe message to request pipe and wait for response in response pipe
  char filled_key[MAX_STRING_SIZE];
  key_string_filling(key,strlen(key),filled_key);
  char buffer[1 + MAX_STRING_SIZE + 1 + 1]; //OP_CODE_SUBSCRIBE  
  sprintf(buffer, "%d %s", OP_CODE_SUBSCRIBE ,filled_key); //DUVIDA: SE faz com que as strings tenham sempre 40 caracteres
  if(write_all(filedesc[1],buffer, sizeof(buffer)) == -1){
    return 1;
  }
  char resp_buffer[4];
  while(read_all(filedesc[2],resp_buffer,sizeof(resp_buffer),NULL) != 1);
  
  printf("Server returned %c for operation: subscribe\n",resp_buffer[2]);
  return 0;
}

int kvs_unsubscribe(const char* key) {
  // send unsubscribe message to request pipe and wait for response in response pipe
  char filled_key[MAX_STRING_SIZE];
  key_string_filling(key,strlen(key),filled_key);
  char buffer[1 + MAX_STRING_SIZE + 1 + 1]; //OP_CODE_SUBSCRIBE  
  sprintf(buffer, "%d %s", OP_CODE_UNSUBSCRIBE ,filled_key);
  if(write_all(filedesc[1],buffer, sizeof(buffer)) == -1){
    return 1;
  }
  //response of type: "%c %c\n" -> OP_CODE_UNSUBSCRIBE, result
  char resp_buffer[4];
  while(read_all(filedesc[2],resp_buffer,sizeof(resp_buffer),NULL) != 1 || get_code(buffer[0]) != OP_CODE_UNSUBSCRIBE);
 
  printf("Server returned %c for operation: unsubscribe\n",resp_buffer[2]);
  return 0;
}

void *kvs_get_notification(void* arg) {
  (void)arg;//to avoid unused parameter warning----------------------------------------------------
  // read from notification pipe
  char buffer[(MAX_STRING_SIZE+1)*2 + strlen("(,)") + 1];
  while (connected){
    //to make sure that the read_all only tries to read if the client is still connected
    if (connected && read_all(filedesc[3], buffer, sizeof(buffer), NULL) == 1) {
      printf("%s\n", buffer);
    }
  }
  return NULL;
}