#include <fcntl.h>
#include <stdio.h>
#include "api.h"
#include "src/common/constants.h"
#include "src/common/protocol.h"
#include "src/common/io.h"

int kvs_connect(char const* req_pipe_path, char const* resp_pipe_path, char const* server_pipe_path,
                char const* notif_pipe_path) {
  if((filedesc[0] = create_pipe(server_pipe_path,O_WRONLY)) == -1){
    return 1;
  }
  
  //------------------------------------------
  char buffer[1 + MAX_PIPE_PATH_LENGTH * 3 + 3 + 1];
  snprintf(buffer, strln(buffer), "%d %s %s %s", OP_CODE_CONNECT,req_pipe_path, resp_pipe_path, notif_pipe_path);

  if(write_all(filedesc[0], buffer, strlen(buffer)) == -1){
    return 1;
  }
 //------------------------------------------
 
  if(filedesc[1] = create_pipe(req_pipe_path,O_WRONLY) == -1){
    return 1;
  }
  if(filedesc[2] = create_pipe(resp_pipe_path,O_RDONLY) == -1){
    return 1;
  }

  if(filedesc[3]= create_pipe(notif_pipe_path,O_RDONLY) == -1){
    return 1;
  }
  //Aguardar resposta do servidor
  return 0;
}
 
int kvs_disconnect(void) {
  // close pipes and unlink pipe files
  //-------------------------------------------
  char buffer = '2'; //OP_CODE_DISCONNECT
  if(write_all(filedesc[0],buffer, strlen(OP_CODE_DISCONNECT)) == -1){
    return 1;
  }
  //-------------------------------------------
  for(int i = 0; i < 4; i++){
    if(close(filedesc[i]) == -1){
      return 1;
    }
  }
  //Aguardar resposta do servidor
  return 0;
}

int kvs_subscribe(const char* key) {
  // send subscribe message to request pipe and wait for response in response pipe
  char buffer[1 + MAX_STRING_SIZE + 1 + 1]; //OP_CODE_SUBSCRIBE  
  sprintf(buffer, "%d %s", OP_CODE_SUBSCRIBE ,key);
  if(write_all(filedesc[1],buffer, strlen(OP_CODE_SUBSCRIBE)) == -1){
    return 1;
  }
  
  if(read_all(filedesc[2],buffer,1,NULL) == -1){
    return 1;
  }
  return 0;
}

int kvs_unsubscribe(const char* key) {
  // send unsubscribe message to request pipe and wait for response in response pipe
  char buffer[1 + MAX_STRING_SIZE + 1 + 1]; //OP_CODE_SUBSCRIBE  
  sprintf(buffer, "%d %s", OP_CODE_UNSUBSCRIBE ,key);
  if(write_all(filedesc[1],buffer, strlen(OP_CODE_UNSUBSCRIBE)) == -1){
    return 1;
  }
  
  if(read_all(filedesc[2],buffer,1,NULL) == -1){
    return 1;
  }
  return 0;
}

int create_pipe(char const* pipe_path,int mode) {
  //unlink pipe
  if(unlink(pipe_path) != 0){
    return -1;
  }
  //create pipe
  if(mkfifo(pipe_path, 0640) != 0){
    return -1;
  }
  int fd = open(pipe_path, mode);
  return fd;
}


