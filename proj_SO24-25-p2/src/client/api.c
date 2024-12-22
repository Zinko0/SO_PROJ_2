#include <fcntl.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include "src/common/constants.h"
#include "src/common/protocol.h"
#include "src/common/io.h"

int filedesc[4];

int kvs_connect(char const* req_pipe_path, char const* resp_pipe_path, char const* server_pipe_path,
                char const* notif_pipe_path) {
  if((filedesc[0] = create_pipe(server_pipe_path,O_WRONLY)) == -1){
    return 1;
  }
  
  //------------------------------------------
  char buffer[1 + MAX_PIPE_PATH_LENGTH * 3 + 3 + 1];
  snprintf(buffer, strlen(buffer), "%d %s %s %s", OP_CODE_CONNECT,req_pipe_path, resp_pipe_path, notif_pipe_path);

  if(write_all(filedesc[0], buffer, strlen(buffer)) == -1){
    return 1;
  }
 //------------------------------------------
 
  if((filedesc[1] = create_pipe(req_pipe_path,O_WRONLY)) == -1){
    return 1;
  }
  if((filedesc[2] = create_pipe(resp_pipe_path,O_RDONLY)) == -1){
    return 1;
  }

  if((filedesc[3]= create_pipe(notif_pipe_path,O_RDONLY)) == -1){
    return 1;
  }
  //Aguardar resposta do servidor
  if(read_all(filedesc[2],buffer,3,NULL) == -1){
    return 1;
  }
  int result = buffer[2] - '0';
  printf("Server returned %d for operation: connect\n",OP_CODE_CONNECT);
  return result;
}
 
int kvs_disconnect(void) {
  // close pipes and unlink pipe files
  //-------------------------------------------
  char buffer[3]; //OP_CODE_DISCONNECT
  sprintf(buffer, "%d", OP_CODE_DISCONNECT);
  if(write_all(filedesc[1],buffer, 1) == -1){
    return 1;
  }
  //-------------------------------------------
  //Aguardar resposta do servidor

  if(read_all(filedesc[2],buffer,3,NULL) == -1){
    return 1;
  }
  int result = buffer[2] - '0';
  for(int i = 0; i < 4; i++){
    if(close(filedesc[i]) == -1){
      return 1;
    }
  }
  //DUVIDA: decidir o que fazer em caso de erro
  printf("Server returned %d for operation: disconnect\n",result);
  return 0;
}

int kvs_subscribe(const char* key) {
  // send subscribe message to request pipe and wait for response in response pipe
  char buffer[1 + MAX_STRING_SIZE + 1 + 1]; //OP_CODE_SUBSCRIBE  
  sprintf(buffer, "%d %s", OP_CODE_SUBSCRIBE ,key); //DUVIDA: SE faz com que as strings tenham sempre 40 caracteres
  if(write_all(filedesc[1],buffer, strlen(buffer)) == -1){
    return 1;
  }
  
  if(read_all(filedesc[2],buffer,3,NULL) == -1){
    return 1;
  }
  int result = buffer[2] - '0';
  printf("Server returned %d for operation: subscribe\n",result);
  
  return 0;
}

int kvs_unsubscribe(const char* key) {
  // send unsubscribe message to request pipe and wait for response in response pipe
  char buffer[1 + MAX_STRING_SIZE + 1 + 1]; //OP_CODE_SUBSCRIBE  
  sprintf(buffer, "%d %s", OP_CODE_UNSUBSCRIBE ,key);
  if(write_all(filedesc[1],buffer, strlen(buffer)) == -1){
    return 1;
  }
  //response of type: "%c %c\n" -> OP_CODE_UNSUBSCRIBE, result
  if(read_all(filedesc[2],buffer,3,NULL) == -1){
    return 1;
  }
  int result = buffer[2] - '0';
  printf("Server returned %d for operation: unsubscribe\n",result);
  return 0;
}



