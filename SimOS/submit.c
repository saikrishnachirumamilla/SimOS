#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include "simos.h"

//===============================================================
// The interface to interact with clients
// current implementation is to get one client input from stdin
// --------------------------
// Should change to create server socket to accept client connections
// and then poll client ports to get their inputs.
// -- Best is to use the select function to get client inputs
// Also, during execution, the terminal output (term.c) should be
// sent back to the client and client prints on its terminal.
//===============================================================

int sockfd;
fd_set active_fd_set;
node_t *head = NULL;

void error(char *msg)
{ perror(msg);
  exit(1);
}

void enqueue(request_t req)
{ node_t *new_node = malloc(sizeof(node_t));
  if (!new_node) return;

  new_node->request = req;
  new_node->next = head;
  head = new_node;
}

request_t* dequeue()
{ node_t *current, *prev = NULL;
  request_t *retreq = NULL;

  if (head == NULL) return NULL;

  current = head;
  while (current->next != NULL)
  { prev = current; current = current->next; }
  retreq = &(current->request);

  if (prev) prev->next = NULL;
  else head = NULL;

  return retreq;
}

void dump_queue()
{ node_t *current = NULL;

  printf("Dump Request Queue:\n");
  current = head;
  while (current != NULL)
  { printf("%s, %s, %d, %d\n",
            current->request.client_id, current->request.filename,
            current->request.sockfd, current->request.port);
    current = current->next;
  }
}

void read_from_client(int fd)
{ char buffer[256];
  int ret, result;
  char *client_id, *filename, *token;
  request_t *req;
  struct sockaddr_in cli_addr;
  socklen_t cli_addrlen = sizeof(cli_addr);

  bzero(buffer, sizeof(buffer));
  token = NULL;
  //client_id = NULL;
  filename = NULL;
  req = NULL;

  ret = recv(fd, buffer, sizeof(buffer), 0);
  if (ret < 0) error("Server ERROR reading from socket");

  token = strtok(buffer, " ");
  //client_id = (char *)malloc(strlen(token));
  //strcpy(client_id, token);
  //token = strtok(NULL, " ");
  filename = (char *)malloc(strlen(token));
  strcpy(filename, token);

  if (strcmp(filename, "nullfile") == 0)
  { close(fd);
    FD_CLR(fd, &active_fd_set);
  }
  else
  { getpeername(fd, (struct sockaddr *)&cli_addr, &cli_addrlen);
    req = malloc(sizeof(request_t));
    req->sockfd = fd;
    //req->client_id = client_id;
    req->filename = filename;
    req->port = (int)cli_addr.sin_port;

    enqueue(*req);
    printf ("Received file name %s\n", filename);
    submit_process (filename, fd);
  }
}

void accept_client()
{ int newsockfd, clilen;
  struct sockaddr_in cli_addr;

  clilen = sizeof(cli_addr);
  newsockfd = accept(sockfd, (struct sockaddr *)&cli_addr, &clilen);
  if (newsockfd < 0) error("ERROR accepting");
  else
  { printf("Accept client socket %d, %d\n", newsockfd, (int)cli_addr.sin_port);
    FD_SET(newsockfd, &active_fd_set);
  }
}

void initialize_socket(int portno)
{ struct sockaddr_in serv_addr;

  sockfd = socket(AF_INET, SOCK_STREAM, 0);
  if (sockfd < 0) error("ERROR opening socket");
  bzero((char *)&serv_addr, sizeof(serv_addr));
  serv_addr.sin_family = AF_INET;
  serv_addr.sin_addr.s_addr = INADDR_ANY;
  serv_addr.sin_port = htons(portno);

  if (bind(sockfd, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0)
    error("ERROR binding");

  listen(sockfd, 5);
}

void socket_select()
{ int i;
  fd_set read_fd_set;

  FD_ZERO(&active_fd_set);
  FD_SET(sockfd, &active_fd_set);

  while (Active)
  { /* Block until input arrives on one or more active sockets. */
    read_fd_set = active_fd_set;
    if (select(FD_SETSIZE, &read_fd_set, NULL, NULL, NULL) < 0)
    { perror("select"); exit(EXIT_FAILURE); }

    /* Service all the sockets with input pending. */
    for (i = 0; i < FD_SETSIZE; ++i)
      if (FD_ISSET(i, &read_fd_set))
      { if (i == sockfd) accept_client();
        else read_from_client(i);
      }
  }
}

void *client_reqhandler(void *arg)
{ char *port = (char *)arg;
  int portno;

  portno = atoi(port);
  initialize_socket (portno);
  socket_select ();
}

//======================================================

pthread_t thid;

void start_client_reqhandler(char *port)
{ int ret;
    ret = pthread_create(&thid, NULL, client_reqhandler, (void *)port);
    if (ret < 0) printf("%s\n", "thread creation problem");
}

void end_client_reqhandler()
{ request_t *req;

  while ((req = dequeue()) != NULL)
  { printf ("Close socket for client %d\n", req->sockfd);
    close(req->sockfd);
  }
  printf ("Close server socket %d\n", sockfd);
  close(sockfd);
}


//void one_submission ()
//{ char fname[100];
//
//  printf ("Submission file: ");
//  scanf ("%s", &fname);
//  if (Debug) printf ("File name: %s has been submitted\n", fname);
//  submit_process (fname);
//}

//void *process_submissions ()
//{ char action;
//  char fname[100];
//
//  while (systemActive) one_submission ();
//  printf ("Client submission interface loop has ended!\n");
//}

// submission thread is not activated due to input competition
// pthread_t submissionThread;

//void start_client_submission ()
//{ int ret;
//
//  ret = pthread_create (&submissionThread, NULL, process_submissions, NULL);
//  if (ret < 0) printf ("Client submission thread creation problem\n");
//  else printf ("Client submission interface has been created successsfully\n");
//}

//void end_client_submission ()
//{ int ret;
//
//  ret = pthread_join (submissionThread, NULL);
//  printf ("Client submission interface has finished %d\n", ret);
//}

//void send_client_result(request_t *req, int result)
//{ char buffer[256];
//  int ret;
//
//  bzero(buffer, sizeof(buffer));
//  sprintf(buffer, "%s %d", req->client_id, result);
//  ret = send(req->sockfd, buffer, sizeof(buffer), 0);
//  if (ret < 0) error("Server ERROR writing to socket");
//}
