
#include "io_helper.h"
#include <pthread.h>
#include <getopt.h>

#define MAXBUF (8192)

// Thread arguments structure
typedef struct {
  char *host;
  int port;
  char *filename;
} client_thread_args_t;

//
// Send an HTTP request for the specified file
//
void client_send(int fd, char *filename) {
  char buf[MAXBUF];
  char hostname[MAXBUF];

  gethostname_or_die(hostname, MAXBUF);

  /* Form and send the HTTP request */
  sprintf(buf, "GET %s HTTP/1.1\n", filename);
  sprintf(buf, "%shost: %s\n\r\n", buf, hostname);
  write_or_die(fd, buf, strlen(buf));
}

//
// Read the HTTP response and print it out
//
void client_print(int fd) {
  char buf[MAXBUF];
  int n;

  // Read and display the HTTP Header
  n = readline_or_die(fd, buf, MAXBUF);
  while (strcmp(buf, "\r\n") && (n > 0)) {
    printf("Header: %s", buf);
    n = readline_or_die(fd, buf, MAXBUF);

    // If you want to look for certain HTTP tags...
    // int length = 0;
    // if (sscanf(buf, "Content-Length: %d ", &length) == 1) {
    //    printf("Length = %d\n", length);
    //}
  }

  // Read and display the HTTP Body
  n = readline_or_die(fd, buf, MAXBUF);
  while (n > 0) {
    printf("%s", buf);
    n = readline_or_die(fd, buf, MAXBUF);
  }
}

//
// Thread function: each thread sends a request and prints the response
//
void *client_thread(void *arg) {
  client_thread_args_t *args = (client_thread_args_t *)arg;
  int clientfd;

  /* Open a connection to the specified host and port */
  clientfd = open_client_fd_or_die(args->host, args->port);

  client_send(clientfd, args->filename);
  client_print(clientfd);

  close_or_die(clientfd);

  return NULL;
}

int main(int argc, char *argv[]) {
  char *host, *filename;
  int port;
  int num_threads = 1;  // Default to 1 thread for backward compatibility
  int c;
  int host_idx = -1, port_idx = -1, filename_idx = -1;

  // First, try to parse -n option if it appears before positional arguments
  opterr = 0;  // Suppress getopt error messages
  optind = 1;  // Reset optind
  
  while ((c = getopt(argc, argv, "n:")) != -1) {
    switch (c) {
    case 'n':
      num_threads = atoi(optarg);
      if (num_threads <= 0) {
        fprintf(stderr, "Error: number of threads must be positive\n");
        exit(1);
      }
      break;
    case '?':
      break;
    default:
      break;
    }
  }

  // Now manually scan all arguments to find -n if it appears after positional args
  // and also find the positional arguments
  for (int i = 1; i < argc; i++) {
    if (strcmp(argv[i], "-n") == 0 && i + 1 < argc) {
      num_threads = atoi(argv[i + 1]);
      if (num_threads <= 0) {
        fprintf(stderr, "Error: number of threads must be positive\n");
        exit(1);
      }
      i++;  // Skip the next argument (the number)
    } else if (argv[i][0] != '-' && host_idx == -1) {
      host_idx = i;
    } else if (argv[i][0] != '-' && host_idx != -1 && port_idx == -1) {
      port_idx = i;
    } else if (argv[i][0] != '-' && port_idx != -1 && filename_idx == -1) {
      filename_idx = i;
    }
  }

  // Check if we found all required positional arguments
  if (host_idx == -1 || port_idx == -1 || filename_idx == -1) {
    fprintf(stderr, "Usage: %s <host> <port> <filename> [-n <num_threads>]\n", argv[0]);
    fprintf(stderr, "   or: %s -n <num_threads> <host> <port> <filename>\n", argv[0]);
    fprintf(stderr, "  -n: number of concurrent threads (default: 1)\n");
    exit(1);
  }

  host = argv[host_idx];
  port = atoi(argv[port_idx]);
  filename = argv[filename_idx];

  // If num_threads is 1, use the original single-threaded approach for backward compatibility
  if (num_threads == 1) {
    int clientfd = open_client_fd_or_die(host, port);
    client_send(clientfd, filename);
    client_print(clientfd);
    close_or_die(clientfd);
    exit(0);
  }

  // Multi-threaded mode: create threads
  pthread_t *threads = malloc(sizeof(pthread_t) * num_threads);
  client_thread_args_t *args = malloc(sizeof(client_thread_args_t) * num_threads);

  if (!threads || !args) {
    fprintf(stderr, "Error: failed to allocate memory for threads\n");
    exit(1);
  }

  // Initialize thread arguments
  for (int i = 0; i < num_threads; i++) {
    args[i].host = host;
    args[i].port = port;
    args[i].filename = filename;
  }

  // Create threads
  for (int i = 0; i < num_threads; i++) {
    if (pthread_create(&threads[i], NULL, client_thread, &args[i]) != 0) {
      fprintf(stderr, "Error: failed to create thread %d\n", i);
      exit(1);
    }
  }

  // Wait for all threads to complete
  for (int i = 0; i < num_threads; i++) {
    pthread_join(threads[i], NULL);
  }

  free(threads);
  free(args);

  exit(0);
}
