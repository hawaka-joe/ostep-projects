#include "io_helper.h"
#include "request.h"
#include <limits.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

char default_root[] = ".";

// Request structure
typedef struct {
  int fd;
  off_t file_size;       // For SFF scheduling
  char first_line[8192]; // For SFF scheduling (first line already read)
  int has_first_line;    // Whether first_line is valid
} request_t;

// Request queue structure
typedef struct {
  request_t *requests;
  int size;
  int count;
  int front;
  int rear;
  pthread_mutex_t mutex;
  pthread_cond_t not_full;
  pthread_cond_t not_empty;
  int shutdown;
} request_queue_t;

// Global variables
request_queue_t *queue;
int num_threads = 1;
int buffer_size = 1;
char *schedalg = "FIFO";

// Initialize request queue
request_queue_t *queue_init(int size) {
  request_queue_t *q = malloc(sizeof(request_queue_t));
  q->requests = malloc(sizeof(request_t) * size);
  q->size = size;
  q->count = 0;
  q->front = 0;
  q->rear = 0;
  q->shutdown = 0;
  pthread_mutex_init(&q->mutex, NULL);
  pthread_cond_init(&q->not_full, NULL);
  pthread_cond_init(&q->not_empty, NULL);
  return q;
}

// Destroy request queue
void queue_destroy(request_queue_t *q) {
  pthread_mutex_destroy(&q->mutex);
  pthread_cond_destroy(&q->not_full);
  pthread_cond_destroy(&q->not_empty);
  free(q->requests);
  free(q);
}

// Insert request into queue (FIFO)
void queue_insert_fifo(request_queue_t *q, request_t req) {
  pthread_mutex_lock(&q->mutex);
  while (q->count == q->size && !q->shutdown) {
    pthread_cond_wait(&q->not_full, &q->mutex);
  }
  if (q->shutdown) {
    pthread_mutex_unlock(&q->mutex);
    return;
  }
  q->requests[q->rear] = req;
  q->rear = (q->rear + 1) % q->size;
  q->count++;
  pthread_cond_signal(&q->not_empty);
  pthread_mutex_unlock(&q->mutex);
}

// Insert request into queue (SFF - sorted by file size)
void queue_insert_sff(request_queue_t *q, request_t req) {
  pthread_mutex_lock(&q->mutex);
  while (q->count == q->size && !q->shutdown) {
    pthread_cond_wait(&q->not_full, &q->mutex);
  }
  if (q->shutdown) {
    pthread_mutex_unlock(&q->mutex);
    return;
  }

  // Insert in sorted order (smallest file first)
  if (q->count == 0) {
    q->requests[q->rear] = req;
    q->rear = (q->rear + 1) % q->size;
  } else {
    // Find insertion point (first position where file_size > req.file_size)
    int insert_pos = q->rear; // Default: insert at end
    int i;
    for (i = 0; i < q->count; i++) {
      int pos = (q->front + i) % q->size;
      if (q->requests[pos].file_size > req.file_size) {
        insert_pos = pos;
        break;
      }
    }

    // Shift elements to make room for insertion
    if (insert_pos == q->rear) {
      // Insert at end (no shifting needed)
      q->requests[q->rear] = req;
      q->rear = (q->rear + 1) % q->size;
    } else {
      // Insert in middle: shift elements from insert_pos to rear-1
      // Move rear to make room
      int current = q->rear;

      // Shift elements backward (toward rear)
      while (current != insert_pos) {
        int prev = (current - 1 + q->size) % q->size;
        q->requests[current] = q->requests[prev];
        current = prev;
      }

      // Insert new element
      q->requests[insert_pos] = req;
      q->rear = (q->rear + 1) % q->size;
    }
  }

  q->count++;
  pthread_cond_signal(&q->not_empty);
  pthread_mutex_unlock(&q->mutex);
}

// Remove request from queue
request_t queue_remove(request_queue_t *q) {
  pthread_mutex_lock(&q->mutex);
  while (q->count == 0 && !q->shutdown) {
    pthread_cond_wait(&q->not_empty, &q->mutex);
  }

  request_t req = {-1, 0};
  if (q->shutdown && q->count == 0) {
    pthread_mutex_unlock(&q->mutex);
    return req;
  }

  req = q->requests[q->front];
  q->front = (q->front + 1) % q->size;
  q->count--;
  pthread_cond_signal(&q->not_full);
  pthread_mutex_unlock(&q->mutex);
  return req;
}

// Get file size for a request (for SFF scheduling)
// Returns file size, and stores first_line in the provided buffer
off_t get_file_size(int fd, char *first_line_buf) {
  char method[8192], uri[8192], version[8192];
  char filename[8192], cgiargs[8192];
  struct stat sbuf;

  // Read the request line
  ssize_t n = readline(fd, first_line_buf, 8192);
  if (n <= 0) {
    return -1;
  }

  sscanf(first_line_buf, "%s %s %s", method, uri, version);

  // Parse URI to get filename
  request_parse_uri(uri, filename, cgiargs);

  if (stat(filename, &sbuf) < 0) {
    // File doesn't exist - use a large size so it goes to end of queue
    // Worker thread will handle the 404 error
    return LONG_MAX;
  }

  return sbuf.st_size;
}

// Worker thread function
void *worker_thread(void *arg) {
  while (1) {
    request_t req = queue_remove(queue);
    if (req.fd == -1 && queue->shutdown) {
      break;
    }
    if (req.fd != -1) {
      if (req.has_first_line) {
        request_handle_with_first_line(req.fd, req.first_line);
      } else {
        request_handle(req.fd);
      }
      close_or_die(req.fd);
    }
  }
  return NULL;
}

//
// ./wserver [-d <basedir>] [-p <portnum>] [-t <threads>] [-b <buffers>] [-s
// <schedalg>]
//
int main(int argc, char *argv[]) {
  int c;
  char *root_dir = default_root;
  int port = 10000;

  while ((c = getopt(argc, argv, "d:p:t:b:s:")) != -1)
    switch (c) {
    case 'd':
      root_dir = optarg;
      break;
    case 'p':
      port = atoi(optarg);
      break;
    case 't':
      num_threads = atoi(optarg);
      if (num_threads <= 0) {
        fprintf(stderr, "threads must be positive\n");
        exit(1);
      }
      break;
    case 'b':
      buffer_size = atoi(optarg);
      if (buffer_size <= 0) {
        fprintf(stderr, "buffers must be positive\n");
        exit(1);
      }
      break;
    case 's':
      schedalg = optarg;
      if (strcmp(schedalg, "FIFO") != 0 && strcmp(schedalg, "SFF") != 0) {
        fprintf(stderr, "schedalg must be FIFO or SFF\n");
        exit(1);
      }
      break;
    default:
      fprintf(stderr, "usage: wserver [-d basedir] [-p port] [-t threads] [-b "
                      "buffers] [-s schedalg]\n");
      exit(1);
    }

  // run out of this directory
  chdir_or_die(root_dir);

  // Initialize request queue
  queue = queue_init(buffer_size);

  // Create worker threads
  pthread_t *threads = malloc(sizeof(pthread_t) * num_threads);
  for (int i = 0; i < num_threads; i++) {
    pthread_create(&threads[i], NULL, worker_thread, NULL);
  }

  // now, get to work
  int listen_fd = open_listen_fd_or_die(port);
  while (1) {
    struct sockaddr_in client_addr;
    int client_len = sizeof(client_addr);
    int conn_fd = accept_or_die(listen_fd, (sockaddr_t *)&client_addr,
                                (socklen_t *)&client_len);

    request_t req;
    req.fd = conn_fd;
    req.has_first_line = 0;

    if (strcmp(schedalg, "SFF") == 0) {
      // For SFF, need to get file size first
      // This reads the first line and stores it
      req.file_size = get_file_size(conn_fd, req.first_line);
      if (req.file_size < 0) {
        // If we can't read the request line, treat as error
        request_error(conn_fd, "", "400", "Bad Request",
                      "Could not read request");
        close_or_die(conn_fd);
        continue;
      }
      req.has_first_line = 1;
      queue_insert_sff(queue, req);
    } else {
      // FIFO scheduling
      req.file_size = 0; // Not used for FIFO
      req.has_first_line = 0;
      queue_insert_fifo(queue, req);
    }
  }

  // Cleanup (this code won't normally be reached)
  queue->shutdown = 1;
  pthread_cond_broadcast(&queue->not_empty);
  for (int i = 0; i < num_threads; i++) {
    pthread_join(threads[i], NULL);
  }
  free(threads);
  queue_destroy(queue);

  return 0;
}
