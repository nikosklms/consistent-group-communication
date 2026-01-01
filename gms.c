#include <arpa/inet.h>
#include <errno.h>
#include <ifaddrs.h> // Needed for getifaddrs()
#include <net/if.h>  // Needed for IFF_LOOPBACK in interface flags
#include <netinet/in.h>
#include <pthread.h>
#include <signal.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/select.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <time.h>
#include <unistd.h>

// --- Configuration ---
#define DEFAULT_PORT 8080
#define MAX_PENDING_CONNECTIONS 128
#define GMS_BUFFER_SIZE 2048
#define SEND_BUFFER_SIZE 2048
#define INITIAL_HASH_TABLE_SIZE 16
#define MAX_LOAD_FACTOR 0.75
#define HASH_TABLE_GROWTH_FACTOR 2
#define MAX_ID_LEN 128
#define MAX_GROUP_LEN 128
#define MAX_ERROR_MSG_LEN 128

// --- Liveness Configuration ---
#define LIVENESS_CHECK_INTERVAL 5
#define LIVENESS_TIMEOUT 15

// --- Message Types ---
#define MSG_TYPE_JOIN 0
#define MSG_TYPE_LEAVE 1
#define MSG_TYPE_PONG 2
// responses
// 0-9
#define RESP_ACK_SUCCESS 0
#define RESP_NACK_DUPLICATE_ID 1
#define RESP_NACK_GROUP_FULL 2
#define RESP_NACK_INTERNAL_ERROR 3
#define RESP_NACK_INVALID_FORMAT 4
#define RESP_NACK_NOT_IN_GROUP 5
// 10-19
#define NOTIFY_MEMBER_JOINED 10
#define NOTIFY_MEMBER_LEFT 11
#define NOTIFY_MEMBER_FAILED 12
// 20-29
#define MSG_TYPE_PING 20

// --- Data Structures ---

typedef struct {
  char *id;
  struct sockaddr_in udp_addr;
  int tcp_client_fd;
  time_t last_pong_time;
} member_t;

typedef struct member_node {
  member_t member;
  struct member_node *next;
} member_node_t;

typedef struct group_node {
  char *group_name;
  member_node_t *members_head;
  int member_count;
  struct group_node *next;
} group_node_t;

typedef struct {
  group_node_t **buckets;
  size_t size;
  size_t num_groups;
  pthread_mutex_t lock;
} hash_table_t;

typedef struct send_item {
  int target_fd;
  uint8_t buffer[SEND_BUFFER_SIZE];
  size_t len;
  struct send_item *next;
} send_item_t;

typedef struct {
  send_item_t *head;
  send_item_t *tail;
  pthread_mutex_t lock;
  pthread_cond_t cond;
  int active;
} send_queue_t;

typedef struct {
  int client_fd;
  struct sockaddr_in client_addr;
  hash_table_t *group_table;
  send_queue_t *sender_queue;
} client_handler_args_t;

typedef struct {
  hash_table_t *group_table;
  send_queue_t *sender_queue;
  int active;
} liveness_args_t;

typedef struct {
  int listen_fd;
  hash_table_t *group_table;
  send_queue_t *sender_queue;
} receiver_args_t;

// --- Global Variables ---
volatile sig_atomic_t terminate_server = 0;

// --- Function Prototypes ---

// Hash Table
unsigned long hash_function(const char *str, size_t table_size);
hash_table_t *create_hash_table(size_t initial_size);
void free_hash_table(hash_table_t *table);
int hash_table_resize(hash_table_t *table);
int add_to_group(hash_table_t *table, const char *group_name, member_t new_member);
int rmv_from_group(hash_table_t *table, const char *group_name, const char *member_id,
                   send_queue_t *queue);
member_node_t *find_member_node(group_node_t *group, const char *member_id);
void free_group_node(group_node_t *node);

// Sender Queue
send_queue_t *create_send_queue();
void destroy_send_queue(send_queue_t *queue);
int enqueue_send_item(send_queue_t *queue, int fd, const uint8_t *data, size_t len);
send_item_t *dequeue_send_item(send_queue_t *queue);

// Threads
void *receiver_thread_func(void *arg);
void *sender_thread_func(void *arg);
void *liveness_thread_func(void *arg);
void *handle_client_thread_entry(void *arg);

// Failure Handling
void handle_member_failure(hash_table_t *table, int failed_fd,
                           const char *failed_ip, // Keep IP for logging/context
                           send_queue_t *queue);

// Helpers
void error_exit(const char *msg) __attribute__((noreturn));
member_t create_member(const char *id, const struct sockaddr_in *udp_addr, int tcp_fd);
void free_member(member_t *member);
void print_sock_info(int sockfd);
void print_groups(hash_table_t *table);
int send_join_response(send_queue_t *queue, int client_fd, uint8_t response_type,
                       const char *group_name, hash_table_t *table, const char *new_member_id);
int send_leave_response(send_queue_t *queue, int client_fd, uint8_t response_type);
int send_notification(send_queue_t *queue, int member_tcp_fd, uint8_t notification_type,
                      const char *group_name, const member_t *affected_member);
int send_ping(send_queue_t *queue, int member_tcp_fd);
void notify_group_members(send_queue_t *queue, hash_table_t *table, const char *group_name,
                          const member_t *changed_member, uint8_t notification_type);

// --- Helper Function Implementations ---

void error_exit(const char *msg) {
  perror(msg);
  exit(EXIT_FAILURE);
}

void print_sock_info(int sockfd) {
  // Find and print the first non-loopback IPv4 address
  char specific_ip[INET_ADDRSTRLEN] = "N/A"; // Buffer for the found IP
  struct ifaddrs *ifaddr, *ifa;
  int family;

  if (getifaddrs(&ifaddr) == -1) {
    perror("getifaddrs failed");
    // Continue without specific IP, already printed 0.0.0.0
  } else {
    for (ifa = ifaddr; ifa != NULL; ifa = ifa->ifa_next) {
      if (ifa->ifa_addr == NULL)
        continue;

      family = ifa->ifa_addr->sa_family;

      // Check if it is a valid IPv4 address and not loopback
      if (family == AF_INET && !(ifa->ifa_flags & IFF_LOOPBACK)) {
        struct sockaddr_in *addr = (struct sockaddr_in *)ifa->ifa_addr;
        // Convert the IP address to string format
        if (inet_ntop(AF_INET, &addr->sin_addr, specific_ip, sizeof(specific_ip)) != NULL) {
          // Found the first suitable address, break the loop
          break;
        } else {
          perror("inet_ntop failed");
          // Reset specific_ip if conversion failed
          strcpy(specific_ip, "Error");
        }
      }
    }
    freeifaddrs(ifaddr); // Free the memory allocated by getifaddrs
  }

  printf("GMS Server reachable on local network at: %s:%d\n", specific_ip, DEFAULT_PORT);
  // *** END MODIFIED PRINTING ***
}

member_t create_member(const char *id, const struct sockaddr_in *udp_addr, int tcp_fd) {
  member_t m;
  m.id = NULL;
  m.tcp_client_fd = -1;
  memset(&m.udp_addr, 0, sizeof(m.udp_addr));
  m.last_pong_time = 0;

  if (!id) {
    fprintf(stderr, "create_member: Error - NULL ID provided.\n");
    return m;
  }
  m.id = strdup(id);
  if (!m.id) {
    perror("create_member: strdup failed");
    return m;
  }
  if (udp_addr) {
    m.udp_addr = *udp_addr;
  }
  m.tcp_client_fd = tcp_fd;
  m.last_pong_time = time(NULL);
  return m;
}

void free_member(member_t *member) {
  if (member) {
    if (member->id) {
      free(member->id);
      member->id = NULL;
    }
    member->tcp_client_fd = -1;
  }
}

// --- Hash Table Implementation ---

unsigned long hash_function(const char *str, size_t table_size) {
  unsigned long hash = 5381;
  int c;
  if (!str || table_size == 0) {
    return 0;
  }
  while ((c = *str++)) {
    hash = ((hash << 5) + hash) + c; // hash * 33 + c
  }
  return hash % table_size;
}

hash_table_t *create_hash_table(size_t initial_size) {
  hash_table_t *table = malloc(sizeof(hash_table_t));
  if (!table) {
    error_exit("create_hash_table: malloc table failed");
  }

  table->size = (initial_size > 0) ? initial_size : 1;
  table->num_groups = 0;
  table->buckets = calloc(table->size, sizeof(group_node_t *));
  if (!table->buckets) {
    free(table);
    error_exit("create_hash_table: calloc buckets failed");
  }

  if (pthread_mutex_init(&table->lock, NULL) != 0) {
    free(table->buckets);
    free(table);
    error_exit("create_hash_table: mutex init failed");
  }
  printf("Hash table created with initial size %zu\n", table->size);
  return table;
}

void free_group_node(group_node_t *node) {
  if (!node) {
    return;
  }
  free(node->group_name);
  member_node_t *current_member = node->members_head;
  while (current_member != NULL) {
    member_node_t *next_member = current_member->next;
    free_member(&current_member->member);
    free(current_member);
    current_member = next_member;
  }
  free(node);
}

void free_hash_table(hash_table_t *table) {
  if (!table) {
    return;
  }
  // Assuming no other threads access after this point, lock not needed
  // pthread_mutex_lock(&table->lock);
  pthread_mutex_destroy(&table->lock); // Destroy before freeing buckets
  for (size_t i = 0; i < table->size; i++) {
    group_node_t *current_group = table->buckets[i];
    while (current_group != NULL) {
      group_node_t *next_group = current_group->next;
      free_group_node(current_group);
      current_group = next_group;
    }
  }
  // pthread_mutex_unlock(&table->lock);
  free(table->buckets);
  free(table);
  printf("Hash table freed.\n");
}

int hash_table_resize(hash_table_t *table) {
  // Assumes lock is held by caller
  if (!table) {
    return -1;
  }
  size_t old_size = table->size;
  group_node_t **old_buckets = table->buckets;
  size_t new_size = old_size * HASH_TABLE_GROWTH_FACTOR;
  if (new_size <= old_size) {
    new_size = old_size + 1; // Ensure growth
  }

  printf("Resizing hash table: old=%zu, new=%zu, groups=%zu\n", old_size, new_size,
         table->num_groups);

  group_node_t **new_buckets = calloc(new_size, sizeof(group_node_t *));
  if (!new_buckets) {
    fprintf(stderr, "ERROR: resize: calloc failed for new buckets!\n");
    return -1; // Keep old table
  }

  // Rehash all existing groups
  for (size_t i = 0; i < old_size; i++) {
    group_node_t *current_group = old_buckets[i];
    while (current_group != NULL) {
      group_node_t *next_group = current_group->next;
      unsigned long new_index = hash_function(current_group->group_name, new_size);
      // Insert at head of the new bucket's list
      current_group->next = new_buckets[new_index];
      new_buckets[new_index] = current_group;
      current_group = next_group;
    }
  }
  free(old_buckets); // Free the old array of pointers
  table->buckets = new_buckets;
  table->size = new_size;
  printf("Hash table resize complete. New size: %zu\n", new_size);
  return 0;
}

member_node_t *find_member_node(group_node_t *group, const char *member_id) {
  if (!group || !member_id) {
    return NULL;
  }
  member_node_t *current = group->members_head;
  while (current != NULL) {
    // Check if member ID exists before comparing
    if (current->member.id && strcmp(current->member.id, member_id) == 0) {
      return current;
    }
    current = current->next;
  }
  return NULL;
}

int add_to_group(hash_table_t *table, const char *group_name, member_t new_member) {
  if (!table || !group_name || !new_member.id || new_member.tcp_client_fd < 0) {
    if (new_member.id) {
      free(new_member.id); // Clean up if input was invalid
    }
    return RESP_NACK_INTERNAL_ERROR;
  }

  pthread_mutex_lock(&table->lock);

  unsigned long index = hash_function(group_name, table->size);
  group_node_t *current_group = table->buckets[index];

  // Find group
  while (current_group != NULL && strcmp(current_group->group_name, group_name) != 0) {
    current_group = current_group->next;
  }

  // --- Create group if not found ---
  if (current_group == NULL) {
    // Check for resize before creating node
    double load_check = (table->size > 0) ? (double)(table->num_groups + 1) / table->size : 0.0;
    if (load_check > MAX_LOAD_FACTOR) {
      printf("Resize needed before adding group '%s'\n", group_name);
      if (hash_table_resize(table) != 0) {
        fprintf(stderr, "add_to_group: Resize failed!\n");
        free_member(&new_member);
        pthread_mutex_unlock(&table->lock);
        return RESP_NACK_INTERNAL_ERROR;
      }
      // Recalculate index and retry find
      index = hash_function(group_name, table->size);
      current_group = table->buckets[index];
      while (current_group != NULL && strcmp(current_group->group_name, group_name) != 0) {
        current_group = current_group->next;
      }
      if (current_group != NULL) { // Should not happen
        fprintf(stderr, "CRITICAL: Group '%s' found after resize!\n", group_name);
        free_member(&new_member);
        pthread_mutex_unlock(&table->lock);
        return RESP_NACK_INTERNAL_ERROR;
      }
    }
    // Create the group node
    current_group = malloc(sizeof(group_node_t));
    if (!current_group) {
      perror("add_to_group: malloc group_node failed");
      free_member(&new_member);
      pthread_mutex_unlock(&table->lock);
      return RESP_NACK_INTERNAL_ERROR;
    }
    current_group->group_name = strdup(group_name);
    if (!current_group->group_name) {
      perror("add_to_group: strdup group_name failed");
      free(current_group);
      free_member(&new_member);
      pthread_mutex_unlock(&table->lock);
      return RESP_NACK_INTERNAL_ERROR;
    }
    current_group->members_head = NULL;
    current_group->member_count = 0;
    // Link into bucket list (insert at head)
    current_group->next = table->buckets[index];
    table->buckets[index] = current_group;
    table->num_groups++;
    printf("Created group: %s (Total groups: %zu)\n", group_name, table->num_groups);
  }

  // --- Check for duplicate member ID ---
  if (find_member_node(current_group, new_member.id) != NULL) {
    printf("Member '%s' already in group '%s'. Join rejected.\n", new_member.id, group_name);
    free_member(&new_member); // Free the passed-in member struct
    pthread_mutex_unlock(&table->lock);
    return RESP_NACK_DUPLICATE_ID;
  }

  // --- Add new member node ---
  member_node_t *new_member_node = malloc(sizeof(member_node_t));
  if (!new_member_node) {
    perror("add_to_group: malloc member_node failed");
    free_member(&new_member);
    pthread_mutex_unlock(&table->lock);
    return RESP_NACK_INTERNAL_ERROR;
  }
  new_member_node->member = new_member; // Transfer ownership
  new_member_node->next = NULL;         // New node is the last one

  // Find the tail of the list
  if (current_group->members_head == NULL) {
    // List was empty, new node is the head
    current_group->members_head = new_member_node;
  } else {
    // Traverse to the end
    member_node_t *tail_finder = current_group->members_head;
    while (tail_finder->next != NULL) {
      tail_finder = tail_finder->next;
    }
    // Link the previous tail to the new node
    tail_finder->next = new_member_node;
  }
  // ------------------------------------
  current_group->member_count++;
  printf("Added member '%s' to group '%s'. Members: %d.\n", new_member.id, group_name,
         current_group->member_count);

  pthread_mutex_unlock(&table->lock);
  print_groups(table);
  return RESP_ACK_SUCCESS;
}

int rmv_from_group(hash_table_t *table, const char *group_name, const char *member_id,
                   send_queue_t *queue) {
  if (!table || !group_name || !member_id || !queue) {
    return RESP_NACK_INTERNAL_ERROR;
  }

  int result = RESP_NACK_NOT_IN_GROUP;
  member_t leaving_member_info;
  leaving_member_info.id = NULL;
  int notify_needed = 0;
  group_node_t *group_to_remove = NULL;

  pthread_mutex_lock(&table->lock); // --- LOCK ---

  unsigned long index = hash_function(group_name, table->size);
  group_node_t *current_group = table->buckets[index];
  group_node_t *prev_group = NULL;

  // Find group
  while (current_group != NULL && strcmp(current_group->group_name, group_name) != 0) {
    prev_group = current_group;
    current_group = current_group->next;
  }
  if (current_group == NULL) { // Group not found
    pthread_mutex_unlock(&table->lock);
    return result;
  }

  // Find member
  member_node_t *current_member_node = current_group->members_head;
  member_node_t *prev_member_node = NULL;
  while (current_member_node != NULL) {
    if (current_member_node->member.id && strcmp(current_member_node->member.id, member_id) == 0) {
      break; // Found
    }
    prev_member_node = current_member_node;
    current_member_node = current_member_node->next;
  }
  if (current_member_node == NULL) { // Member not found
    pthread_mutex_unlock(&table->lock);
    return result;
  }

  // --- Member found - Remove (inside lock) ---
  printf("Removing member '%s' from group '%s'.\n", member_id, group_name);

  leaving_member_info = current_member_node->member;
  leaving_member_info.id = strdup(member_id); // Copy ID for notification
  if (!leaving_member_info.id) {
    perror("rmv_from_group: strdup failed");
    pthread_mutex_unlock(&table->lock);
    return RESP_NACK_INTERNAL_ERROR;
  }

  // Unlink node
  if (prev_member_node == NULL) {
    current_group->members_head = current_member_node->next;
  } else {
    prev_member_node->next = current_member_node->next;
  }

  // Free original node data and node struct
  free(current_member_node->member.id);
  free(current_member_node);
  current_group->member_count--;
  result = RESP_ACK_SUCCESS;
  notify_needed = 1;

  // Check if group became empty
  if (current_group->member_count == 0) {
    printf("Group '%s' empty, removing.\n", group_name);
    if (prev_group == NULL) { // Group was first in bucket
      table->buckets[index] = current_group->next;
    } else {
      prev_group->next = current_group->next;
    }
    // Mark group for removal after unlock
    group_to_remove = current_group;
    table->num_groups--;
    printf("Total groups now: %zu\n", table->num_groups);
  }

  pthread_mutex_unlock(&table->lock); // --- UNLOCK ---

  // --- Operations after Unlock ---
  if (group_to_remove) {
    free_group_node(group_to_remove); // Free group node now
  }
  if (notify_needed) {
    printf("Queueing notification for LEAVE: %s\n", leaving_member_info.id);
    notify_group_members(queue, table, group_name, &leaving_member_info, NOTIFY_MEMBER_LEFT);
    print_groups(table);
  }
  if (leaving_member_info.id) {
    free(leaving_member_info.id); // Free the copied ID
  }
  return result;
}

void print_groups(hash_table_t *table) {
  if (!table) {
    return;
  }
  printf("\n--- Current Groups (Size: %zu, Count: %zu) ---\n", table->size, table->num_groups);

  pthread_mutex_lock(&table->lock);
  int groups_found = 0;
  for (size_t i = 0; i < table->size; i++) {
    group_node_t *group = table->buckets[i];
    if (group != NULL) {
      groups_found = 1;
      printf(" Bucket %zu:\n", i);
      while (group != NULL) {
        printf("  -> Group: %s (Members: %d)\n", group->group_name, group->member_count);
        member_node_t *member_node = group->members_head;
        if (member_node == NULL) {
          printf("      (No members)\n");
        }
        while (member_node != NULL) {
          char udp_ip[INET_ADDRSTRLEN];
          const char *id_str = member_node->member.id ? member_node->member.id : "null";
          if (member_node->member.udp_addr.sin_family == AF_INET) {
            inet_ntop(AF_INET, &(member_node->member.udp_addr.sin_addr), udp_ip, INET_ADDRSTRLEN);
          } else {
            strcpy(udp_ip, "N/A");
          }
          printf("      - ID: %s "
                 "(UDP: %s:%d, FD: %d, Pong: %ld)\n",
                 id_str, udp_ip, ntohs(member_node->member.udp_addr.sin_port),
                 member_node->member.tcp_client_fd, member_node->member.last_pong_time);
          member_node = member_node->next;
        }
        group = group->next;
      } // end while(group)
    } // end if(group != NULL)
  } // end for
  if (!groups_found) {
    printf("   (No groups currently exist)\n");
  }
  pthread_mutex_unlock(&table->lock);
  printf("--- End Groups ---\n\n");
}

// --- Sender Queue Implementation ---

send_queue_t *create_send_queue() {
  send_queue_t *q = malloc(sizeof(send_queue_t));
  if (!q) {
    error_exit("malloc send_queue");
  }
  q->head = q->tail = NULL;
  if (pthread_mutex_init(&q->lock, NULL) != 0) {
    free(q);
    error_exit("SendQ mutex");
  }
  if (pthread_cond_init(&q->cond, NULL) != 0) {
    pthread_mutex_destroy(&q->lock);
    free(q);
    error_exit("SendQ cond");
  }
  q->active = 1;
  printf("Send queue created.\n");
  return q;
}

void destroy_send_queue(send_queue_t *q) {
  if (!q) {
    return;
  }
  pthread_mutex_lock(&q->lock);
  q->active = 0;
  pthread_cond_broadcast(&q->cond); // Wake up sender
  send_item_t *curr = q->head;
  while (curr) {
    send_item_t *next = curr->next;
    free(curr);
    curr = next;
  }
  q->head = q->tail = NULL;
  pthread_mutex_unlock(&q->lock);
  pthread_cond_destroy(&q->cond);
  pthread_mutex_destroy(&q->lock);
  free(q);
  printf("Send queue destroyed.\n");
}

int enqueue_send_item(send_queue_t *q, int fd, const uint8_t *data, size_t len) {
  if (!q || !data || len == 0 || len > SEND_BUFFER_SIZE || fd < 0) {
    fprintf(stderr, "enqueue_send_item: Invalid args\n");
    return -1;
  }
  send_item_t *item = malloc(sizeof(send_item_t));
  if (!item) {
    perror("malloc send_item");
    return -1;
  }
  item->target_fd = fd;
  memcpy(item->buffer, data, len);
  item->len = len;
  item->next = NULL;

  pthread_mutex_lock(&q->lock);
  if (!q->active) { // Don't enqueue if shutting down
    pthread_mutex_unlock(&q->lock);
    free(item);
    printf("Warning: Tried enqueue on inactive queue.\n");
    return -1;
  }
  if (!q->tail) { // Empty queue
    q->head = q->tail = item;
  } else {
    q->tail->next = item;
    q->tail = item;
  }
  pthread_cond_signal(&q->cond); // Signal sender
  pthread_mutex_unlock(&q->lock);
  return 0;
}

send_item_t *dequeue_send_item(send_queue_t *q) {
  if (!q) {
    return NULL;
  }
  pthread_mutex_lock(&q->lock);
  while (!q->head && q->active) { // Loop while empty and active
    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    ts.tv_sec += 1; // 1 second timeout
    pthread_cond_timedwait(&q->cond, &q->lock, &ts);
    // Re-evaluate loop condition after wait/timeout
  }
  if (!q->head) { // Still empty (must be inactive)
    pthread_mutex_unlock(&q->lock);
    return NULL;
  }
  // Dequeue
  send_item_t *item = q->head;
  q->head = item->next;
  if (!q->head) {
    q->tail = NULL; // Queue became empty
  }
  pthread_mutex_unlock(&q->lock);
  return item;
}

// --- Sending Helper (Enqueue) Functions ---

int send_join_response(send_queue_t *q, int fd, uint8_t resp_type, const char *grp_name,
                       hash_table_t *tbl, const char *new_id) {
  if (fd < 0) {
    return -1;
  }
  uint8_t buf[SEND_BUFFER_SIZE];
  size_t off = 0;

  // Need space for at least the type byte
  if (off + sizeof(uint8_t) > sizeof(buf)) {
    return -1;
  }
  buf[off++] = resp_type;

  if (resp_type == RESP_ACK_SUCCESS) {
    if (!grp_name || !tbl || !new_id) {
      return -1;
    } // Invalid args for ACK

    pthread_mutex_lock(&tbl->lock);
    unsigned long idx = hash_function(grp_name, tbl->size);
    group_node_t *g = tbl->buckets[idx];
    while (g && strcmp(g->group_name, grp_name) != 0) {
      g = g->next;
    }
    if (!g) { // Group not found after successful add? Should not happen.
      pthread_mutex_unlock(&tbl->lock);
      fprintf(stderr, "send_join_resp: Group '%s' vanished!\n", grp_name);
      return -1;
    }

    // Count members and check size
    uint16_t cnt = 0;
    size_t list_size = 0;
    member_node_t *node_cnt = g->members_head;
    while (node_cnt) {
      if (node_cnt->member.id && strcmp(node_cnt->member.id, new_id) != 0) {
        cnt++;
        size_t idl = strlen(node_cnt->member.id);
        if (idl > MAX_ID_LEN)
          idl = MAX_ID_LEN;
        list_size += sizeof(uint32_t) + sizeof(uint16_t) + sizeof(uint8_t) + idl;
      }
      node_cnt = node_cnt->next;
    }

    // Check total required size
    if (off + sizeof(uint16_t) + list_size > sizeof(buf)) {
      pthread_mutex_unlock(&tbl->lock);
      fprintf(stderr, "ACK buffer too small for '%s'\n", grp_name);
      return -1;
    }

    // Serialize count
    uint16_t net_cnt = htons(cnt);
    memcpy(buf + off, &net_cnt, sizeof(uint16_t));
    off += sizeof(uint16_t);

    // Serialize members
    member_node_t *mn = g->members_head;
    while (mn) {
      if (!mn->member.id || strcmp(mn->member.id, new_id) == 0) {
        mn = mn->next;
        continue;
      }
      uint32_t ip = mn->member.udp_addr.sin_addr.s_addr;
      uint16_t port = mn->member.udp_addr.sin_port;
      uint8_t idl = strlen(mn->member.id);
      if (idl > MAX_ID_LEN)
        idl = MAX_ID_LEN;

      // Copy fields one by one
      memcpy(buf + off, &ip, sizeof(uint32_t));
      off += sizeof(uint32_t);
      memcpy(buf + off, &port, sizeof(uint16_t));
      off += sizeof(uint16_t);
      buf[off++] = idl;
      memcpy(buf + off, mn->member.id, idl);
      off += idl;

      mn = mn->next;
    }
    pthread_mutex_unlock(&tbl->lock);
  } else {                         // NACK
    const char *m = "Join failed"; // Default
    if (resp_type == RESP_NACK_DUPLICATE_ID) {
      m = "ID already exists in group";
    } else if (resp_type == RESP_NACK_INTERNAL_ERROR) {
      m = "Internal server error";
    } else if (resp_type == RESP_NACK_INVALID_FORMAT) {
      m = "Invalid request format";
    }
    // Add cases for other NACKs if needed

    uint8_t l = strlen(m);
    if (l > MAX_ERROR_MSG_LEN) {
      l = MAX_ERROR_MSG_LEN;
    }
    if (off + sizeof(uint8_t) + l <= sizeof(buf)) {
      buf[off++] = l;
      memcpy(buf + off, m, l);
      off += l;
    } else {          // Not enough space for message
      buf[off++] = 0; // Send NACK type with 0 length message
    }
  }
  return enqueue_send_item(q, fd, buf, off);
}

int send_leave_response(send_queue_t *q, int fd, uint8_t resp_type) {
  if (fd < 0) {
    return -1;
  }
  uint8_t buf[SEND_BUFFER_SIZE];
  size_t off = 0;

  // Response Type
  if (off + sizeof(uint8_t) > sizeof(buf)) {
    return -1;
  }
  buf[off++] = resp_type;

  // Add error message for NACK cases
  if (resp_type != RESP_ACK_SUCCESS) {
    const char *m = "Leave failed"; // Default
    if (resp_type == RESP_NACK_NOT_IN_GROUP) {
      m = "Not member or group missing";
    } else if (resp_type == RESP_NACK_INTERNAL_ERROR) {
      m = "Internal server error on leave";
    }
    // Add other specific leave NACK messages if needed

    uint8_t l = strlen(m);
    if (l > MAX_ERROR_MSG_LEN) {
      l = MAX_ERROR_MSG_LEN;
    }
    if (off + sizeof(uint8_t) + l <= sizeof(buf)) {
      buf[off++] = l;
      memcpy(buf + off, m, l);
      off += l;
    } else { // Not enough space for message
      buf[off++] = 0;
    }
  }
  return enqueue_send_item(q, fd, buf, off);
}

int send_notification(send_queue_t *q, int fd, uint8_t n_type, const char *g_name,
                      const member_t *aff_mem) {
  if (fd < 0 || !g_name || !aff_mem || !aff_mem->id) {
    return -1;
  }
  uint8_t buf[SEND_BUFFER_SIZE];
  size_t off = 0;

  // Serialize Type
  if (off + sizeof(uint8_t) > sizeof(buf)) {
    return -1;
  }
  buf[off++] = n_type;

  // Serialize Group Name
  uint8_t gl = strlen(g_name);
  if (gl > MAX_GROUP_LEN) {
    gl = MAX_GROUP_LEN;
  }
  if (off + sizeof(uint8_t) + gl > sizeof(buf)) {
    return -1;
  }
  buf[off++] = gl;
  memcpy(buf + off, g_name, gl);
  off += gl;

  // Serialize Member ID
  uint8_t il = strlen(aff_mem->id);
  if (il > MAX_ID_LEN) {
    il = MAX_ID_LEN;
  }
  if (off + sizeof(uint8_t) + il > sizeof(buf)) {
    return -1;
  }
  buf[off++] = il;
  memcpy(buf + off, aff_mem->id, il);
  off += il;

  // Serialize IP and Port (Network Byte Order)
  uint32_t ip = aff_mem->udp_addr.sin_addr.s_addr;
  uint16_t port = aff_mem->udp_addr.sin_port;
  if (off + sizeof(uint32_t) + sizeof(uint16_t) > sizeof(buf)) {
    return -1;
  }
  memcpy(buf + off, &ip, sizeof(uint32_t));
  off += sizeof(uint32_t);
  memcpy(buf + off, &port, sizeof(uint16_t));
  off += sizeof(uint16_t);

  return enqueue_send_item(q, fd, buf, off);
}

int send_ping(send_queue_t *q, int fd) {
  if (fd < 0) {
    return -1;
  }
  uint8_t t = MSG_TYPE_PING;
  return enqueue_send_item(q, fd, &t, sizeof(t));
}

void notify_group_members(send_queue_t *q, hash_table_t *tbl, const char *g_name,
                          const member_t *chg_mem, uint8_t n_type) {
  if (!q || !tbl || !g_name || !chg_mem || !chg_mem->id) {
    return;
  }

  // Need to copy target FDs first to avoid holding lock during enqueue
  int target_fds[MAX_ID_LEN]; // Assuming max members <= MAX_ID_LEN for simplicity
  int target_count = 0;

  pthread_mutex_lock(&tbl->lock);
  unsigned long idx = hash_function(g_name, tbl->size);
  group_node_t *g = tbl->buckets[idx];
  while (g && strcmp(g->group_name, g_name) != 0) {
    g = g->next;
  }
  if (g) {
    member_node_t *n = g->members_head;
    while (n && target_count < MAX_ID_LEN) { // Prevent overflow
      if (n->member.id && n->member.tcp_client_fd >= 0 && strcmp(n->member.id, chg_mem->id) != 0) {
        target_fds[target_count++] = n->member.tcp_client_fd;
      }
      n = n->next;
    }
  } else {
    fprintf(stderr, "notify_group_members: Group '%s' not found.\n", g_name);
  }
  pthread_mutex_unlock(&tbl->lock);

  // Enqueue notifications outside the lock
  // printf("Notifying %d members for event %d in group %s\n", target_count, n_type, g_name);
  for (int i = 0; i < target_count; i++) {
    send_notification(q, target_fds[i], n_type, g_name, chg_mem);
  }
}

// --- Failure Handling (Updated Signature and Logic) ---

/*
 * Handles the failure or disconnect of a member connection (identified by FD).
 * Removes ALL member entries associated with the failed FD from ALL groups,
 * notifies remaining group members, and closes the TCP socket.
 */
void handle_member_failure(hash_table_t *table, int failed_fd,
                           const char *failed_ip, // Keep IP for logging/context
                           send_queue_t *queue) {
  if (!table || failed_fd < 0 || !queue) {
    fprintf(stderr, "handle_member_failure: Invalid arguments.\n");
    return;
  }
  printf("Handling failure/disconnect for connection fd=%d (IP: %s)\n", failed_fd,
         failed_ip ? failed_ip : "N/A");

  // List to store info about members removed, for notification
  // Max possible entries = MAX_GROUPS (if client was in all groups)
  member_t removed_members_info[128];              // Store copies
  char removed_from_group[128][MAX_GROUP_LEN + 1]; // Store group names
  int removed_count = 0;

  pthread_mutex_lock(&table->lock); // --- LOCK ---

  // Iterate through all buckets and groups to find and remove matching FDs
  for (size_t i = 0; i < table->size; ++i) {
    group_node_t *group = table->buckets[i];
    group_node_t *prev_group = NULL;

    while (group != NULL) {
      group_node_t *next_group = group->next;
      member_node_t *member_node = group->members_head;
      member_node_t *prev_member_node = NULL;
      int removed_from_this_group_flag = 0; // Track if removal happened in this group

      while (member_node != NULL) {
        // Check if the member's FD matches the failed FD
        if (member_node->member.tcp_client_fd == failed_fd) {
          // --- Found a member associated with the failed FD ---
          printf("Removing member '%s' from group '%s' due to fd %d failure.\n",
                 member_node->member.id ? member_node->member.id : "??", group->group_name,
                 failed_fd);

          // Copy data for notification BEFORE freeing node
          if (removed_count < 128) {                                   // Prevent overflow
            removed_members_info[removed_count] = member_node->member; // Copy struct
            removed_members_info[removed_count].id =
                strdup(member_node->member.id ? member_node->member.id : ""); // strdup ID
            strncpy(removed_from_group[removed_count], group->group_name, MAX_GROUP_LEN);
            removed_from_group[removed_count][MAX_GROUP_LEN] = '\0';
            if (!removed_members_info[removed_count].id) {
              perror("handle_member_failure: strdup failed for notify");
              // Mark as invalid? For now, just proceed.
            }
            removed_count++;
          } else {
            fprintf(stderr,
                    "Warning: Exceeded notification buffer in handle_member_failure "
                    "for fd %d\n",
                    failed_fd);
          }

          // Unlink node
          if (!prev_member_node)
            group->members_head = member_node->next;
          else
            prev_member_node->next = member_node->next;

          member_node_t *node_to_free = member_node;
          member_node = member_node->next;    // Advance iterator BEFORE freeing
          free_member(&node_to_free->member); // Frees the ID inside member_t
          free(node_to_free);                 // Frees the node struct

          group->member_count--;
          removed_from_this_group_flag = 1;
          continue; // Continue checking rest of list in this group
        } // end if(match fd)

        prev_member_node = member_node;
        member_node = member_node->next;
      } // End while member_node

      // Check if group became empty *after* iterating through its members
      if (group->member_count == 0 && removed_from_this_group_flag) {
        printf("Group '%s' empty after failure of fd %d, removing.\n", group->group_name,
               failed_fd);
        if (!prev_group)
          table->buckets[i] = next_group;
        else
          prev_group->next = next_group;
        free_group_node(group);
        table->num_groups--;
        group = NULL;
      }

      // Advance group pointer
      if (group) {
        prev_group = group;
        group = next_group;
      } else {
        group = next_group;
      }
    } // End while group
  } // End for buckets

  pthread_mutex_unlock(&table->lock); // --- UNLOCK ---

  // --- Stage 2: Send Notifications for removed members ---
  // (Do this after unlock to avoid holding lock during enqueue)
  for (int i = 0; i < removed_count; ++i) {
    if (removed_members_info[i].id) { // Check if strdup succeeded
      printf("Queueing failure notification for %s in group %s\n", removed_members_info[i].id,
             removed_from_group[i]);
      print_groups(table);
      notify_group_members(queue, table, removed_from_group[i], &removed_members_info[i],
                           NOTIFY_MEMBER_FAILED);
      free(removed_members_info[i].id); // Free the copied ID
      removed_members_info[i].id = NULL;
    }
  }

  // --- Stage 3: Close the socket ---
  // No conditional check needed anymore, if this function is called, the FD failed.
  if (failed_fd >= 0) {
    printf("Closing socket %d associated with failure (IP: %s)\n", failed_fd,
           failed_ip ? failed_ip : "N/A");
    shutdown(failed_fd, SHUT_RDWR);
    close(failed_fd);
  } else {
    printf("Warning: handle_member_failure called with invalid fd %d\n", failed_fd);
  }

  printf("Finished failure handling for fd=%d\n", failed_fd);
}

// --- Thread Function Implementations ---

void *receiver_thread_func(void *arg) {
  receiver_args_t *r_args = (receiver_args_t *)arg;
  int listen_fd = r_args->listen_fd;
  hash_table_t *group_table = r_args->group_table;
  send_queue_t *sender_queue = r_args->sender_queue;

  struct sockaddr_in client_addr;
  socklen_t client_len = sizeof(client_addr);
  fd_set read_fds;
  struct timeval tv;
  int max_fd = listen_fd;

  printf("Receiver thread started. Waiting (thread-per-client)...\n");

  while (!terminate_server) {
    FD_ZERO(&read_fds);
    FD_SET(listen_fd, &read_fds);
    tv.tv_sec = 1; // 1 second timeout for select
    tv.tv_usec = 0;

    int activity = select(max_fd + 1, &read_fds, NULL, NULL, &tv);

    if (terminate_server) {
      break; // Check flag immediately after select returns or times out
    }

    if (activity < 0) {
      if (errno == EINTR) {
        continue; // Interrupted by signal, loop again
      }
      perror("select receiver");
      break; // Exit on other errors
    }

    if (activity == 0) {
      continue; // Timeout, loop again to check flag and wait
    }

    if (FD_ISSET(listen_fd, &read_fds)) {
      int client_fd = accept(listen_fd, (struct sockaddr *)&client_addr, &client_len);

      if (terminate_server) { // Check flag again after accept
        if (client_fd >= 0) {
          close(client_fd);
        }
        break;
      }
      if (client_fd < 0) {
        // EINTR is possible if signal arrives between select and accept
        if (errno != EINTR && errno != EAGAIN && errno != EWOULDBLOCK) {
          perror("accept failed");
        }
        continue; // Try again
      }

      char ip_str[INET_ADDRSTRLEN];
      inet_ntop(AF_INET, &client_addr.sin_addr, ip_str, sizeof(ip_str));
      printf("Accepted %s:%d fd %d. Launching handler thread...\n", ip_str,
             ntohs(client_addr.sin_port), client_fd);

      // Create args for handler thread
      client_handler_args_t *h_args = malloc(sizeof(client_handler_args_t));
      if (!h_args) {
        perror("malloc handler args");
        close(client_fd);
        continue;
      }
      h_args->client_fd = client_fd;
      h_args->client_addr = client_addr;
      h_args->group_table = group_table;
      h_args->sender_queue = sender_queue;

      // Create and detach thread
      pthread_t tid;
      int create_err = pthread_create(&tid, NULL, handle_client_thread_entry, h_args);
      if (create_err != 0) {
        fprintf(stderr, "pthread_create handler failed: %s\n", strerror(create_err));
        free(h_args);
        close(client_fd);
      } else {
        pthread_detach(tid); // Let thread clean up itself
      }
    } // End if FD_ISSET
  } // End while (!terminate_server)
  printf("Receiver thread terminating.\n");
  return NULL;
}

void *sender_thread_func(void *arg) {
  send_queue_t *queue = (send_queue_t *)arg;
  printf("Sender thread started.\n");
  while (1) {
    send_item_t *item = dequeue_send_item(queue);
    if (!item) {
      if (!queue->active) {
        break;
      } // Queue inactive and empty
      else {
        continue;
      } // Spurious wakeup or timeout
    }

    ssize_t sent = send(item->target_fd, item->buffer, item->len, MSG_NOSIGNAL);
    if (sent < 0) {
      if (errno != EINTR) { // Ignore interrupt errors
        fprintf(stderr, "Sender: send fd %d failed: %s\n", item->target_fd, strerror(errno));
        // Optionally trigger failure mechanism more proactively here?
      }
    } else if ((size_t)sent != item->len) {
      fprintf(stderr, "Sender: Partial send fd %d (%zd/%zu)\n", item->target_fd, sent, item->len);
      // TODO: Handle partial sends? Re-queue remaining bytes?
    }
    free(item); // Free item after processing
  }
  printf("Sender thread terminating.\n");
  return NULL;
}

/*
 * Thread that periodically checks the liveness of connected members.
 * Sends only ONE PING per active TCP connection (FD) per cycle.
 * If a member doesn't respond with PONG within the timeout,
 * identifies the failed FD and calls handle_member_failure for that FD.
 */
void *liveness_thread_func(void *arg) {
  liveness_args_t *l_args = (liveness_args_t *)arg;
  hash_table_t *table = l_args->group_table;
  send_queue_t *queue = l_args->sender_queue;

  // Local structure for tracking timed-out FDs in this cycle
  struct TimedOutFD {
    int fd;
    char ip_address[16]; // Store IP for logging in handler
  };

  struct TimedOutFD *timed_out_fds = NULL;
  int timed_out_count = 0;
  int timed_out_cap = 0;

  // Temporary storage for FDs already pinged in the current cycle
  int *pinged_fds = NULL;
  int pinged_count = 0;
  int pinged_cap = 0;

  printf("Liveness thread started. Interval:%d, Timeout:%d\n", LIVENESS_CHECK_INTERVAL,
         LIVENESS_TIMEOUT);

  while (l_args->active && !terminate_server) {
    struct timespec sleep_rem = {LIVENESS_CHECK_INTERVAL, 0};
    while (nanosleep(&sleep_rem, &sleep_rem) == -1 && errno == EINTR) {
      if (!l_args->active || terminate_server) {
        goto liveness_exit_loop;
      }
    }
    if (!l_args->active || terminate_server) {
      break;
    }

    time_t current_time = time(NULL);
    timed_out_count = 0; // Reset timed-out FD count
    pinged_count = 0;    // Reset pinged FD count

    // --- Stage 1: Iterate, check timeout, ping ONCE per FD ---
    pthread_mutex_lock(&table->lock);

    for (size_t i = 0; i < table->size; ++i) {
      group_node_t *g = table->buckets[i];
      while (g != NULL) {
        member_node_t *m = g->members_head;
        while (m != NULL) {
          int current_client_fd = m->member.tcp_client_fd;
          if (current_client_fd >= 0 && m->member.id != NULL) {

            // Check for timeout
            if (difftime(current_time, m->member.last_pong_time) > LIVENESS_TIMEOUT) {
              // --- Member Timed Out ---
              // Check if this FD is already marked for failure handling
              int fd_already_timed_out = 0;
              for (int k = 0; k < timed_out_count; ++k) {
                if (timed_out_fds[k].fd == current_client_fd) {
                  fd_already_timed_out = 1;
                  break;
                }
              }

              if (!fd_already_timed_out) {
                // Add FD to timed-out list (resize if needed)
                if (timed_out_count >= timed_out_cap) {
                  int new_cap = (timed_out_cap == 0) ? 10 : timed_out_cap * 2;
                  struct TimedOutFD *temp =
                      realloc(timed_out_fds, new_cap * sizeof(struct TimedOutFD));
                  if (!temp) {
                    perror("realloc timed_out_fds failed");
                    goto next_member;
                  }
                  timed_out_fds = temp;
                  timed_out_cap = new_cap;
                }
                // Store FD and IP address string
                timed_out_fds[timed_out_count].fd = current_client_fd;
                if (m->member.udp_addr.sin_family == AF_INET) {
                  if (inet_ntop(AF_INET, &(m->member.udp_addr.sin_addr),
                                timed_out_fds[timed_out_count].ip_address, 100) == NULL) {
                    perror("Liveness: inet_ntop failed for timed out member");
                    strcpy(timed_out_fds[timed_out_count].ip_address,
                           "0.0.0.0"); // Default
                  }
                } else {
                  strcpy(timed_out_fds[timed_out_count].ip_address, "N/A");
                }
                timed_out_count++;
              } // end if !fd_already_timed_out
            } else {
              // --- Member is Alive - Send PING if not already sent to this FD ---
              int already_pinged = 0;
              for (int k = 0; k < pinged_count; ++k) {
                if (pinged_fds[k] == current_client_fd) {
                  already_pinged = 1;
                  break;
                }
              }
              if (!already_pinged) {
                send_ping(queue, current_client_fd);
                // Add FD to pinged list (resize if needed)
                if (pinged_count >= pinged_cap) {
                  int new_pinged_cap = (pinged_cap == 0) ? 20 : pinged_cap * 2;
                  int *temp_fds = realloc(pinged_fds, new_pinged_cap * sizeof(int));
                  if (!temp_fds) {
                    perror("realloc pinged_fds failed");
                  } else {
                    pinged_fds = temp_fds;
                    pinged_cap = new_pinged_cap;
                  }
                }
                if (pinged_count < pinged_cap) {
                  pinged_fds[pinged_count++] = current_client_fd;
                }
              } // end if !already_pinged
            } // end else (member is alive)
          } // end if valid member
        next_member:
          m = m->next;
        } // end member loop
        g = g->next;
      } // end group loop
    } // end bucket loop

    pthread_mutex_unlock(&table->lock);

    // --- Stage 2: Process the collected timed-out FDs ---
    if (timed_out_count > 0 && timed_out_fds) {
      printf("Liveness: Processing %d timed-out connections...\n", timed_out_count);
      for (int i = 0; i < timed_out_count; ++i) {
        // Call failure handler with the FD and associated IP
        printf("Liveness: Handling failure for fd=%d, ip=%s\n", timed_out_fds[i].fd,
               timed_out_fds[i].ip_address);
        handle_member_failure(table, timed_out_fds[i].fd, timed_out_fds[i].ip_address, queue);
      }
    }

    // Free the temporary pinged FDs list for this cycle
    if (pinged_fds) {
      free(pinged_fds);
      pinged_fds = NULL;
      pinged_cap = 0;
    }

  } // End main loop

liveness_exit_loop:
  // Final cleanup of allocated memory
  if (timed_out_fds) {
    free(timed_out_fds);
    timed_out_fds = NULL;
  }
  if (pinged_fds) {
    free(pinged_fds);
    pinged_fds = NULL;
  }
  printf("Liveness thread terminating.\n");
  return NULL;
}

/*
 * Handles communication with a single connected client.
 * Processes JOIN, LEAVE, PONG messages. Calls handle_member_failure on disconnect/error.
 * PONG handler updates all instances associated with the client FD.
 */
void *handle_client_thread_entry(void *arg) {
  client_handler_args_t *args = (client_handler_args_t *)arg;
  int client_fd = args->client_fd;
  struct sockaddr_in client_addr = args->client_addr; // Local copy
  hash_table_t *group_table = args->group_table;
  send_queue_t *sender_queue = args->sender_queue;
  free(args); // Free args struct immediately

  unsigned char buffer[GMS_BUFFER_SIZE];
  ssize_t bytes_received = 0;
  char client_ip_str[INET_ADDRSTRLEN];
  inet_ntop(AF_INET, &client_addr.sin_addr, client_ip_str, INET_ADDRSTRLEN);
  int client_tcp_port = ntohs(client_addr.sin_port);
  char *connection_member_id = NULL; // Still useful for context/logging
  int internal_error_occurred = 0;

  printf("Handler [%lu]: Started for %s:%d fd %d\n", pthread_self(), client_ip_str, client_tcp_port,
         client_fd);

  // Main loop for this client
  while (!terminate_server && !internal_error_occurred) {
    bytes_received = recv(client_fd, buffer, GMS_BUFFER_SIZE, 0);

    if (terminate_server) {
      break;
    }
    if (bytes_received <= 0) {
      break;
    }

    size_t current_offset = 0;
    while (current_offset < (size_t)bytes_received) {
      if (terminate_server || internal_error_occurred) {
        break;
      }
      if (current_offset + 1 > (size_t)bytes_received) {
        break;
      }
      uint8_t msg_type = buffer[current_offset];
      size_t msg_start_offset = current_offset;
      current_offset++;

      // --- JOIN ---
      if (msg_type == MSG_TYPE_JOIN) {
        size_t hdr_len = 1 + 1 + 2;
        if (current_offset + hdr_len > (size_t)bytes_received) {
          current_offset = msg_start_offset;
          break;
        }
        uint8_t id_len = buffer[current_offset++];
        uint8_t group_len = buffer[current_offset++];
        uint16_t udp_port_net;
        memcpy(&udp_port_net, buffer + current_offset, sizeof(uint16_t));
        current_offset += sizeof(uint16_t);
        // We don't store client_udp_port globally anymore for the handler

        if (id_len == 0 || group_len == 0 || id_len > MAX_ID_LEN || group_len > MAX_GROUP_LEN) {
          fprintf(stderr, "H[%lu]: Invalid JOIN lens fd %d\n", pthread_self(), client_fd);
          send_join_response(sender_queue, client_fd, RESP_NACK_INVALID_FORMAT, NULL, NULL, NULL);
          current_offset = bytes_received;
          continue;
        }
        size_t body_len = id_len + group_len;
        if (current_offset + body_len > (size_t)bytes_received) {
          current_offset = msg_start_offset;
          break;
        }
        char id_buf[MAX_ID_LEN + 1];
        char grp_buf[MAX_GROUP_LEN + 1];
        memcpy(id_buf, buffer + current_offset, id_len);
        id_buf[id_len] = '\0';
        current_offset += id_len;
        memcpy(grp_buf, buffer + current_offset, group_len);
        grp_buf[group_len] = '\0';
        current_offset += group_len;

        printf("H[%lu]: JOIN '%s'->'%s' UDP Port:%d\n", pthread_self(), id_buf, grp_buf,
               ntohs(udp_port_net));

        // Update connection_member_id (useful for logging/context)
        if (!connection_member_id) {
          connection_member_id = strdup(id_buf);
          if (!connection_member_id) {
            perror("strdup conn ID failed");
            internal_error_occurred = 1;
            break;
          }
        } else if (strcmp(connection_member_id, id_buf) != 0) {
          fprintf(stderr, "Warn: JOIN ID '%s' != previous conn ID '%s'\n", id_buf,
                  connection_member_id);
          free(connection_member_id);
          connection_member_id = strdup(id_buf);
          if (!connection_member_id) {
            perror("strdup conn ID update failed");
            internal_error_occurred = 1;
            break;
          }
        }

        struct sockaddr_in udp_addr = client_addr;
        udp_addr.sin_port = udp_port_net;
        member_t member = create_member(id_buf, &udp_addr, client_fd);

        if (!member.id) {
          send_join_response(sender_queue, client_fd, RESP_NACK_INTERNAL_ERROR, grp_buf, NULL,
                             NULL);
        } else {
          int add_res = add_to_group(group_table, grp_buf, member);
          send_join_response(sender_queue, client_fd, add_res, grp_buf, group_table, id_buf);
          if (add_res == RESP_ACK_SUCCESS) {
            member_t *added_member_info = NULL;
            member_t notify_copy;
            notify_copy.id = NULL;
            pthread_mutex_lock(&group_table->lock);
            unsigned long idx = hash_function(grp_buf, group_table->size);
            group_node_t *g = group_table->buckets[idx];
            while (g && strcmp(g->group_name, grp_buf) != 0)
              g = g->next;
            if (g) {
              member_node_t *n = find_member_node(g, id_buf);
              if (n)
                added_member_info = &n->member;
            }
            if (added_member_info) {
              notify_copy = *added_member_info;
              notify_copy.id = strdup(added_member_info->id);
            }
            pthread_mutex_unlock(&group_table->lock);
            if (notify_copy.id) {
              notify_group_members(sender_queue, group_table, grp_buf, &notify_copy,
                                   NOTIFY_MEMBER_JOINED);
              free(notify_copy.id);
            } else if (added_member_info) {
              fprintf(stderr, "Error copying member info for JOIN notification (%s)\n", id_buf);
            }
          }
        }
        // --- LEAVE ---
      } else if (msg_type == MSG_TYPE_LEAVE) {
        size_t hdr_len = 1 + 1;
        if (current_offset + hdr_len > (size_t)bytes_received) {
          current_offset = msg_start_offset;
          break;
        }
        uint8_t id_len = buffer[current_offset++];
        uint8_t group_len = buffer[current_offset++];
        if (id_len == 0 || group_len == 0 || id_len > MAX_ID_LEN || group_len > MAX_GROUP_LEN) {
          fprintf(stderr, "H[%lu]: Invalid LEAVE len fd %d\n", pthread_self(), client_fd);
          send_leave_response(sender_queue, client_fd, RESP_NACK_INVALID_FORMAT);
          current_offset = bytes_received;
          break;
        }
        size_t body_len = id_len + group_len;
        if (current_offset + body_len > (size_t)bytes_received) {
          current_offset = msg_start_offset;
          break;
        }
        char id_buf[MAX_ID_LEN + 1];
        char grp_buf[MAX_GROUP_LEN + 1];
        memcpy(id_buf, buffer + current_offset, id_len);
        id_buf[id_len] = '\0';
        current_offset += id_len;
        memcpy(grp_buf, buffer + current_offset, group_len);
        grp_buf[group_len] = '\0';
        current_offset += group_len;

        printf("H[%lu]: LEAVE '%s' from '%s'\n", pthread_self(), id_buf, grp_buf);

        // We still need connection_member_id to verify the LEAVE request source,
        // even if PONG/Failure uses FD primarily.
        // Simply call rmv_from_group and let it handle validation
        int rem_res = rmv_from_group(group_table, grp_buf, id_buf, sender_queue);
        send_leave_response(sender_queue, client_fd, rem_res);
        // Note: A successful leave doesn't terminate the connection handler

        // --- PONG ---
      } else if (msg_type == MSG_TYPE_PONG) {
        // printf("H[%lu]: Received PONG on fd %d \n", // Debug Print
        //       pthread_self(), client_fd);

        // Update last_pong_time for all member entries associated with this FD
        pthread_mutex_lock(&group_table->lock);
        int total_updated = 0;
        for (size_t i = 0; i < group_table->size; ++i) {
          group_node_t *g = group_table->buckets[i];
          while (g) {
            member_node_t *n = g->members_head;
            while (n) {
              if (n->member.tcp_client_fd == client_fd) {
                n->member.last_pong_time = time(NULL);
                total_updated++;
              }
              n = n->next;
            }
            g = g->next;
          }
        }
        pthread_mutex_unlock(&group_table->lock);

        // if (total_updated == 0 && connection_member_id) { // Debug Print
        //      fprintf(stderr, "H[%lu]: PONG received from %s (fd %d) but no member entry
        //      found with matching FD.\n",
        //              pthread_self(), connection_member_id, client_fd);
        // }
        // --- UNKNOWN ---
      } else {
        fprintf(stderr, "H[%lu]: Unknown message type %u from fd %d\n", pthread_self(), msg_type,
                client_fd);
        send_join_response(sender_queue, client_fd, RESP_NACK_INVALID_FORMAT, NULL, NULL, NULL);
        current_offset = bytes_received;
        break;
      }
    } // End while processing buffer

    if (internal_error_occurred) {
      break;
    }
  } // End while recv loop

  // --- Cleanup Logic (after main loop terminates) ---

  printf("Handler [%lu]: Exiting main loop fd %d (ID: %s). Reason code (last recv): %ld\n",
         pthread_self(), client_fd, connection_member_id ? connection_member_id : "N/A",
         bytes_received);

  // Call handle_member_failure using the FD associated with this handler thread.
  // This function will now remove ALL entries for this FD and close the socket.
  // We pass client_ip_str mainly for logging purposes within handle_member_failure.
  if (client_fd >= 0) { // Ensure FD is valid before calling handler
    handle_member_failure(group_table, client_fd, client_ip_str, sender_queue);
  }

  // Free the ID associated with this handler thread
  if (connection_member_id) {
    free(connection_member_id);
    connection_member_id = NULL;
  }
  printf("Handler Thread [%lu]: Finished fd %d.\n", pthread_self(), client_fd);
  return NULL; // Exit thread
}

// --- Main Function ---
int main(int argc, char *argv[]) {
  int port = DEFAULT_PORT;
  if (argc > 1) {
    errno = 0; // Reset errno before strtol
    long p_long = strtol(argv[1], NULL, 10);
    // Check for conversion errors and range
    if (errno != 0 || p_long <= 0 || p_long > 65535) {
      fprintf(stderr, "Invalid port: %s. Using default %d.\n", argv[1], DEFAULT_PORT);
    } else {
      port = (int)p_long;
    }
  }

  // Initialize server socket
  int listen_fd = socket(AF_INET, SOCK_STREAM, 0);
  if (listen_fd < 0) {
    error_exit("socket creation failed");
  }

  int opt = 1;
  if (setsockopt(listen_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt)) < 0) {
    perror("Warning: setsockopt(SO_REUSEADDR) failed");
  }

  struct sockaddr_in server_addr;
  memset(&server_addr, 0, sizeof(server_addr));
  server_addr.sin_family = AF_INET;
  server_addr.sin_addr.s_addr = htonl(INADDR_ANY);
  server_addr.sin_port = htons(port);

  if (bind(listen_fd, (struct sockaddr *)&server_addr, sizeof(server_addr)) < 0) {
    close(listen_fd); // Close socket if bind fails
    error_exit("bind failed");
  }

  if (listen(listen_fd, MAX_PENDING_CONNECTIONS) < 0) {
    close(listen_fd);
    error_exit("listen failed");
  }
  print_sock_info(listen_fd);

  // Initialize shared data structures
  hash_table_t *group_table = create_hash_table(INITIAL_HASH_TABLE_SIZE);
  send_queue_t *sender_queue = create_send_queue();

  // Prepare thread arguments
  receiver_args_t r_args = {listen_fd, group_table, sender_queue};
  liveness_args_t l_args = {group_table, sender_queue, 1};

  // Create persistent threads
  pthread_t receiver_tid, sender_tid, liveness_tid;
  if (pthread_create(&sender_tid, NULL, sender_thread_func, sender_queue) != 0) {
    // Partial cleanup might be needed if some threads started
    error_exit("Failed to create sender thread");
  }
  if (pthread_create(&liveness_tid, NULL, liveness_thread_func, &l_args) != 0) {
    // Signal sender to stop? Join sender?
    error_exit("Failed to create liveness thread");
  }
  if (pthread_create(&receiver_tid, NULL, receiver_thread_func, &r_args) != 0) {
    // Signal sender/liveness to stop? Join them?
    error_exit("Failed to create receiver thread");
  }
  printf("GMS Server started (Thread-Per-Client Model).\n");

  // Main thread waits for receiver thread to terminate (upon shutdown signal)
  pthread_join(receiver_tid, NULL);
  printf("Receiver thread joined.\n");

  // Initiate shutdown sequence for other persistent threads
  printf("Initiating shutdown...\n");

  // 1. Signal Liveness thread to stop
  l_args.active = 0;
  printf("Signalled liveness thread to stop.\n");

  // 2. Signal Sender thread to stop and wake it up
  printf("Signalling sender thread to stop...\n");
  pthread_mutex_lock(&sender_queue->lock);
  sender_queue->active = 0;
  pthread_cond_broadcast(&sender_queue->cond); // Wake up if waiting
  pthread_mutex_unlock(&sender_queue->lock);

  // 3. Wait for persistent threads to finish
  printf("Waiting for liveness thread...\n");
  pthread_join(liveness_tid, NULL);
  printf("Liveness thread joined.\n");

  printf("Waiting for sender thread...\n");
  pthread_join(sender_tid, NULL);
  printf("Sender thread joined.\n");

  printf("All main threads joined.\n");

  // Final Cleanup
  printf("Final cleanup...\n");
  close(listen_fd); // Close listening socket
  destroy_send_queue(sender_queue);
  free_hash_table(group_table);
  printf("GMS Server shutdown complete.\n");
  return EXIT_SUCCESS;
}