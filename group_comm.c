#include <arpa/inet.h>
#include <ctype.h>
#include <endian.h> // For htobe64 / be64toh
#include <errno.h>  // Για error codes (π.χ., EWOULDBLOCK, ETIMEDOUT)
#include <limits.h>
#include <netinet/in.h>
#include <pthread.h> // Για threads, mutex, cond var
#include <semaphore.h>
#include <stdatomic.h>
#include <stdint.h> // uint64_t etc.
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/time.h> // Για gettimeofday() ή clock_gettime()
#include <time.h>     // Για time()
#include <unistd.h>   // Για close(), sleep()

// --- Ορισμός Δομών ---

#define MAX_MEMBERS 5
#define MAX_GROUPS 5
#define MAX_ID_LEN 128
#define MAX_GROUP_LEN 128
#define MAX_ADDR_LEN 16
#define GMS_BUFFER_SIZE 2048        // Μέγεθος buffer για λήψη από GMS
#define GMS_RESPONSE_QUEUE_SIZE 100 // Μέγεθος ουράς για απαντήσεις GMS
#define UDP_BUFFER_SIZE 1024        // Max UDP payload size
#define GROUP_MSG_QUEUE_SIZE 100    // Μέγεθος ουράς UDP μηνυμάτων *ανά group*
#define PENDING_ACKS_SIZE 100       // Μέγεθος buffer για seq nums που περιμένουν ACK (ανά group)
#define INITIAL_MEMBER_LIST_CAPACITY 5 // Αρχική χωρητικότητα για λίστες ID σε PendingAckInfo
#define RETRANSMISSION_TIMEOUT 2       // Seconds to wait for ACKs before retransmitting
#define RETRANSMISSION_CHECK_INTERVAL 1 // Seconds between retransmission checks
#define OUT_OF_ORDER_BUFFER_SIZE 200    // Max OOO μηνύματα ανά αποστολέα ανά group
#define UNSTABLE_BUFFER_SIZE 100        // Buffer for unstable data waiting for global seq no
#define VC_PENDING_ACKS_SIZE 100        // Max pending ACKs for VC control messages per group
#define MAX_CONTROL_MSG_RETRIES 50      // Max retries for control messages
#define LOSS_PROBABILITY 0
#define MICRO_DELAY 10000

// --- Τύποι Μηνυμάτων Πρωτοκόλλου UDP Group ---
#define TYPE_ACK 101            // Επιβεβαίωση λήψης (ACK)
#define TYPE_DATA_UNSTABLE 110  // Unstable data multicast by sender
#define TYPE_SEQ_REQUEST 111    // Small request sent to sequencer
#define TYPE_SEQ_ASSIGNMENT 112 // Small assignment multicast by sequencer
#define TYPE_STATE_REQUEST 113  // Request state from sequencer (sent by new member)
#define TYPE_STATE_RESPONSE 114 // State response from sequencer (sent to new member)
// -- ΤΥΠΟΙ ΓΙΑ VIEW SYNCHRONICITY ---
#define TYPE_VIEW_CHANGE_START 120   // Initiate flush protocol (contains failed member ID)
#define TYPE_VIEW_CHANGE_STATE 121   // Member sends its state (e.g., expected_global_seq)
#define TYPE_VIEW_CHANGE_CONFIRM 122 // Coordinator confirms agreed state (e.g., agreed_delivery_gseq)dd
#define TYPE_VIEW_CHANGE_ACK 123     // ACK for START, STATE, or CONFIRM messages
#define TYPE_VIEW_CHANGE_RESTART 124 // ΤΥΠΟΣ
// (Προαιρετικά για ανταλλαγή μηνυμάτων κατά το flush)
// #define TYPE_FLUSH_RETRANSMIT_REQUEST 123
// #define TYPE_FLUSH_RETRANSMIT_DATA    124
// -----------------------------------------
// Χρησιμοποιούμε τους ίδιους αριθμούς για ευκολία
#define ACKED_TYPE_DATA_UNSTABLE TYPE_DATA_UNSTABLE   // π.χ., 110
#define ACKED_TYPE_SEQ_ASSIGNMENT TYPE_SEQ_ASSIGNMENT // π.χ., 112

// --- Τύποι για τα στοιχεία στην ουρά εισερχομένων του group ---
#define MSG_BUFFER_TYPE_DATA 0x00   // Κανονικό μήνυμα δεδομένων από άλλο μέλος
#define MSG_BUFFER_TYPE_NOTIFY 0x01 // Ειδοποίηση συστήματος (Join/Left/Fail)

// --- Υποτύποι για τα Notifications που μπαίνουν στην ουρά ---
// (Χρησιμοποιούμε τους ίδιους κωδικούς με του GMS για ευκολία)
#define NOTIFY_SUBTYPE_JOINED NOTIFY_MEMBER_JOINED
#define NOTIFY_SUBTYPE_LEFT NOTIFY_MEMBER_LEFT
#define NOTIFY_SUBTYPE_FAILED NOTIFY_MEMBER_FAILED

#define UNASSIGNED_GLOBAL_SEQ UINT64_MAX // Special value for unassigned global sequence number
#define VC_STATE_SENTINEL UINT64_MAX     // Sentinel value for unreceived states

#define PENDING_SEQ_ASSIGNMENTS_SIZE 5000 // Size for pending SEQ_REQUESTs per group
#define SEQ_REQUEST_TIMEOUT 3             // Seconds to wait for SEQ_ASSIGNMENT before retrying SEQ_REQUEST
#define MAX_SEQ_REQUEST_RETRIES 50        // Max retries for SEQ_REQUEST before potentially declaring sequencer failed

#ifndef ANSI_COLOR_GREEN // Ορισμός αν δεν υπάρχει ήδη
#define ANSI_COLOR_GREEN   "\x1b[32m" // Start Green Text
#endif
#ifndef ANSI_COLOR_RESET // Ορισμός αν δεν υπάρχει ήδη
#define ANSI_COLOR_RESET   "\x1b[0m"  // Reset to Default Color
#endif


typedef struct {
  int active;                          // 1 = waiting for SEQ_ASSIGNMENT, 0 = received or cancelled
  uint32_t original_local_seq_no;      // The local sequence number of the DATA_UNSTABLE message
  char original_sender_id[MAX_ID_LEN]; // The ID of the node that sent the DATA_UNSTABLE
  time_t request_sent_time;            // Time the *last* SEQ_REQUEST was sent
  int retry_count;                     // How many times SEQ_REQUEST has been resent
  // Store enough info to resend the SEQ_REQUEST
  char group_name[MAX_GROUP_LEN];
  // We don't need to store the request body itself, as it's small and fixed format
} PendingSeqAssignmentInfo;

// --- Δομή για Out-of-Order μηνύματα ---
typedef struct {
  unsigned char data[UDP_BUFFER_SIZE];    // Payload
  int len;                                // Μήκος payload
  struct sockaddr_in sender_addr;         // Αποστολέας
  uint32_t sequence_number;               // Ο seq no αυτού του μηνύματος
  char sender_id[MAX_ID_LEN];             // ID αποστολέα
  int received_vector_clock[MAX_MEMBERS]; // Store the VC it came with
  int active;                             // 1 αν η θέση χρησιμοποιείται, 0 αλλιώς
  uint8_t original_msg_type;              // Store original type (REQUEST or DELIVER)
} OutOfOrderMsg;
// -----------------------------------------

typedef struct {
  char member_id[MAX_ID_LEN];
  char address[MAX_ADDR_LEN];
  int port;
  uint32_t expected_recv_seq; // Επόμενο seq no που περιμένουμε από αυτόν
  OutOfOrderMsg ooo_buffer[OUT_OF_ORDER_BUFFER_SIZE];
  int ooo_count; // Πόσα μηνύματα είναι τώρα στον buffer
} member_t;

// ΕΝΗΜΕΡΩΜΕΝΗ Δομή για Εισερχόμενα Στοιχεία στην Ουρά του Group
typedef struct {
  uint8_t internal_type;               // MSG_BUFFER_TYPE_DATA ή MSG_BUFFER_TYPE_NOTIFY
  unsigned char data[UDP_BUFFER_SIZE]; // Payload (είτε από UDP είτε φτιαγμένο notification)
  int len;
  struct sockaddr_in sender_addr;         // Ποιος έστειλε το DATA (αγνοείται για NOTIFY)
  uint32_t sequence_number;               // Ο seq no του μηνύματος DATA (αγνοείται για NOTIFY)
  char sender_id[MAX_ID_LEN];             // Το ID του αποστολέα DATA (αγνοείται για NOTIFY)
  int received_vector_clock[MAX_MEMBERS]; // Αποθηκεύουμε το VC του αποστολέα
  uint64_t global_sequence_number;        // Global sequence number (if applicable)
} GroupQueueItem;                         // Μετονομασία από GroupUDPMessage

// --- ΝΕΑ Δομή για Unstable Data Messages ---
typedef struct {
  unsigned char data[UDP_BUFFER_SIZE]; // Payload
  int len;
  struct sockaddr_in sender_addr;         // Original sender address
  uint32_t sequence_number;               // Original sender's Local seq no
  char sender_id[MAX_ID_LEN];             // Original sender's ID
  int received_vector_clock[MAX_MEMBERS]; // VC from original sender
  uint64_t global_sequence_number;        // Assigned Global sequence number (UINT64_MAX if not assigned yet)
  int active;                             // Is this slot used?
  int data_missing;
} UnstableDataMsg;
// -----------------------------------------

// --- ΕΝΗΜΕΡΩΜΕΝΗ Δομή για Παρακολούθηση Pending ACKs (Per-Member) ---
typedef struct {
  uint32_t seq_no;         // Ο αριθμός του μηνύματος που στείλαμε
  int active;              // 1 αν περιμένει ACKs, 0 αν ΟΚ ή άκυρο
  time_t sent_time;        // Πότε στάλθηκε (για retransmissions)
  int target_member_count; // Πόσα μέλη ήταν ο στόχος αρχικά
  int acks_received_count; // Πόσα ACKs έχουμε λάβει γι' αυτό
  char **target_member_ids; // Λίστα με τα IDs των μελών-στόχων (δυναμικός πίνακας από strings)
  int target_member_cap; // Χωρητικότητα του παραπάνω πίνακα
  char **acked_member_ids; // Λίστα με τα IDs όσων έστειλαν ACK (δυναμικός πίνακας από strings)
  int acked_member_cap; // Χωρητικότητα του παραπάνω πίνακα
  unsigned char *message_data; // Αντίγραφο του πλήρους μηνύματος (header+payload) που στάλθηκε
  size_t message_len;        // Μήκος του message_data
  uint8_t original_msg_type; // Type of message sent (DATA_UNSTABLE or SEQ_ASSIGNMENT)
} PendingAckInfo;
// --------------------------------------------------------------------

// --- ΝΕΑ Δομή για Pending ACKs των VC Control Messages ---
typedef struct {
  int active;                        // 1 = περιμένουμε ACK, 0 = ήρθε ή ακυρώθηκε
  uint8_t message_type_sent;         // Τι τύπος μηνύματος στάλθηκε (START, STATE, CONFIRM)
  char target_member_id[MAX_ID_LEN]; // Σε ποιον το στείλαμε (από ποιον περιμένουμε ACK)
  time_t sent_time;                  // Πότε στάλθηκε η τελευταία φορά
  int retry_count;                   // Πόσες φορές το ξαναστείλαμε
  unsigned char message_data[UDP_BUFFER_SIZE]; // Αντίγραφο του μηνύματος που στείλαμε
  size_t message_len;                          // Μήκος του message_data
                                               // Maybe add view change instance ID later if needed for correlation
} PendingControlAck;
// -------------------------------------------------------

typedef struct {
  // ... existing fields ...
  char group_name[MAX_GROUP_LEN];
  member_t members[MAX_MEMBERS];
  int member_count;
  int is_active;
  char my_id_in_group[MAX_ID_LEN];
  uint32_t next_send_seq;
  int vector_clock[MAX_MEMBERS];

  uint64_t next_global_seq;
  uint64_t expected_global_seq;
  UnstableDataMsg unstable_data_buffer[UNSTABLE_BUFFER_SIZE];
  int unstable_count;
  pthread_mutex_t unstable_mutex;

  PendingAckInfo pending_acks[PENDING_ACKS_SIZE];
  pthread_mutex_t pending_ack_mutex;

  GroupQueueItem message_buffer[GROUP_MSG_QUEUE_SIZE];
  int msg_head;
  int msg_tail;
  int msg_count;
  pthread_mutex_t msg_mutex;
  pthread_cond_t msg_cond;

  volatile int state_transfer_complete;
  pthread_mutex_t state_transfer_mutex;
  pthread_cond_t state_transfer_cond;

  // --- MODIFIED/NEW View Synchronicity Fields ---
  volatile int view_change_active; // Flag: 0=no, 1=yes
  int view_change_stage;           // e.g., 0=idle, 1=started/restarted, 2=state_collected,
                                   // 3=confirmed/flushing
  uint64_t agreed_delivery_gseq;   // Highest global seq agreed for current view
  int sync_acks_needed;            // Coordinator: How many STATE messages needed
  int sync_acks_received;          // Coordinator: How many STATE messages received
  pthread_mutex_t vc_state_mutex;  // Mutex for all VC state variables below

  // List of members confirmed failed *during this VC instance*
  char vc_failed_member_list[MAX_MEMBERS][MAX_ID_LEN];
  int vc_failed_member_count;

  // State collection fields (used by coordinator)
  uint64_t vc_received_states[MAX_MEMBERS]; // Store received expected_global_seq (indexed by
                                            // member index in *current* list)
  int vc_state_reported[MAX_MEMBERS];       // Flag per member index (0 or 1)

  // Reliability for VC control messages
  PendingControlAck vc_pending_acks[VC_PENDING_ACKS_SIZE];
  pthread_mutex_t vc_pending_ack_mutex;
  // --- End View Synchronicity Fields ---

  // --- State for Reliable SEQ_REQUEST ---
  PendingSeqAssignmentInfo pending_seq_assignments[PENDING_SEQ_ASSIGNMENTS_SIZE];
  pthread_mutex_t pending_seq_assignment_mutex;

} group_t;

// --- Σταθερές Πρωτοκόλλου --- (ίδιες με πριν)
#define MSG_TYPE_JOIN 0
#define MSG_TYPE_LEAVE 1
#define MSG_TYPE_PONG 2
#define RESP_ACK_SUCCESS 0
#define RESP_NACK_DUPLICATE_ID 1
#define RESP_NACK_GROUP_FULL 2
#define RESP_NACK_INTERNAL_ERROR 3
#define RESP_NACK_INVALID_FORMAT 4
#define RESP_NACK_NOT_IN_GROUP 5
#define NOTIFY_MEMBER_JOINED 10
#define NOTIFY_MEMBER_LEFT 11
#define NOTIFY_MEMBER_FAILED 12
#define MSG_TYPE_PING 20

// --- Δομή για την Ουρά Απαντήσεων GMS ---
typedef struct {
  unsigned char data[GMS_BUFFER_SIZE];
  int len;
} GmsResponseMessage;

typedef struct {
  GmsResponseMessage messages[GMS_RESPONSE_QUEUE_SIZE];
  int head;
  int tail;
  int count;
  pthread_mutex_t mutex;
} GmsResponseQueue;

// --- Global Μεταβλητές --- (προσθήκη ουράς)

static group_t global_groups[MAX_GROUPS];
static int gms_tcp_socket = -1;
static int global_udp_socket = -1;
static struct sockaddr_in my_udp_addr;
static int library_initialized = 0;

// Mutex/Cond Var για συγχρονισμό join/leave με τον handler
static pthread_mutex_t group_data_mutex = PTHREAD_MUTEX_INITIALIZER; // Προστατεύει global_groups

// Ουρά για απαντήσεις GMS (προς join/leave)
static GmsResponseQueue gms_queue;

// Στοιχεία του TCP Handler Thread
static pthread_t tcp_handler_thread_id;
static volatile int keep_tcp_handler_running = 0;

sem_t join_mtx, leave_mtx;

// Πίνακας για αποθήκευση ενεργών handles
int active_handles[MAX_GROUPS];
int active_handle_count = 0;

// Νέα globals για UDP receiver και ουρά
static pthread_t udp_receiver_thread_id;           // Handle για το νέο thread
static volatile int keep_udp_receiver_running = 0; // Flag για το νέο thread

static pthread_t retransmission_thread_id;                  // <-- Handle
static volatile int keep_retransmission_thread_running = 0; // <-- Flag

// --- Instrumentation Counters ---
static atomic_ullong app_messages_sent_count = 0;         // Count successful grp_send calls initiated
static atomic_ullong app_messages_recvd_count = 0;        // Count DATA messages delivered by grp_recv
static atomic_ullong proto_data_unstable_sent_count = 0;  // Count TYPE_DATA_UNSTABLE UDP sends
static atomic_ullong proto_seq_request_sent_count = 0;    // Count TYPE_SEQ_REQUEST UDP sends
static atomic_ullong proto_seq_assignment_sent_count = 0; // Count TYPE_SEQ_ASSIGNMENT UDP sends
static atomic_ullong proto_ack_sent_count = 0;            // Count TYPE_ACK UDP sends (for DATA and SEQ_ASSIGN)
static atomic_ullong proto_state_request_sent_count = 0;  // Count TYPE_STATE_REQUEST UDP sends (in grp_join)
static atomic_ullong proto_state_response_sent_count = 0; // Count TYPE_STATE_RESPONSE UDP sends
static atomic_ullong proto_vc_start_sent_count = 0;       // Count TYPE_VIEW_CHANGE_START UDP sends
static atomic_ullong proto_vc_state_sent_count = 0;       // Count TYPE_VIEW_CHANGE_STATE UDP sends
static atomic_ullong proto_vc_confirm_sent_count = 0;     // Count TYPE_VIEW_CHANGE_CONFIRM UDP sends
static atomic_ullong proto_vc_restart_sent_count = 0;     // Count TYPE_VIEW_CHANGE_RESTART UDP sends
static atomic_ullong proto_vc_ack_sent_count = 0;         // Count TYPE_VIEW_CHANGE_ACK UDP sends

// --- Βοηθητικές Συναρτήσεις Ουράς ---

static char *get_sequencer_id(group_t *group);
static void clear_pending_ack_entry(PendingAckInfo *entry);
static void track_control_ack(group_t *group, const char *target_id, uint8_t msg_type, const unsigned char *msg_data,
                              size_t msg_len);
static void send_view_change_confirm(group_t *group, uint64_t agreed_gseq);

// Printing counters for testing only in ONE group (and also that should be the first group to join)
void print_instrumentation_counters() {
  char member_id[MAX_ID_LEN];
  snprintf(member_id, MAX_ID_LEN, "%s", global_groups[0].my_id_in_group);
  printf(ANSI_COLOR_GREEN "--- Instrumentation Counters for [%s] ---" ANSI_COLOR_RESET "\n", member_id);
  printf(ANSI_COLOR_GREEN "  App Messages Sent:      %llu [%s]" ANSI_COLOR_RESET "\n", atomic_load(&app_messages_sent_count), member_id);
  printf(ANSI_COLOR_GREEN "  App Messages Recvd:     %llu [%s]" ANSI_COLOR_RESET "\n", atomic_load(&app_messages_recvd_count), member_id);
  printf(ANSI_COLOR_GREEN "  Proto DATA_UNSTABLE:    %llu [%s]" ANSI_COLOR_RESET "\n", atomic_load(&proto_data_unstable_sent_count), member_id);
  printf(ANSI_COLOR_GREEN "  Proto SEQ_REQUEST:      %llu [%s]" ANSI_COLOR_RESET "\n", atomic_load(&proto_seq_request_sent_count), member_id);
  printf(ANSI_COLOR_GREEN "  Proto SEQ_ASSIGNMENT:   %llu [%s]" ANSI_COLOR_RESET "\n", atomic_load(&proto_seq_assignment_sent_count), member_id);
  printf(ANSI_COLOR_GREEN "  Proto ACK (Data/Seq):   %llu [%s]" ANSI_COLOR_RESET "\n", atomic_load(&proto_ack_sent_count), member_id);
  printf(ANSI_COLOR_GREEN "  Proto STATE_REQUEST:    %llu [%s]" ANSI_COLOR_RESET "\n", atomic_load(&proto_state_request_sent_count), member_id);
  printf(ANSI_COLOR_GREEN "  Proto STATE_RESPONSE:   %llu [%s]" ANSI_COLOR_RESET "\n", atomic_load(&proto_state_response_sent_count), member_id);
  printf(ANSI_COLOR_GREEN "  Proto VC_START:         %llu [%s]" ANSI_COLOR_RESET "\n", atomic_load(&proto_vc_start_sent_count), member_id);
  printf(ANSI_COLOR_GREEN "  Proto VC_STATE:         %llu [%s]" ANSI_COLOR_RESET "\n", atomic_load(&proto_vc_state_sent_count), member_id);
  printf(ANSI_COLOR_GREEN "  Proto VC_CONFIRM:       %llu [%s]" ANSI_COLOR_RESET "\n", atomic_load(&proto_vc_confirm_sent_count), member_id);
  printf(ANSI_COLOR_GREEN "  Proto VC_RESTART:       %llu [%s]" ANSI_COLOR_RESET "\n", atomic_load(&proto_vc_restart_sent_count), member_id);
  printf(ANSI_COLOR_GREEN "  Proto VC_ACK:           %llu [%s]" ANSI_COLOR_RESET "\n", atomic_load(&proto_vc_ack_sent_count), member_id);
  printf(ANSI_COLOR_GREEN "-----------------------------------------" ANSI_COLOR_RESET "\n");
  fflush(stdout);
}

/*
 * Sends the TYPE_VIEW_CHANGE_RESTART message reliably (called by new coordinator C')
 * to all other members remaining after *all* known failures in the VC.
 * Message Format: Type(1) + GroupLen(1) + GroupName + SenderLen(1) + SenderID(C')
 * + FailedCount(1) + [ FailedID_i_Len(1) + FailedID_i ] * FailedCount
 */
static void send_view_change_restart_generalized(group_t *group) {
  if (!group)
    return;

  char group_name_copy[MAX_GROUP_LEN + 1];
  char my_id_copy[MAX_ID_LEN + 1]; // C's ID
  char current_failed_list[MAX_MEMBERS][MAX_ID_LEN];
  int current_failed_count = 0;
  member_t remaining_members_info[MAX_MEMBERS]; // Store info of OTHERS remaining
  int remaining_count = 0;
  unsigned char *restart_msg_buf = NULL;
  size_t restart_msg_len = 0;

  // --- 1. Get current VC state (failed list) ---
  pthread_mutex_lock(&group->vc_state_mutex);
  if (!group->view_change_active) { // Should be active if restart is called
    pthread_mutex_unlock(&group->vc_state_mutex);
    fprintf(stderr, "SendVCRestartGen Error: Called but VC not active.\n");
    return;
  }
  // Copy the list of members known to have failed *so far* in this VC
  current_failed_count = group->vc_failed_member_count;
  printf("SendVCRestartGen: Restarting VC for group '%s'. Current failed count: %d\n", group->group_name,
         current_failed_count);
  for (int i = 0; i < current_failed_count; ++i) {
    strncpy(current_failed_list[i], group->vc_failed_member_list[i], MAX_ID_LEN);
    current_failed_list[i][MAX_ID_LEN - 1] = '\0';
    printf("  - Known failed: %s\n", current_failed_list[i]);
  }
  // Reset sync counters for the new round coordinated by C'
  group->sync_acks_received = 0;
  group->sync_acks_needed = 0; // Will be set below based on remaining_count
  // Reset state tracking arrays
  for (int k = 0; k < MAX_MEMBERS; ++k) {
    group->vc_received_states[k] = VC_STATE_SENTINEL;
    group->vc_state_reported[k] = 0;
  }
  pthread_mutex_unlock(&group->vc_state_mutex);

  // --- 2. Get necessary info & Find remaining members (excluding ALL failed and self) ---
  pthread_mutex_lock(&group_data_mutex);
  if (!group->is_active) {
    pthread_mutex_unlock(&group_data_mutex);
    return;
  } // Handle inactive group
  strncpy(group_name_copy, group->group_name, MAX_GROUP_LEN);
  group_name_copy[MAX_GROUP_LEN] = '\0';
  strncpy(my_id_copy, group->my_id_in_group, MAX_ID_LEN);
  my_id_copy[MAX_ID_LEN] = '\0';

  printf("SendVCRestartGen: Identifying remaining members to send RESTART (Excluding %d failed):\n",
         current_failed_count);
  remaining_count = 0; // Reset count for this check
  for (int i = 0; i < group->member_count; i++) {
    int is_failed = 0;
    for (int f = 0; f < current_failed_count; ++f) {
      if (current_failed_list[f][0] != '\0' && strcmp(group->members[i].member_id, current_failed_list[f]) == 0) {
        is_failed = 1;
        break;
      }
    }
    int is_self = (strcmp(group->members[i].member_id, my_id_copy) == 0);

    if (!is_failed && !is_self) {
      if (remaining_count < MAX_MEMBERS) {
        memcpy(&remaining_members_info[remaining_count], &group->members[i], sizeof(member_t));
        printf("  - Adding remaining member to send list: %s\n", group->members[i].member_id);
        remaining_count++;
      } else {
        fprintf(stderr, "SendVCRestartGen Warning: Exceeded MAX_MEMBERS while collecting "
                        "remaining members.\n");
      }
    }
  }
  pthread_mutex_unlock(&group_data_mutex);

  // Update sync_acks_needed for the new coordinator (C')
  pthread_mutex_lock(&group->vc_state_mutex);
  group->sync_acks_needed = remaining_count;
  printf("SendVCRestartGen: Set sync_acks_needed to %d for coordinator '%s'.\n", remaining_count, my_id_copy);
  pthread_mutex_unlock(&group->vc_state_mutex);

  // --- 3. Build TYPE_VIEW_CHANGE_RESTART message ---
  uint8_t msg_type = TYPE_VIEW_CHANGE_RESTART;
  uint8_t gn_len = strlen(group_name_copy);
  uint8_t sender_len = strlen(my_id_copy);            // C''s ID
  uint8_t num_failed = (uint8_t)current_failed_count; // Cast might truncate if > 255 failures

  // Calculate message length
  restart_msg_len = 1 + 1 + gn_len + 1 + sender_len + 1; // Up to FailedCount
  for (int i = 0; i < current_failed_count; ++i) {
    if (current_failed_list[i][0] == '\0')
      continue;                                            // Skip empty slots if any
    restart_msg_len += 1 + strlen(current_failed_list[i]); // Len + ID
  }

  if (restart_msg_len < UDP_BUFFER_SIZE) {
    restart_msg_buf = malloc(restart_msg_len);
    if (restart_msg_buf) {
      unsigned char *ptr = restart_msg_buf;
      // Serialize Type, Group, Sender(C')
      memcpy(ptr, &msg_type, 1);
      ptr++;
      memcpy(ptr, &gn_len, 1);
      ptr++;
      memcpy(ptr, group_name_copy, gn_len);
      ptr += gn_len;
      memcpy(ptr, &sender_len, 1);
      ptr++;
      memcpy(ptr, my_id_copy, sender_len);
      ptr += sender_len;
      // Serialize FailedCount
      memcpy(ptr, &num_failed, 1);
      ptr++;
      // Serialize each Failed ID (Len + ID)
      for (int i = 0; i < current_failed_count; ++i) {
        if (current_failed_list[i][0] == '\0')
          continue; // Skip empty slots if any
        uint8_t failed_id_len = strlen(current_failed_list[i]);
        memcpy(ptr, &failed_id_len, 1);
        ptr++;
        memcpy(ptr, current_failed_list[i], failed_id_len);
        ptr += failed_id_len;
      }
    } else {
      perror("SendVCRestartGen: malloc failed");
      return;
    } // Cannot proceed
  } else {
    fprintf(stderr, "SendVCRestartGen Error: RESTART message too large!\n");
    return;
  } // Cannot proceed

  // --- 4. Send the message reliably to other remaining members ---
  if (restart_msg_buf && remaining_count > 0) {
    printf("SendVCRestartGen: Sending TYPE_VIEW_CHANGE_RESTART reliably to %d other members...\n", remaining_count);
    int errors = 0;
    for (int i = 0; i < remaining_count; i++) {
      member_t *target_member = &remaining_members_info[i];
      if (target_member->address[0] == '\0' || target_member->port <= 0)
        continue;
      struct sockaddr_in dest_addr;
      memset(&dest_addr, 0, sizeof(dest_addr));
      dest_addr.sin_family = AF_INET;
      dest_addr.sin_port = htons(target_member->port);
      if (inet_pton(AF_INET, target_member->address, &dest_addr.sin_addr) <= 0)
        continue;

      ssize_t sent_bytes = sendto(global_udp_socket, restart_msg_buf, restart_msg_len, 0, (struct sockaddr *)&dest_addr,
                                  sizeof(struct sockaddr_in));
      if (sent_bytes < 0) {
        char addr_str[INET_ADDRSTRLEN];
        inet_ntop(AF_INET, &dest_addr.sin_addr, addr_str, sizeof(addr_str));
        fprintf(stderr, "SendVCRestartGen: sendto failed for member %s at %s:%d - %s\n", target_member->member_id,
                addr_str, ntohs(dest_addr.sin_port), strerror(errno));
        errors++;
      } else {
        // Track ACK for the RESTART message
        track_control_ack(group, target_member->member_id, TYPE_VIEW_CHANGE_RESTART, restart_msg_buf, restart_msg_len);
      }
    }
    if (errors > 0) {
      fprintf(stderr, "SendVCRestartGen Warning: Failed initial send of RESTART message to %d members.\n", errors);
    }
  } else if (remaining_count == 0) {
    // Coordinator is the only one left after multiple failures.
    printf("SendVCRestartGen: No other remaining members. Proceeding to confirm/finalize "
           "immediately.\n");
    pthread_mutex_lock(&group->vc_state_mutex);
    if (group->view_change_active) {         // Check if still active
      pthread_mutex_lock(&group_data_mutex); // Need own expected GSeq
      group->agreed_delivery_gseq = group->expected_global_seq;
      pthread_mutex_unlock(&group_data_mutex);
      group->view_change_stage = 3; // Move to confirmed stage
      printf("  -> Only member left. Setting agreed GSeq to %lu and proceeding to confirm.\n",
             (unsigned long)group->agreed_delivery_gseq);
      uint64_t agreed_gseq_copy = group->agreed_delivery_gseq;
      pthread_mutex_unlock(&group->vc_state_mutex);
      // Call confirm (will find no one else to send to, then call process_stable)
      send_view_change_confirm(group, agreed_gseq_copy);
    } else {
      pthread_mutex_unlock(&group->vc_state_mutex);
    }
  }

  // Cleanup
  if (restart_msg_buf) {
    free(restart_msg_buf);
  }
  printf("SendVCRestartGen: Finished sending restart messages for group '%s'.\n", group->group_name);
}

/*
 * Finds the ID of the sequencer/coordinator for the group, excluding specific IDs.
 * Rule: Member with the lexicographically smallest ID among the *survivors*.
 * Survivors are those in group->members NOT present in exclude_list.
 * WARNING: Must be called with group_data_mutex HELD!
 * Returns a strdup'd copy of the ID (must be freed) or NULL.
 */
static char *get_sequencer_id_excluding_list(group_t *group, const char exclude_list[][MAX_ID_LEN], int exclude_count) {
  if (!group || !group->is_active || group->member_count == 0) {
    return NULL;
  }
  char *current_sequencer_id = NULL;
  int found_first = 0;

  for (int i = 0; i < group->member_count; ++i) {
    if (group->members[i].member_id[0] == '\0')
      continue; // Skip empty slots

    // Check if this member is in the exclude list
    int is_excluded = 0;
    for (int ex = 0; ex < exclude_count; ++ex) {
      if (exclude_list[ex][0] != '\0' && strcmp(group->members[i].member_id, exclude_list[ex]) == 0) {
        is_excluded = 1;
        break;
      }
    }
    if (is_excluded)
      continue; // Skip excluded member

    // This member is a potential candidate
    if (!found_first) {
      current_sequencer_id = group->members[i].member_id;
      found_first = 1;
    } else {
      if (strcmp(group->members[i].member_id, current_sequencer_id) < 0) {
        current_sequencer_id = group->members[i].member_id;
      }
    }
  }

  // Return copy if found
  return (current_sequencer_id) ? strdup(current_sequencer_id) : NULL;
}

/*
 * Records the need to receive an ACK for a sent VC control message.
 * Finds a free slot in the vc_pending_acks buffer.
 */
static void track_control_ack(group_t *group, const char *target_id, uint8_t msg_type, const unsigned char *msg_data,
                              size_t msg_len) {
  if (!group || !target_id || !msg_data || msg_len == 0 || msg_len > UDP_BUFFER_SIZE) {
    fprintf(stderr, "TrackCtrlAck Error: Invalid arguments.\n");
    return;
  }

  pthread_mutex_lock(&group->vc_pending_ack_mutex);

  // Find a free slot
  int free_slot = -1;
  for (int i = 0; i < VC_PENDING_ACKS_SIZE; i++) {
    if (!group->vc_pending_acks[i].active) {
      free_slot = i;
      break;
    }
  }

  if (free_slot == -1) {
    // Buffer is full! Evict oldest? Log error?
    // For now, just log an error and don't track. This message might not be reliable.
    fprintf(stderr,
            "TrackCtrlAck Error: VC Pending ACK buffer is full for group '%s'! Cannot track "
            "ACK for msg type %u to %s.\n",
            group->group_name, msg_type, target_id);
  } else {
    PendingControlAck *p_ctrl_ack = &group->vc_pending_acks[free_slot];
    p_ctrl_ack->active = 1;
    p_ctrl_ack->message_type_sent = msg_type;
    strncpy(p_ctrl_ack->target_member_id, target_id, MAX_ID_LEN - 1);
    p_ctrl_ack->target_member_id[MAX_ID_LEN - 1] = '\0';
    p_ctrl_ack->sent_time = time(NULL);
    p_ctrl_ack->retry_count = 0;
    memcpy(p_ctrl_ack->message_data, msg_data, msg_len);
    p_ctrl_ack->message_len = msg_len;

    printf("TrackCtrlAck: Tracking ACK for msg type %u sent to %s (slot %d).\n", msg_type, target_id, free_slot);
  }

  pthread_mutex_unlock(&group->vc_pending_ack_mutex);
}

/*
 * Finalizes the view change:
 * 1. Reads the list of failed members from VC state.
 * 2. Removes ALL failed members from the list.
 * 3. Updates pending ACKs for ALL failed members.
 * 4. Enqueues failure notifications for ALL failed members.
 * 5. Resets view change state variables.
 * 6. Checks for sequencer change.
 */
static void finalize_failed_view_generalized(group_t *group) {
  if (!group)
    return;

  char failed_list_copy[MAX_MEMBERS][MAX_ID_LEN];
  int failed_count_copy = 0;
  // Structure to store info needed for notifications
  struct failed_info {
    char id[MAX_ID_LEN + 1];
    char ip[MAX_ADDR_LEN];
    int port;
  };
  struct failed_info failed_details[MAX_MEMBERS];
  int failed_details_count = 0;

  printf("FinalizeViewGen: Finalizing view change for group '%s'.\n", group->group_name);

  // --- 1. Get Failed Member List from VC State & Reset VC State ---
  pthread_mutex_lock(&group->vc_state_mutex);
  // Copy the list of members that failed *during this VC*
  failed_count_copy = group->vc_failed_member_count;
  printf("  -> Finalizing for %d failed members discovered during VC:\n", failed_count_copy);
  for (int i = 0; i < failed_count_copy; ++i) {
    strncpy(failed_list_copy[i], group->vc_failed_member_list[i], MAX_ID_LEN);
    failed_list_copy[i][MAX_ID_LEN - 1] = '\0';
    printf("     - %s\n", failed_list_copy[i]);
  }

  printf("  -> Resetting view change state (Active=0).\n");
  group->view_change_active = 0;
  group->view_change_stage = 0;
  group->vc_failed_member_count = 0; // Clear the list count
  for (int i = 0; i < MAX_MEMBERS; ++i)
    group->vc_failed_member_list[i][0] = '\0'; // Clear list content
  group->agreed_delivery_gseq = 0;
  group->sync_acks_needed = 0;
  group->sync_acks_received = 0;
  for (int k = 0; k < MAX_MEMBERS; ++k) {
    group->vc_received_states[k] = VC_STATE_SENTINEL;
    group->vc_state_reported[k] = 0;
  }
  pthread_mutex_unlock(&group->vc_state_mutex);

  if (failed_count_copy == 0) {
    fprintf(stderr, "FinalizeViewGen Warning: Finalizing VC but no failed members recorded in list?\n");
    // Still proceed to check sequencer change, maybe someone left normally concurrently?
  }

  // --- 2. Remove Member(s) & Update Pending ACKs ---
  pthread_mutex_lock(&group_data_mutex); // Lock group data

  char *old_sequencer_id = get_sequencer_id(group); // Get sequencer before removals

  // --- Find info and indices for all failed members (relative to current list) ---
  int failed_indices[MAX_MEMBERS]; // Store indices to remove
  int indices_to_remove_count = 0;
  failed_details_count = 0; // Reset details count for this run

  for (int f = 0; f < failed_count_copy; ++f) {
    if (failed_list_copy[f][0] == '\0')
      continue; // Skip empty slots in copied list

    for (int i = 0; i < group->member_count; ++i) {
      if (strcmp(group->members[i].member_id, failed_list_copy[f]) == 0) {
        // Store details for notification if space allows
        if (failed_details_count < MAX_MEMBERS) {
          strncpy(failed_details[failed_details_count].id, group->members[i].member_id, MAX_ID_LEN);
          failed_details[failed_details_count].id[MAX_ID_LEN] = '\0';
          strncpy(failed_details[failed_details_count].ip, group->members[i].address, MAX_ADDR_LEN - 1);
          failed_details[failed_details_count].ip[MAX_ADDR_LEN - 1] = '\0';
          failed_details[failed_details_count].port = group->members[i].port;
          failed_details_count++;
        } else {
          fprintf(stderr, "FinalizeViewGen Warning: Max failed details stored.\n");
        }

        // Check if index already marked for removal
        int already_marked = 0;
        for (int k = 0; k < indices_to_remove_count; ++k) {
          if (failed_indices[k] == i) {
            already_marked = 1;
            break;
          }
        }
        // Mark index for removal if not already marked and space allows
        if (!already_marked && indices_to_remove_count < MAX_MEMBERS) {
          failed_indices[indices_to_remove_count++] = i;
        } else if (!already_marked) {
          fprintf(stderr, "FinalizeViewGen Warning: Max indices to remove stored.\n");
        }
        break; // Found this failed ID, move to next failed ID in outer loop
      }
    }
  }

  // Sort indices in descending order for safe removal
  for (int i = 0; i < indices_to_remove_count - 1; ++i) {
    for (int j = 0; j < indices_to_remove_count - i - 1; ++j) {
      if (failed_indices[j] < failed_indices[j + 1]) {
        int temp = failed_indices[j];
        failed_indices[j] = failed_indices[j + 1];
        failed_indices[j + 1] = temp;
      }
    }
  }

  // --- Remove members (descending index order) ---
  int members_removed_count = 0;
  for (int i = 0; i < indices_to_remove_count; ++i) {
    int idx_to_remove = failed_indices[i];
    if (idx_to_remove >= 0 && idx_to_remove < group->member_count) { // Check bounds strictly
      printf("  -> Removing member '%s' (index %d) from list.\n", group->members[idx_to_remove].member_id,
             idx_to_remove);
      // Shift elements down
      for (int k = idx_to_remove; k < group->member_count - 1; k++) {
        memcpy(&group->members[k], &group->members[k + 1], sizeof(member_t));
        // Shift corresponding VC entries etc. if necessary
        // If VC is indexed by member position, shifting is needed:
        group->vector_clock[k] = group->vector_clock[k + 1];
      }
      group->member_count--;
      members_removed_count++;
      // Clear the new last slot
      if (group->member_count >= 0 && group->member_count < MAX_MEMBERS) { // Check bounds strictly
        memset(&group->members[group->member_count], 0, sizeof(member_t));
        group->members[group->member_count].port = -1;
        // Clear corresponding VC slot if indexed by position
        group->vector_clock[group->member_count] = 0;
      }
    } else {
      fprintf(stderr, "FinalizeViewGen Warning: Invalid index %d marked for removal (count=%d).\n", idx_to_remove,
              group->member_count);
    }
  }
  printf("  -> Member list updated. New count: %d\n", group->member_count);

  // --- Update Pending ACKs for ALL failed members ---
  printf("  -> Updating pending ACKs for %d failed members.\n", failed_count_copy);
  pthread_mutex_lock(&group->pending_ack_mutex);
  for (int i = 0; i < PENDING_ACKS_SIZE; i++) {
    PendingAckInfo *p_ack = &group->pending_acks[i];
    if (p_ack->active == 1) {
      int changes_made = 0; // Track if we modify this entry
      // Iterate through the list of *all* failed members from this VC
      for (int f = 0; f < failed_count_copy; ++f) {
        const char *current_failed_id = failed_list_copy[f];
        if (current_failed_id[0] == '\0')
          continue; // Skip empty slots

        // Check and remove current_failed_id from target_member_ids
        int target_idx = -1;
        for (int j = 0; j < p_ack->target_member_count; j++) {
          if (p_ack->target_member_ids && p_ack->target_member_ids[j] &&
              strcmp(p_ack->target_member_ids[j], current_failed_id) == 0) {
            target_idx = j;
            break;
          }
        }
        if (target_idx != -1) {
          free(p_ack->target_member_ids[target_idx]);
          // Shift remaining target IDs down
          for (int k = target_idx; k < p_ack->target_member_count - 1; k++) {
            p_ack->target_member_ids[k] = p_ack->target_member_ids[k + 1];
          }
          p_ack->target_member_ids[p_ack->target_member_count - 1] = NULL; // Nullify last pointer
          p_ack->target_member_count--;
          changes_made = 1;
        }

        // Check and remove current_failed_id from acked_member_ids
        int acked_idx = -1;
        for (int k = 0; k < p_ack->acks_received_count; ++k) {
          if (p_ack->acked_member_ids && p_ack->acked_member_ids[k] &&
              strcmp(p_ack->acked_member_ids[k], current_failed_id) == 0) {
            acked_idx = k;
            break;
          }
        }
        if (acked_idx != -1) {
          free(p_ack->acked_member_ids[acked_idx]);
          // Shift remaining acked IDs down
          for (int l = acked_idx; l < p_ack->acks_received_count - 1; ++l) {
            p_ack->acked_member_ids[l] = p_ack->acked_member_ids[l + 1];
          }
          p_ack->acked_member_ids[p_ack->acks_received_count - 1] = NULL; // Nullify last pointer
          p_ack->acks_received_count--;
          changes_made = 1;
        }
      } // End loop through failed members for this p_ack entry

      // Re-check if message is now fully acknowledged or has no targets left
      if (changes_made) { // Only re-check if we potentially removed someone
        if (p_ack->active == 1 && p_ack->target_member_count > 0 &&
            p_ack->acks_received_count >= p_ack->target_member_count) {
          printf("      - Message Seq=%u fully ACKed after failure finalization.\n", p_ack->seq_no);
          clear_pending_ack_entry(p_ack);
        } else if (p_ack->active == 1 && p_ack->target_member_count == 0) {
          printf("      - Message Seq=%u has no targets left after failure finalization. "
                 "Clearing.\n",
                 p_ack->seq_no);
          clear_pending_ack_entry(p_ack);
        }
      }
    } // End if p_ack active
  } // End loop through pending_acks
  pthread_mutex_unlock(&group->pending_ack_mutex);

  // --- 3. Enqueue Failure Notification(s) for Application ---
  pthread_mutex_lock(&group->msg_mutex);
  printf("  -> Enqueuing %d failure notifications for application.\n", failed_details_count);
  for (int i = 0; i < failed_details_count; ++i) {
    struct failed_info *finfo = &failed_details[i];
    if (finfo->id[0] == '\0')
      continue; // Skip if ID is empty

    printf("     - Enqueuing failure notification for '%s'.\n", finfo->id);
    uint8_t subtype = NOTIFY_SUBTYPE_FAILED;
    unsigned char notify_buffer[MAX_ID_LEN + MAX_ADDR_LEN + 10]; // Adjust size if needed
    unsigned char *nptr = notify_buffer;
    int nlen = 0;
    uint8_t n_id_l = strlen(finfo->id);
    uint8_t n_ip_l = strlen(finfo->ip);
    uint16_t n_port_n = htons(finfo->port);
    size_t needed = 1 + 1 + n_id_l + 1 + n_ip_l + 2; // Type(1)+IDLen(1)+ID+IPLen(1)+IP+Port(2)

    if (needed < sizeof(notify_buffer)) {
      // Build notification buffer
      *nptr++ = subtype;
      nlen++;
      *nptr++ = n_id_l;
      nlen++;
      memcpy(nptr, finfo->id, n_id_l);
      nptr += n_id_l;
      nlen += n_id_l;
      *nptr++ = n_ip_l;
      nlen++;
      memcpy(nptr, finfo->ip, n_ip_l);
      nptr += n_ip_l;
      nlen += n_ip_l;
      memcpy(nptr, &n_port_n, sizeof(uint16_t));
      nlen += sizeof(uint16_t);

      // Enqueue item
      if (group->msg_count < GROUP_MSG_QUEUE_SIZE) {
        GroupQueueItem *q_item = &group->message_buffer[group->msg_tail];
        q_item->internal_type = MSG_BUFFER_TYPE_NOTIFY;
        q_item->len = nlen;
        memcpy(q_item->data, notify_buffer, nlen);
        group->msg_tail = (group->msg_tail + 1) % GROUP_MSG_QUEUE_SIZE;
        group->msg_count++;
        pthread_cond_signal(&group->msg_cond); // Signal grp_recv
        printf("       - Notification enqueued.\n");
      } else {
        fprintf(stderr,
                "FinalizeViewGen Error: Delivery queue full, cannot enqueue notification "
                "for %s!\n",
                finfo->id);
      }
    } else {
      fprintf(stderr, "FinalizeViewGen Error: Notification data too large for %s.\n", finfo->id);
    }
  }
  pthread_mutex_unlock(&group->msg_mutex);

  // --- 4. Check for Sequencer Change ---
  // group_data_mutex is still HELD
  char *new_sequencer_id = get_sequencer_id(group); // Get sequencer based on the *final* list
  if (new_sequencer_id) {
    if (strcmp(group->my_id_in_group, new_sequencer_id) == 0) {
      // I am the sequencer now (or was already)
      if (!old_sequencer_id || strcmp(old_sequencer_id, new_sequencer_id) != 0) {
        // And I wasn't the sequencer before the failure(s)
        printf("  -> I ('%s') became the NEW sequencer after failure finalization.\n", group->my_id_in_group);
        // Synchronize next_global_seq with expected_global_seq
        group->next_global_seq = group->expected_global_seq;
        printf("      Set next_global_seq = expected_global_seq = %lu\n", (unsigned long)group->next_global_seq);
      } else {
        // I was already the sequencer, no change needed for next_global_seq here
      }
    }
    free(new_sequencer_id);
  } else if (group->member_count > 0) {
    fprintf(stderr, "FinalizeViewGen Error: Could not determine new sequencer even though "
                    "members remain!\n");
  } else {
    printf("FinalizeViewGen: Group is now empty.\n");
  }
  if (old_sequencer_id) {
    free(old_sequencer_id);
  }

  pthread_mutex_unlock(&group_data_mutex); // Unlock group data

  // --- 5. Reset View Change State (already done at the beginning) ---
  printf("FinalizeViewGen: View change finalized for group '%s'.\n", group->group_name);
}

/*
 * Called by all members after receiving CONFIRM (or by coordinator after sending CONFIRM).
 * Delivers stable messages from the unstable_buffer up to the agreed GSeq (max_gseq).
 * Calls finalize_failed_view_generalized at the end.
 */
static void process_stable_messages_for_delivery(group_t *group, uint64_t max_gseq) {
  if (!group)
    return;

  printf("ProcessStable: Processing messages up to GSeq %lu for group '%s'.\n", (unsigned long)max_gseq,
         group->group_name);

  int delivered_count = 0;
  uint64_t current_delivery_target_gseq;

  // We need to deliver messages in global sequence order, starting from
  // the current expected_global_seq up to max_gseq.
  pthread_mutex_lock(&group_data_mutex); // Lock to get initial expected_global_seq
  current_delivery_target_gseq = group->expected_global_seq;
  pthread_mutex_unlock(&group_data_mutex);
  printf("  -> Starting delivery check from GSeq %lu.\n", (unsigned long)current_delivery_target_gseq);

  // Loop while the target GSeq is within the agreed range
  while (current_delivery_target_gseq <= max_gseq) {
    int found_message_for_this_gseq = 0;
    // int unstable_idx = -1;
    UnstableDataMsg msg_to_deliver_copy; // Copy message data to avoid holding locks too long

    // Lock unstable buffer to search for the target message
    pthread_mutex_lock(&group->unstable_mutex);
    for (int i = 0; i < UNSTABLE_BUFFER_SIZE; i++) {
      if (group->unstable_data_buffer[i].active &&
          group->unstable_data_buffer[i].global_sequence_number == current_delivery_target_gseq) {
        // Found it, copy data and mark inactive under lock
        memcpy(&msg_to_deliver_copy, &group->unstable_data_buffer[i], sizeof(UnstableDataMsg));
        group->unstable_data_buffer[i].active = 0; // Mark as processed here
        group->unstable_count--;
        found_message_for_this_gseq = 1;
        break;
      }
    }
    pthread_mutex_unlock(&group->unstable_mutex); // Unlock unstable buffer

    if (found_message_for_this_gseq) {
      printf("  -> Found message with GSeq %lu (Sender: %s, LocalSeq: %u) in unstable buffer.\n",
             (unsigned long)msg_to_deliver_copy.global_sequence_number, msg_to_deliver_copy.sender_id,
             msg_to_deliver_copy.sequence_number);

      // Perform FINAL Causal Check (requires group_data_mutex)
      int final_causally_ready = 1; // Assume ready unless check fails
      int sender_list_idx = -1;
      int current_member_count_local;

      pthread_mutex_lock(&group_data_mutex);
      current_member_count_local = group->member_count; // Get current count under lock
      // Find sender index in the current member list
      for (int i = 0; i < current_member_count_local; ++i) {
        if (strcmp(group->members[i].member_id, msg_to_deliver_copy.sender_id) == 0) {
          sender_list_idx = i;
          break;
        }
      }

      if (sender_list_idx != -1) {
        // Compare message's VC with local VC
        for (int k = 0; k < current_member_count_local; k++) {
          if (k < MAX_MEMBERS) { // Bounds check
            // Check: msg_vc[k] <= local_vc[k] for all k
            if (msg_to_deliver_copy.received_vector_clock[k] > group->vector_clock[k]) {
              final_causally_ready = 0;
              printf("  -> Message GSeq %lu failed FINAL causal check (k=%d, "
                     "received=%d > local=%d).\n",
                     (unsigned long)msg_to_deliver_copy.global_sequence_number, k,
                     msg_to_deliver_copy.received_vector_clock[k], group->vector_clock[k]);
              break;
            }
          }
        }
      } else {
        // Sender is no longer in the group (e.g., the failed member)
        // Deliver based on the agreed_gseq (View Synchronicity)
        printf("  -> Sender '%s' for GSeq %lu not found in current members. Skipping final "
               "causal check (delivering based on agreement).\n",
               msg_to_deliver_copy.sender_id, (unsigned long)msg_to_deliver_copy.global_sequence_number);
        final_causally_ready = 1; // Force delivery based on agreement
      }

      if (final_causally_ready) {
        // Move message to final delivery queue
        pthread_mutex_lock(&group->msg_mutex);
        if (group->msg_count < GROUP_MSG_QUEUE_SIZE) {
          GroupQueueItem *deliver_slot = &group->message_buffer[group->msg_tail];
          // Copy data
          deliver_slot->internal_type = MSG_BUFFER_TYPE_DATA;
          memcpy(deliver_slot->data, msg_to_deliver_copy.data, msg_to_deliver_copy.len);
          deliver_slot->len = msg_to_deliver_copy.len;
          deliver_slot->sender_addr = msg_to_deliver_copy.sender_addr;
          deliver_slot->sequence_number = msg_to_deliver_copy.sequence_number;
          strncpy(deliver_slot->sender_id, msg_to_deliver_copy.sender_id, MAX_ID_LEN - 1);
          deliver_slot->sender_id[MAX_ID_LEN - 1] = '\0';
          memcpy(deliver_slot->received_vector_clock, msg_to_deliver_copy.received_vector_clock,
                 sizeof(deliver_slot->received_vector_clock));
          deliver_slot->global_sequence_number = msg_to_deliver_copy.global_sequence_number;

          // Update final queue state
          group->msg_tail = (group->msg_tail + 1) % GROUP_MSG_QUEUE_SIZE;
          group->msg_count++;
          delivered_count++;

          // Update local state (VC and expected GSeq) - group_data_mutex is HELD
          // Merge the delivered message's VC
          for (int k = 0; k < current_member_count_local; k++) {
            if (k < MAX_MEMBERS && msg_to_deliver_copy.received_vector_clock[k] > group->vector_clock[k]) {
              group->vector_clock[k] = msg_to_deliver_copy.received_vector_clock[k];
            }
          }
          // Update expected global sequence number
          group->expected_global_seq = current_delivery_target_gseq + 1;

          printf("  -> Delivered GSeq %lu. Updated local ExpectedGSeq to %lu.\n",
                 (unsigned long)current_delivery_target_gseq, (unsigned long)group->expected_global_seq);

          pthread_cond_signal(&group->msg_cond);
          pthread_mutex_unlock(&group->msg_mutex); // Unlock message queue
          pthread_mutex_unlock(&group_data_mutex); // Unlock group data

          // Move to the next target GSeq
          current_delivery_target_gseq++;

        } else {
          // Final queue full
          fprintf(stderr,
                  "ProcessStable Error: Final delivery queue full. Cannot deliver GSeq "
                  "%lu.\n",
                  (unsigned long)current_delivery_target_gseq);
          pthread_mutex_unlock(&group->msg_mutex);
          pthread_mutex_unlock(&group_data_mutex); // Unlock group data
          // Put message back into unstable buffer? Or discard? Discarding is simpler but
          // loses message. Let's log error and stop processing for now.
          fprintf(stderr, "ProcessStable Error: Message GSeq %lu lost due to full delivery queue!\n",
                  (unsigned long)msg_to_deliver_copy.global_sequence_number);
          break; // Stop processing
        }
      } else {
        // Failed causal check - Should not happen for messages <= agreed GSeq if logic is
        // correct This implies a preceding message is missing or wasn't delivered.
        fprintf(stderr,
                "ProcessStable Error: Message GSeq %lu failed final causal check "
                "unexpectedly. Stopping delivery.\n",
                (unsigned long)current_delivery_target_gseq);
        pthread_mutex_unlock(&group_data_mutex); // Unlock group data

        // Put message back into unstable buffer? Requires finding a free slot again.
        // For simplicity, log error and stop. This indicates a protocol error state.
        fprintf(stderr,
                "ProcessStable Error: Message GSeq %lu put back into unstable (or lost if "
                "full) due to causal failure!\n",
                (unsigned long)msg_to_deliver_copy.global_sequence_number);
        // Attempt to put back (best effort)
        pthread_mutex_lock(&group->unstable_mutex);
        int put_back_slot = -1;
        for (int i = 0; i < UNSTABLE_BUFFER_SIZE; ++i)
          if (!group->unstable_data_buffer[i].active) {
            put_back_slot = i;
            break;
          }
        if (put_back_slot != -1) {
          memcpy(&group->unstable_data_buffer[put_back_slot], &msg_to_deliver_copy, sizeof(UnstableDataMsg));
          group->unstable_data_buffer[put_back_slot].active = 1; // Mark active again
          group->unstable_count++;
        }
        pthread_mutex_unlock(&group->unstable_mutex);
        break; // Stop processing
      }
    } else {
      // Message for current_delivery_target_gseq not found in unstable buffer
      // It might have been delivered already, or lost.
      // We assume it was delivered if expected_global_seq was already incremented past it.
      pthread_mutex_lock(&group_data_mutex);
      int already_past = (current_delivery_target_gseq < group->expected_global_seq);
      pthread_mutex_unlock(&group_data_mutex);

      if (!already_past) {
        printf("  -> Message for GSeq %lu not found in unstable buffer. Assuming lost or "
               "delayed. Moving to next GSeq.\n",
               (unsigned long)current_delivery_target_gseq);
        // We MUST deliver in order up to max_gseq. If a message is missing, we cannot
        // proceed past it unless we have a mechanism to handle gaps (which this protocol
        // doesn't seem to have explicitly). For now, let's stop delivery if we hit a
        // missing message within the agreed range.
        fprintf(stderr,
                "ProcessStable Error: Message GSeq %lu is missing. Stopping delivery "
                "before max_gseq.\n",
                (unsigned long)current_delivery_target_gseq);
        break; // Stop processing
      } else {
        // We are checking a GSeq that is *lower* than our current expected one.
        // This means we already delivered it (or skipped it). Continue to the next target.
        printf("  -> GSeq %lu already processed (ExpectedGSeq=%lu). Moving to next GSeq.\n",
               (unsigned long)current_delivery_target_gseq, (unsigned long)group->expected_global_seq);
        current_delivery_target_gseq++;
      }
    }
  } // End while loop

  printf("ProcessStable: Finished delivering messages. Delivered %d messages in this run.\n", delivered_count);

  // --- Call finalize_failed_view ---
  // This removes the failed members and enqueues the failure notifications *after*
  // all agreed-upon messages have been delivered.
  finalize_failed_view_generalized(group);
}

/*
 * Sends the TYPE_VIEW_CHANGE_CONFIRM message (called by coordinator)
 * containing the agreed GSeq to all other remaining members reliably.
 */
static void send_view_change_confirm(group_t *group, uint64_t agreed_gseq) {
  if (!group) {
    fprintf(stderr, "SendVCConfirm Error: Invalid group pointer.\n");
    return;
  }

  printf("SendVCConfirm: Coordinator sending confirmation for group '%s', AgreedGSeq=%lu.\n", group->group_name,
         (unsigned long)agreed_gseq);

  char group_name_copy[MAX_GROUP_LEN + 1];
  char my_id_copy[MAX_ID_LEN + 1]; // Coordinator's ID
  char current_failed_list[MAX_MEMBERS][MAX_ID_LEN];
  int current_failed_count = 0;
  member_t remaining_members_info[MAX_MEMBERS]; // Temp storage for targets
  int remaining_count = 0;                      // Count of OTHER remaining members
  unsigned char *confirm_msg_buf = NULL;
  size_t confirm_msg_len = 0;

  // --- 1. Get necessary info under lock ---
  pthread_mutex_lock(&group->vc_state_mutex);
  // Check if view change is still active and in the correct stage (state collected)
  if (!group->view_change_active || group->view_change_stage < 2) { // Stage 2 = states collected
    fprintf(stderr, "SendVCConfirm Error: View change not active or not in correct stage (%d).\n",
            group->view_change_stage);
    pthread_mutex_unlock(&group->vc_state_mutex);
    return;
  }
  // Copy the list of failed members for exclusion
  current_failed_count = group->vc_failed_member_count;
  for (int i = 0; i < current_failed_count; ++i) {
    strncpy(current_failed_list[i], group->vc_failed_member_list[i], MAX_ID_LEN);
    current_failed_list[i][MAX_ID_LEN - 1] = '\0';
  }
  // Update stage to indicate confirmation is being sent / flushing begins
  group->view_change_stage = 3;
  pthread_mutex_unlock(&group->vc_state_mutex); // Unlock VC state mutex

  pthread_mutex_lock(&group_data_mutex);
  if (!group->is_active) {
    pthread_mutex_unlock(&group_data_mutex);
    fprintf(stderr, "SendVCConfirm Error: Group '%s' is not active.\n", group->group_name);
    return;
  }
  strncpy(group_name_copy, group->group_name, MAX_GROUP_LEN);
  group_name_copy[MAX_GROUP_LEN] = '\0';
  strncpy(my_id_copy, group->my_id_in_group, MAX_ID_LEN);
  my_id_copy[MAX_ID_LEN] = '\0';

  // Find addresses of other remaining members (excluding all known failed and self)
  printf("SendVCConfirm: Identifying remaining members to send CONFIRM msg (Excluding %d failed):\n",
         current_failed_count);
  remaining_count = 0;
  for (int i = 0; i < group->member_count; i++) {
    int is_failed = 0;
    for (int f = 0; f < current_failed_count; ++f) {
      if (current_failed_list[f][0] != '\0' && strcmp(group->members[i].member_id, current_failed_list[f]) == 0) {
        is_failed = 1;
        break;
      }
    }
    int is_self = (strcmp(group->members[i].member_id, my_id_copy) == 0);

    if (!is_failed && !is_self) {
      if (remaining_count < MAX_MEMBERS) {
        memcpy(&remaining_members_info[remaining_count], &group->members[i], sizeof(member_t));
        printf("  - Adding remaining member to send list: %s\n", group->members[i].member_id);
        remaining_count++;
      } else {
        fprintf(stderr, "SendVCConfirm Warning: Exceeded MAX_MEMBERS.\n");
      }
    }
  }
  pthread_mutex_unlock(&group_data_mutex); // Unlock group data

  // --- 2. Build TYPE_VIEW_CHANGE_CONFIRM message ---
  uint8_t msg_type = TYPE_VIEW_CHANGE_CONFIRM;
  uint8_t gn_len = strlen(group_name_copy);
  uint8_t sender_len = strlen(my_id_copy);  // Coordinator's ID
  uint64_t gseq_net = htobe64(agreed_gseq); // Agreed GSeq

  // Header: Type(1) + GroupLen(1) + GroupName + SenderLen(1) + MyID(CoordinatorID) +
  // AgreedGSeq(8)
  confirm_msg_len = 1 + 1 + gn_len + 1 + sender_len + sizeof(uint64_t);

  if (confirm_msg_len < UDP_BUFFER_SIZE) {
    confirm_msg_buf = malloc(confirm_msg_len);
    if (confirm_msg_buf) {
      unsigned char *ptr = confirm_msg_buf;
      memcpy(ptr, &msg_type, 1);
      ptr++;
      memcpy(ptr, &gn_len, 1);
      ptr++;
      memcpy(ptr, group_name_copy, gn_len);
      ptr += gn_len;
      memcpy(ptr, &sender_len, 1);
      ptr++;
      memcpy(ptr, my_id_copy, sender_len);
      ptr += sender_len;
      memcpy(ptr, &gseq_net, sizeof(uint64_t));
    } else {
      perror("SendVCConfirm: malloc failed");
      return;
    } // Cannot proceed
  } else {
    fprintf(stderr, "SendVCConfirm Error: CONFIRM message header too large!\n");
    return;
  } // Cannot proceed

  // --- 3. Send the message reliably to other remaining members ---
  if (confirm_msg_buf && remaining_count > 0) {
    printf("SendVCConfirm: Sending TYPE_VIEW_CHANGE_CONFIRM (AgreedGSeq=%lu) reliably to %d "
           "other members...\n",
           (unsigned long)agreed_gseq, remaining_count);
    int errors = 0;
    for (int i = 0; i < remaining_count; i++) {
      member_t *target_member = &remaining_members_info[i];
      if (target_member->address[0] == '\0' || target_member->port <= 0)
        continue;

      struct sockaddr_in dest_addr;
      memset(&dest_addr, 0, sizeof(dest_addr));
      dest_addr.sin_family = AF_INET;
      dest_addr.sin_port = htons(target_member->port);
      if (inet_pton(AF_INET, target_member->address, &dest_addr.sin_addr) <= 0)
        continue;

      ssize_t sent_bytes = sendto(global_udp_socket, confirm_msg_buf, confirm_msg_len, 0, (struct sockaddr *)&dest_addr,
                                  sizeof(struct sockaddr_in));
      if (sent_bytes < 0) {
        char addr_str[INET_ADDRSTRLEN]; // For error reporting
        inet_ntop(AF_INET, &dest_addr.sin_addr, addr_str,
                  sizeof(addr_str)); // Use correct address
        fprintf(stderr, "SendVCConfirm: sendto failed for member %s at %s:%d - %s\n", target_member->member_id,
                addr_str, ntohs(dest_addr.sin_port), strerror(errno));
        errors++;
      } else {
        // Track control message ACK
        track_control_ack(group, target_member->member_id, TYPE_VIEW_CHANGE_CONFIRM, confirm_msg_buf, confirm_msg_len);
      }
    }
    if (errors > 0) {
      fprintf(stderr, "SendVCConfirm Warning: Failed initial send of CONFIRM message to %d members.\n", errors);
      // Coordinator still proceeds, relies on retransmission thread
    }
  } else if (remaining_count == 0) {
    printf("SendVCConfirm: No other remaining members to send CONFIRM to.\n");
    // Coordinator is the only one left, can proceed directly.
  }

  // Cleanup message buffer (it's copied by track_control_ack)
  if (confirm_msg_buf) {
    free(confirm_msg_buf);
  }

  // --- 4. Coordinator proceeds to next step (delivery) ---
  // The coordinator, having sent the confirm, knows the agreed GSeq
  // and can now start delivering stable messages up to that point.
  printf("SendVCConfirm: Coordinator proceeding to deliver stable messages up to GSeq %lu.\n",
         (unsigned long)agreed_gseq);
  process_stable_messages_for_delivery(group, agreed_gseq); // Call the next step

  printf("SendVCConfirm: Finished confirmation process for group '%s'.\n", group->group_name);
}

/*
 * Sends this member's state (currently just expected_global_seq)
 * to the coordinator during a view change.
 */
static void send_view_change_state(group_t *group, const char *coordinator_id) {
  if (!group || !coordinator_id) {
    fprintf(stderr, "SendVCState Error: Invalid arguments.\n");
    return;
  }

  printf("SendVCState: Preparing to send state for group '%s' to coordinator '%s'.\n", group->group_name,
         coordinator_id);

  struct sockaddr_in coordinator_addr;
  int found_coord_addr = 0;
  uint64_t my_expected_gseq = 0;
  char group_name_copy[MAX_GROUP_LEN + 1];
  char my_id_copy[MAX_ID_LEN + 1];

  // --- 1. Get Coordinator Address and Own State (under lock) ---
  pthread_mutex_lock(&group_data_mutex);
  if (!group->is_active) {
    pthread_mutex_unlock(&group_data_mutex);
    fprintf(stderr, "SendVCState Error: Group '%s' is not active.\n", group->group_name);
    return;
  }
  // Copy data needed outside the lock
  my_expected_gseq = group->expected_global_seq;
  strncpy(group_name_copy, group->group_name, MAX_GROUP_LEN);
  group_name_copy[MAX_GROUP_LEN] = '\0';
  strncpy(my_id_copy, group->my_id_in_group, MAX_ID_LEN);
  my_id_copy[MAX_ID_LEN] = '\0';

  // Find coordinator address
  for (int i = 0; i < group->member_count; i++) {
    if (strcmp(group->members[i].member_id, coordinator_id) == 0) {
      member_t *coord = &group->members[i];
      if (coord->address[0] != '\0' && coord->port > 0) {
        memset(&coordinator_addr, 0, sizeof(coordinator_addr));
        coordinator_addr.sin_family = AF_INET;
        coordinator_addr.sin_port = htons(coord->port);
        if (inet_pton(AF_INET, coord->address, &coordinator_addr.sin_addr) > 0) {
          found_coord_addr = 1;
        } else {
          fprintf(stderr, "SendVCState Error: Invalid IP address format for coordinator '%s'.\n", coordinator_id);
        }
      } else {
        fprintf(stderr, "SendVCState Error: Missing address/port for coordinator '%s'.\n", coordinator_id);
      }
      break; // Found the coordinator ID, no need to check further
    }
  }
  pthread_mutex_unlock(&group_data_mutex); // Unlock group data

  if (!found_coord_addr) {
    fprintf(stderr,
            "SendVCState Error: Could not find network address for coordinator '%s'. Cannot "
            "send state.\n",
            coordinator_id);
    return;
  }

  // --- 2. Build TYPE_VIEW_CHANGE_STATE message ---
  unsigned char *state_msg_buf = NULL;
  size_t state_msg_len = 0;

  uint8_t msg_type = TYPE_VIEW_CHANGE_STATE;
  uint8_t gn_len = strlen(group_name_copy);
  uint8_t sender_len = strlen(my_id_copy);       // My ID as sender
  uint64_t gseq_net = htobe64(my_expected_gseq); // State being sent

  // Header: Type(1) + GroupLen(1) + GroupName + SenderLen(1) + MyID + State_ExpectedGSeq(8)
  state_msg_len = 1 + 1 + gn_len + 1 + sender_len + sizeof(uint64_t);

  if (state_msg_len < UDP_BUFFER_SIZE) {
    state_msg_buf = malloc(state_msg_len);
    if (state_msg_buf) {
      unsigned char *ptr = state_msg_buf;
      memcpy(ptr, &msg_type, 1);
      ptr++;
      memcpy(ptr, &gn_len, 1);
      ptr++;
      memcpy(ptr, group_name_copy, gn_len);
      ptr += gn_len;
      memcpy(ptr, &sender_len, 1);
      ptr++;
      memcpy(ptr, my_id_copy, sender_len);
      ptr += sender_len;
      memcpy(ptr, &gseq_net, sizeof(uint64_t));
    } else {
      perror("SendVCState: malloc failed for STATE message");
      return;
    }
  } else {
    fprintf(stderr, "SendVCState Error: STATE message header too large!\n");
    return;
  }

  // --- 3. Send the message to the coordinator ---
  if (state_msg_buf) {
    printf("SendVCState: Sending state (ExpectedGSeq=%lu) to coordinator '%s'...\n", (unsigned long)my_expected_gseq,
           coordinator_id);
    ssize_t sent_bytes = sendto(global_udp_socket, state_msg_buf, state_msg_len, 0,
                                (struct sockaddr *)&coordinator_addr, sizeof(struct sockaddr_in));
    if (sent_bytes < 0) {
      perror("SendVCState: sendto failed");
      // Should we retry? For now, just log the error. Reliability handled by ACK tracking.
    } else {
      // Track control message ACK
      track_control_ack(group, coordinator_id, TYPE_VIEW_CHANGE_STATE, state_msg_buf, state_msg_len);

      // Optionally update local stage to indicate state sent
      pthread_mutex_lock(&group->vc_state_mutex);
      if (group->view_change_active && group->view_change_stage < 2) { // Update only if in early stage
        // Stage 2 now means "State Sent / Waiting for Confirm" for non-coordinators
        // Stage 2 for coordinators means "Collecting States"
        // Let's not change stage here for non-coordinators, wait for CONFIRM.
        // group->view_change_stage = 2;
      }
      pthread_mutex_unlock(&group->vc_state_mutex);
    }
    free(state_msg_buf); // Free the buffer after sending (or attempting send)
  }
}

/*
 * Initiates the view change process when the *first* failure is detected for a group.
 * Called only by the member determined to be the initial coordinator.
 * Sends TYPE_VIEW_CHANGE_START messages reliably to all other remaining members.
 */
static void initiate_failure_view_change(group_t *group, const char *first_failed_id) {
  if (!group || !first_failed_id) {
    fprintf(stderr, "InitiateVC Error: Invalid arguments.\n");
    return;
  }

  printf("InitiateVC: Starting for group '%s', first failed member '%s'.\n", group->group_name, first_failed_id);

  char group_name_copy[MAX_GROUP_LEN + 1];
  char my_id_copy[MAX_ID_LEN + 1];              // Coordinator's ID
  member_t remaining_members_info[MAX_MEMBERS]; // Store info of OTHERS
  int remaining_count = 0;
  unsigned char *start_msg_buf = NULL;
  size_t start_msg_len = 0;

  // --- 1. Get necessary info (group name, my ID, other survivors) ---
  pthread_mutex_lock(&group_data_mutex);
  if (!group->is_active) { /* Handle inactive group */
    pthread_mutex_unlock(&group_data_mutex);
    return;
  }
  strncpy(group_name_copy, group->group_name, MAX_GROUP_LEN);
  group_name_copy[MAX_GROUP_LEN] = '\0';
  strncpy(my_id_copy, group->my_id_in_group, MAX_ID_LEN);
  my_id_copy[MAX_ID_LEN] = '\0';

  printf("InitiateVC: Identifying remaining members (Excluding '%s' and self):\n", first_failed_id);
  for (int i = 0; i < group->member_count; i++) {
    if (strcmp(group->members[i].member_id, first_failed_id) == 0)
      continue;
    if (strcmp(group->members[i].member_id, my_id_copy) == 0)
      continue;
    if (remaining_count < MAX_MEMBERS) {
      memcpy(&remaining_members_info[remaining_count], &group->members[i], sizeof(member_t));
      printf("  - Adding remaining member to send list: %s\n", group->members[i].member_id);
      remaining_count++;
    } else {
      fprintf(stderr, "InitiateVC Warning: Exceeded MAX_MEMBERS.\n");
    }
  }
  printf("InitiateVC: Found %d other remaining members to notify.\n", remaining_count);
  pthread_mutex_unlock(&group_data_mutex);

  // --- 2. Set Coordinator's VC State (sync_acks_needed) ---
  pthread_mutex_lock(&group->vc_state_mutex);
  // Ensure VC is marked active and in stage 1 (should have been done by caller)
  if (!group->view_change_active || group->view_change_stage != 1) {
    fprintf(stderr,
            "InitiateVC Error: Called but VC state not correctly initialized (active=%d, "
            "stage=%d).\n",
            group->view_change_active, group->view_change_stage);
    pthread_mutex_unlock(&group->vc_state_mutex);
    return;
  }
  group->sync_acks_needed = remaining_count; // Need state from all other survivors
  printf("InitiateVC: Coordinator '%s' set sync_acks_needed to %d.\n", my_id_copy, remaining_count);
  pthread_mutex_unlock(&group->vc_state_mutex);

  // --- 3. Build the TYPE_VIEW_CHANGE_START message ---
  uint8_t msg_type = TYPE_VIEW_CHANGE_START;
  uint8_t gn_len = strlen(group_name_copy);
  uint8_t sender_len = strlen(my_id_copy); // Coordinator's ID
  uint8_t failed_len = strlen(first_failed_id);

  // Header: Type(1) + GroupLen(1) + GroupName + SenderLen(1) + MyID(CoordinatorID) +
  // FailedIDLen(1) + FailedID
  start_msg_len = 1 + 1 + gn_len + 1 + sender_len + 1 + failed_len;

  if (start_msg_len < UDP_BUFFER_SIZE) {
    start_msg_buf = malloc(start_msg_len);
    if (start_msg_buf) {
      unsigned char *ptr = start_msg_buf;
      memcpy(ptr, &msg_type, 1);
      ptr++;
      memcpy(ptr, &gn_len, 1);
      ptr++;
      memcpy(ptr, group_name_copy, gn_len);
      ptr += gn_len;
      memcpy(ptr, &sender_len, 1);
      ptr++;
      memcpy(ptr, my_id_copy, sender_len);
      ptr += sender_len;
      memcpy(ptr, &failed_len, 1);
      ptr++;
      memcpy(ptr, first_failed_id, failed_len);
    } else { /* Handle malloc failure */
      perror("InitiateVC: malloc failed");
      return;
    }
  } else { /* Handle message too large */
    fprintf(stderr, "InitiateVC Error: START message too large!\n");
    return;
  }

  // --- 4. Send TYPE_VIEW_CHANGE_START reliably to other remaining members ---
  if (start_msg_buf && remaining_count > 0) {
    printf("InitiateVC: Sending TYPE_VIEW_CHANGE_START reliably to %d other members...\n", remaining_count);
    int errors = 0;
    for (int i = 0; i < remaining_count; i++) {
      member_t *target_member = &remaining_members_info[i];
      if (target_member->address[0] == '\0' || target_member->port <= 0)
        continue;
      struct sockaddr_in dest_addr;
      memset(&dest_addr, 0, sizeof(dest_addr));
      dest_addr.sin_family = AF_INET;
      dest_addr.sin_port = htons(target_member->port);
      if (inet_pton(AF_INET, target_member->address, &dest_addr.sin_addr) <= 0)
        continue;

      ssize_t sent_bytes = sendto(global_udp_socket, start_msg_buf, start_msg_len, 0, (struct sockaddr *)&dest_addr,
                                  sizeof(struct sockaddr_in));
      if (sent_bytes < 0) {
        perror("InitiateVC: sendto failed for VIEW_CHANGE_START");
        errors++;
      } else {
        // Track control message ACK
        track_control_ack(group, target_member->member_id, TYPE_VIEW_CHANGE_START, start_msg_buf, start_msg_len);
      }
    }
    if (errors > 0) {
      fprintf(stderr, "InitiateVC Warning: Failed initial send of START message to %d members.\n", errors);
    }
  } else if (remaining_count == 0) {
    // Coordinator is the only survivor. Move directly to confirmation/finalization.
    printf("InitiateVC: No other remaining members found. (I am the only one left).\n");
    pthread_mutex_lock(&group->vc_state_mutex);
    if (group->view_change_active && group->sync_acks_needed == 0) { // Check if still active and no ACKs needed
      pthread_mutex_lock(&group_data_mutex);                         // Need own expected GSeq
      group->agreed_delivery_gseq = group->expected_global_seq;
      pthread_mutex_unlock(&group_data_mutex);
      group->view_change_stage = 3; // Move to confirmed stage
      printf("  -> Only member left. Setting agreed GSeq to %lu and proceeding to confirm.\n",
             (unsigned long)group->agreed_delivery_gseq);
      uint64_t agreed_gseq_copy = group->agreed_delivery_gseq;
      pthread_mutex_unlock(&group->vc_state_mutex);
      // Call confirm (will find no one to send to, then call process_stable)
      send_view_change_confirm(group, agreed_gseq_copy);
    } else {
      pthread_mutex_unlock(&group->vc_state_mutex);
    }
  }

  // Cleanup
  if (start_msg_buf) {
    free(start_msg_buf);
  }
  printf("InitiateVC: Finished initiating view change for group '%s'.\n", group->group_name);
}

// --- ΝΕΑ Βοηθητική Συνάρτηση: Εύρεση Sequencer σε δοθείσα λίστα ---
/* Finds the sequencer within a given list of members.
 * Used by grp_join to find the *previous* sequencer.
 * Returns a strdup'd copy of the sequencer's ID, or NULL if list is empty/invalid.
 * Caller MUST free the returned string.
 */
static char *find_sequencer_in_list(member_t member_list[], int count) {
  if (!member_list || count <= 0) {
    return NULL;
  }
  char *min_id = NULL;
  int min_id_found = 0;
  for (int i = 0; i < count; i++) {
    if (member_list[i].member_id[0] != '\0') {
      if (!min_id_found || strcmp(member_list[i].member_id, min_id) < 0) {
        min_id = member_list[i].member_id;
        min_id_found = 1;
      }
    }
  }
  return (min_id) ? strdup(min_id) : NULL;
}

// --- ΝΕΑ Βοηθητική Συνάρτηση: Εύρεση Sequencer ---
/*
 * Βρίσκει το ID του τρέχοντα sequencer για το group.
 * Κανόνας: Το μέλος με το λεξικογραφικά μικρότερο ID.
 * ΠΡΟΣΟΧΗ: Πρέπει να καλείται με το group_data_mutex κλειδωμένο!
 * Επιστρέφει ένα ΑΝΤΙΓΡΑΦΟ του ID (που πρέπει να γίνει free) ή NULL.
 */
static char *get_sequencer_id(group_t *group) {
  if (!group || !group->is_active || group->member_count == 0) {
    return NULL;
  }
  char *current_sequencer_id = NULL;
  // Βρες το πρώτο έγκυρο ID για αρχική σύγκριση
  for (int i = 0; i < group->member_count; ++i) {
    if (group->members[i].member_id[0] != '\0') {
      current_sequencer_id = group->members[i].member_id;
      break;
    }
  }
  if (current_sequencer_id == NULL)
    return NULL; // Δεν βρέθηκε κανένα έγκυρο ID;

  // Σύγκρινε με τα υπόλοιπα
  for (int i = 1; i < group->member_count; i++) {
    if (group->members[i].member_id[0] != '\0') {
      if (strcmp(group->members[i].member_id, current_sequencer_id) < 0) {
        current_sequencer_id = group->members[i].member_id;
      }
    }
  }
  // Επέστρεψε αντίγραφο
  return strdup(current_sequencer_id);
}

/*
 * Clears a PendingAckInfo entry, freeing associated memory.
 * Used for DATA/SEQ_ASSIGNMENT ACK tracking.
 */
void clear_pending_ack_entry(PendingAckInfo *entry) {
  if (!entry)
    return;
  int old_target_count = entry->target_member_count; // Store counts before resetting
  int old_acked_count = entry->acks_received_count;

  entry->active = 0;
  entry->seq_no = 0;
  entry->acks_received_count = 0;
  entry->target_member_count = 0;
  if (entry->message_data) {
    free(entry->message_data);
    entry->message_data = NULL;
  }
  entry->message_len = 0;
  if (entry->target_member_ids) {
    // Use the count stored *before* clearing the entry
    for (int i = 0; i < old_target_count && i < entry->target_member_cap; i++) {
      if (entry->target_member_ids[i]) {
        free(entry->target_member_ids[i]);
        entry->target_member_ids[i] = NULL; // Good practice
      }
    }
    free(entry->target_member_ids);
    entry->target_member_ids = NULL;
  }
  entry->target_member_cap = 0;
  if (entry->acked_member_ids) {
    // Use the count stored *before* clearing the entry
    for (int i = 0; i < old_acked_count && i < entry->acked_member_cap; i++) {
      if (entry->acked_member_ids[i]) {
        free(entry->acked_member_ids[i]);
        entry->acked_member_ids[i] = NULL; // Good practice
      }
    }
    free(entry->acked_member_ids);
    entry->acked_member_ids = NULL;
  }
  entry->acked_member_cap = 0;
  entry->original_msg_type = 0;
}

// Αρχικοποίηση της ουράς (καλείται στην grp_init)
void init_gms_queue() {
  gms_queue.head = 0;
  gms_queue.tail = 0;
  gms_queue.count = 0;
  pthread_mutex_init(&gms_queue.mutex, NULL);
}

// Καταστροφή της ουράς (καλείται στην grp_shutdown)
void destroy_gms_queue() { pthread_mutex_destroy(&gms_queue.mutex); }

// Προσθήκη μηνύματος στην ουρά (καλείται από τον tcp_handler_func)
int enqueue_gms_response(const unsigned char *data, int len) {
  int result = -1;
  pthread_mutex_lock(&gms_queue.mutex);
  if (gms_queue.count < GMS_RESPONSE_QUEUE_SIZE) {
    // Αντιγραφή δεδομένων στο επόμενο διαθέσιμο slot
    memcpy(gms_queue.messages[gms_queue.tail].data, data, len);
    gms_queue.messages[gms_queue.tail].len = len;
    gms_queue.tail = (gms_queue.tail + 1) % GMS_RESPONSE_QUEUE_SIZE;
    gms_queue.count++;
    printf("Queue: Enqueued message (%d bytes), count=%d\n", len, gms_queue.count);
    result = 0; // Επιτυχία
  } else {
    fprintf(stderr, "Error: GMS Response Queue is full!\n");
    result = -1; // Αποτυχία - Η ουρά είναι γεμάτη
  }
  pthread_mutex_unlock(&gms_queue.mutex);
  return result;
}

// Αφαίρεση μηνύματος από την ουρά (καλείται από grp_join/grp_leave)
int dequeue_gms_response(unsigned char *buffer, int buffer_size) {
  int msg_len = -1;
  pthread_mutex_lock(&gms_queue.mutex);
  if (gms_queue.count > 0) {
    // Πάρε το μήκος του μηνύματος στην αρχή της ουράς
    msg_len = gms_queue.messages[gms_queue.head].len;
    if (msg_len <= buffer_size) {
      // Αντιγραφή δεδομένων στον buffer του καλούντα
      memcpy(buffer, gms_queue.messages[gms_queue.head].data, msg_len);
    } else {
      fprintf(stderr, "Error: Dequeue buffer too small (%d bytes required, %d provided).\n", msg_len, buffer_size);
      msg_len = -1; // Σφάλμα - ανεπαρκής buffer
    }
    // Προχώρα τον δείκτη κεφαλής ανεξάρτητα από το αν έγινε αντιγραφή
    gms_queue.head = (gms_queue.head + 1) % GMS_RESPONSE_QUEUE_SIZE;
    gms_queue.count--;
    printf("Queue: Dequeued message (%d bytes), count=%d\n", (msg_len > 0 ? msg_len : 0), gms_queue.count);
  } else {
    // Η ουρά είναι άδεια
    msg_len = 0; // Επιστροφή 0 για άδεια ουρά
  }
  pthread_mutex_unlock(&gms_queue.mutex);
  return msg_len; // Επιστρέφει μήκος ή -1 για λάθος buffer ή 0 για άδεια ουρά
}

// --- Retransmission Thread (CORRECTED Lock Order) ---
void *retransmission_thread_func(void *arg) {
  printf("Retransmission thread started (Check Interval: %d s, Timeout: %d s).\n", RETRANSMISSION_CHECK_INTERVAL,
         RETRANSMISSION_TIMEOUT);

  while (keep_retransmission_thread_running) {
    sleep(RETRANSMISSION_CHECK_INTERVAL);
    if (!keep_retransmission_thread_running)
      break;

    time_t current_time = time(NULL);

    for (int g = 0; g < MAX_GROUPS; g++) {

      // --- Acquire Global Lock FIRST ---
      pthread_mutex_lock(&group_data_mutex);

      // --- Check if Group is Active ---
      if (global_groups[g].is_active) {
        group_t *target_group = &global_groups[g]; // Safe to get pointer now

        // --- Check Pending ACKs for DATA/SEQ_ASSIGN ---
        pthread_mutex_lock(&target_group->pending_ack_mutex);
        for (int i = 0; i < PENDING_ACKS_SIZE; i++) {
          PendingAckInfo *p_ack = &target_group->pending_acks[i];
          if (p_ack->active == 1) {
            if (difftime(current_time, p_ack->sent_time) > RETRANSMISSION_TIMEOUT) {
              printf("Retransmission: Timeout detected for %s Seq=%u in group '%s'.\n",
                     (p_ack->original_msg_type == TYPE_DATA_UNSTABLE) ? "DATA" : "SEQ_ASSIGN", p_ack->seq_no,
                     target_group->group_name);
              int retransmissions_sent = 0;
              // Iterate through targets who haven't ACKed
              // group_data_mutex is HELD, safe to access members list
              for (int target_idx = 0; target_idx < p_ack->target_member_count; target_idx++) {
                char *target_id = p_ack->target_member_ids[target_idx];
                if (!target_id)
                  continue;
                int has_acked = 0;
                for (int acked_idx = 0; acked_idx < p_ack->acks_received_count; acked_idx++) {
                  if (p_ack->acked_member_ids[acked_idx] &&
                      strcmp(p_ack->acked_member_ids[acked_idx], target_id) == 0) {
                    has_acked = 1;
                    break;
                  }
                }

                if (!has_acked) {
                  member_t *dest_member = NULL;
                  for (int m = 0; m < target_group->member_count; ++m)
                    if (strcmp(target_group->members[m].member_id, target_id) == 0) {
                      dest_member = &target_group->members[m];
                      break;
                    }

                  if (dest_member && dest_member->address[0] != '\0' && dest_member->port > 0) {
                    struct sockaddr_in dest_addr;
                    memset(&dest_addr, 0, sizeof(dest_addr));
                    dest_addr.sin_family = AF_INET;
                    dest_addr.sin_port = htons(dest_member->port);
                    if (inet_pton(AF_INET, dest_member->address, &dest_addr.sin_addr) <= 0) {
                      continue;
                    }
                    printf("Retransmitting %s Seq=%u to %s (%s:%d)\n",
                           (p_ack->original_msg_type == TYPE_DATA_UNSTABLE) ? "DATA" : "SEQ_ASSIGN", p_ack->seq_no,
                           target_id, dest_member->address, dest_member->port);
                    ssize_t sent_bytes = sendto(global_udp_socket, p_ack->message_data, p_ack->message_len, 0,
                                                (struct sockaddr *)&dest_addr, sizeof(dest_addr));
                    if (sent_bytes < 0) {
                      perror("Retransmission sendto failed");
                    } else {
                      retransmissions_sent++;
                    }
                  } else {
                    printf("Retransmission: Member %s not found or invalid "
                           "address for DATA/SEQ_ASSIGN retransmit.\n",
                           target_id);
                  }
                }
              }
              if (retransmissions_sent > 0) {
                p_ack->sent_time = time(NULL); /* TODO: retry count for data */
              }
              // TODO: Handle max retries for data messages? (e.g., declare member
              // failed?)
            }
          }
        }
        pthread_mutex_unlock(&target_group->pending_ack_mutex); // Release specific lock

        // --- Check Pending ACKs for VC Control Messages ---
        pthread_mutex_lock(&target_group->vc_pending_ack_mutex);
        for (int i = 0; i < VC_PENDING_ACKS_SIZE; i++) {
          PendingControlAck *p_ctrl_ack = &target_group->vc_pending_acks[i];
          if (p_ctrl_ack->active == 1) {
            if (difftime(current_time, p_ctrl_ack->sent_time) > RETRANSMISSION_TIMEOUT) {
              const char *msg_type_str = "UNKNOWN_VC";
              if (p_ctrl_ack->message_type_sent == TYPE_VIEW_CHANGE_START)
                msg_type_str = "START";
              else if (p_ctrl_ack->message_type_sent == TYPE_VIEW_CHANGE_STATE)
                msg_type_str = "STATE";
              else if (p_ctrl_ack->message_type_sent == TYPE_VIEW_CHANGE_CONFIRM)
                msg_type_str = "CONFIRM";
              else if (p_ctrl_ack->message_type_sent == TYPE_VIEW_CHANGE_RESTART)
                msg_type_str = "RESTART";

              printf("Retransmission: Timeout detected for VC Control Msg Type=%s to "
                     "Member='%s' in group '%s'.\n",
                     msg_type_str, p_ctrl_ack->target_member_id, target_group->group_name);

              p_ctrl_ack->retry_count++;
              if (p_ctrl_ack->retry_count > MAX_CONTROL_MSG_RETRIES) {
                fprintf(stderr,
                        "Retransmission Error: Max retries (%d) reached for VC "
                        "Control Msg Type=%s to Member='%s'. Giving up on ACK.\n",
                        MAX_CONTROL_MSG_RETRIES, msg_type_str, p_ctrl_ack->target_member_id);
                p_ctrl_ack->active = 0; // Deactivate entry

                // *** CRITICAL TODO: Trigger Failure Handling ***
                // Exceeding retries for VC messages implies failure.
                // Need to call a function here that simulates a GMS notification
                // or directly triggers the failure handling logic in
                // tcp_handler_func for p_ctrl_ack->target_member_id. This requires
                // careful design to avoid deadlocks and re-entrancy issues.
                fprintf(stderr,
                        "Retransmission: *** Member %s potentially failed (timeout on "
                        "VC ACK). Failure handling NOT YET IMPLEMENTED HERE. ***\n",
                        p_ctrl_ack->target_member_id);

                continue; // Move to next pending ack
              }

              // Find target address and resend
              // group_data_mutex is HELD, safe to access members list
              member_t *dest_member = NULL;
              struct sockaddr_in dest_addr;
              int found_addr = 0;

              for (int m = 0; m < target_group->member_count; m++) {
                if (strcmp(target_group->members[m].member_id, p_ctrl_ack->target_member_id) == 0) {
                  dest_member = &target_group->members[m];
                  if (dest_member->address[0] != '\0' && dest_member->port > 0) {
                    memset(&dest_addr, 0, sizeof(dest_addr));
                    dest_addr.sin_family = AF_INET;
                    dest_addr.sin_port = htons(dest_member->port);
                    if (inet_pton(AF_INET, dest_member->address, &dest_addr.sin_addr) > 0) {
                      found_addr = 1;
                    }
                  }
                  break;
                }
              }

              if (found_addr) {
                printf("Retransmitting VC Control Msg Type=%s to %s (%s:%d) (Retry "
                       "%d)\n",
                       msg_type_str, p_ctrl_ack->target_member_id, dest_member->address, dest_member->port,
                       p_ctrl_ack->retry_count);
                ssize_t sent_bytes = sendto(global_udp_socket, p_ctrl_ack->message_data, p_ctrl_ack->message_len, 0,
                                            (struct sockaddr *)&dest_addr, sizeof(dest_addr));
                if (sent_bytes < 0) {
                  perror("Retransmission sendto failed for VC control msg");
                } else {
                  p_ctrl_ack->sent_time = time(NULL);
                } // Update sent time only on successful send
              } else {
                printf("Retransmission: Member %s not found or invalid address in "
                       "group %s, cannot retransmit VC Control Msg Type=%s\n",
                       p_ctrl_ack->target_member_id, target_group->group_name, msg_type_str);
                // Member might have left/failed in the meantime. Deactivate?
                p_ctrl_ack->active = 0; // Deactivate if member is gone
              }
            } // end if timeout
          } // end if active
        } // end for vc_pending_acks
        pthread_mutex_unlock(&target_group->vc_pending_ack_mutex); // Release specific lock
        // ----------------------------------------------------

        // *** NEW: Check Pending SEQ_ASSIGNMENT Requests ***
        pthread_mutex_lock(&target_group->pending_seq_assignment_mutex);
        for (int i = 0; i < PENDING_SEQ_ASSIGNMENTS_SIZE; i++) {
          PendingSeqAssignmentInfo *p_assign = &target_group->pending_seq_assignments[i];

          if (p_assign->active == 1) {
            if (difftime(current_time, p_assign->request_sent_time) > SEQ_REQUEST_TIMEOUT) {
              printf("Retransmission: Timeout waiting for SEQ_ASSIGNMENT for LocalSeq=%u in group '%s'.\n",
                     p_assign->original_local_seq_no, target_group->group_name);

              p_assign->retry_count++;
              if (p_assign->retry_count > MAX_SEQ_REQUEST_RETRIES) {
                fprintf(
                    stderr,
                    "Retransmission Error: Max retries (%d) reached for SEQ_REQUEST LocalSeq=%u. Giving up tracking.\n",
                    MAX_SEQ_REQUEST_RETRIES, p_assign->original_local_seq_no);
                // NOTE: We don't declare failure here, just stop tracking this request.
                // The original DATA_UNSTABLE might still be ACKed by others.
                // If the sequencer *did* fail, a view change should eventually handle it.
                p_assign->active = 0; // Stop trying
                continue;             // Move to next pending assignment check
              }

              // Find the *CURRENT* sequencer address (group_data_mutex is HELD)
              char *current_sequencer_id = get_sequencer_id(target_group); // Needs free
              struct sockaddr_in current_seq_addr;
              int found_current_seq = 0;
              if (current_sequencer_id) {
                for (int m = 0; m < target_group->member_count; m++) {
                  if (strcmp(target_group->members[m].member_id, current_sequencer_id) == 0) {
                    memset(&current_seq_addr, 0, sizeof(current_seq_addr));
                    current_seq_addr.sin_family = AF_INET;
                    current_seq_addr.sin_port = htons(target_group->members[m].port);
                    if (inet_pton(AF_INET, target_group->members[m].address, &current_seq_addr.sin_addr) > 0) {
                      found_current_seq = 1;
                    } else {
                      fprintf(stderr, "Retransmission Error: Invalid IP for current sequencer %s.\n",
                              current_sequencer_id);
                    }
                    break;
                  }
                }
              } else {
                fprintf(stderr,
                        "Retransmission Warning: Cannot find current sequencer for group %s to resend SEQ_REQUEST.\n",
                        target_group->group_name);
              }

              if (found_current_seq) {
                // Rebuild the SEQ_REQUEST message
                uint8_t req_msg_type = TYPE_SEQ_REQUEST;
                // Use group name stored in p_assign
                uint8_t req_group_name_len = strlen(p_assign->group_name);
                // Use original sender ID stored in p_assign
                uint8_t req_sender_id_len = strlen(p_assign->original_sender_id);
                uint32_t req_seq_no_net = htonl(p_assign->original_local_seq_no);
                size_t req_header_len = 1 + 1 + req_group_name_len + 1 + req_sender_id_len + sizeof(uint32_t);
                unsigned char *request_buffer = malloc(req_header_len);

                if (request_buffer) {
                  unsigned char *req_ptr = request_buffer;
                  memcpy(req_ptr, &req_msg_type, sizeof(uint8_t));
                  req_ptr++;
                  memcpy(req_ptr, &req_group_name_len, sizeof(uint8_t));
                  req_ptr++;
                  memcpy(req_ptr, p_assign->group_name, req_group_name_len);
                  req_ptr += req_group_name_len;
                  memcpy(req_ptr, &req_sender_id_len, sizeof(uint8_t));
                  req_ptr++;
                  memcpy(req_ptr, p_assign->original_sender_id, req_sender_id_len);
                  req_ptr += req_sender_id_len;
                  memcpy(req_ptr, &req_seq_no_net, sizeof(uint32_t));

                  printf("Retransmitting SEQ_REQUEST for LocalSeq=%u to current sequencer '%s' (Retry %d)\n",
                         p_assign->original_local_seq_no, current_sequencer_id, p_assign->retry_count);
                  ssize_t sent_bytes = sendto(global_udp_socket, request_buffer, req_header_len, MSG_DONTWAIT,
                                              (struct sockaddr *)&current_seq_addr, sizeof(current_seq_addr));
                  if (sent_bytes < 0) {
                    // Don't update time if send failed, will retry next interval
                    perror("Retransmission sendto failed for SEQ_REQUEST");
                  } else {
                    p_assign->request_sent_time = time(NULL); // Update time only on successful send
                  }
                  free(request_buffer);
                } else {
                  perror("Retransmission: malloc failed for SEQ_REQUEST");
                  // If malloc fails, we probably can't retry effectively.
                  // Keep the retry count, maybe it works next time? Or deactivate?
                  // Let's log and continue for now.
                }
              } else {
                printf("Retransmission: Cannot find/resolve current sequencer '%s' to resend SEQ_REQUEST for "
                       "LocalSeq=%u.\n",
                       current_sequencer_id ? current_sequencer_id : "UNKNOWN", p_assign->original_local_seq_no);
                // Keep retrying? Maybe the sequencer will appear/address resolve.
                // Update timestamp to avoid rapid retries.
                p_assign->request_sent_time = time(NULL);
              }
              if (current_sequencer_id)
                free(current_sequencer_id); // Free the ID obtained from get_sequencer_id

            } // end if timeout
          } // end if active
        } // end for pending_seq_assignments
        pthread_mutex_unlock(&target_group->pending_seq_assignment_mutex);
        // *** END NEW Check ***

      } // end if group is active

      // --- Release Global Lock ---
      pthread_mutex_unlock(&group_data_mutex);

    } // end for groups
  } // end while keep_running

  printf("Retransmission thread exiting.\n");
  return NULL;
}

// --- Συνάρτηση TCP Handler Thread (Τροποποιημένη για Ουρά) ---

/*
 * Λαμβάνει μηνύματα από GMS. Βάζει τις απαντήσεις JOIN/LEAVE στην ουρά.
 * Επεξεργάζεται απευθείας Notifications και PINGs.
 */
void *tcp_handler_func(void *arg) {
  unsigned char buffer[GMS_BUFFER_SIZE];
  ssize_t nbytes; // Use ssize_t for read/recv return value

  printf("TCP Handler thread started (listens on socket %d).\n", gms_tcp_socket);

  while (keep_tcp_handler_running) {
    // TODO: Implement robust read loop for partial reads / multiple messages.
    // For now, assuming one message per read for simplicity.
    nbytes = read(gms_tcp_socket, buffer, sizeof(buffer) - 1); // Use read for TCP

    if (!keep_tcp_handler_running)
      break; // Check flag after potential block

    if (nbytes <= 0) {
      if (nbytes == 0) {
        fprintf(stderr, "TCP Handler: GMS connection closed.\n");
      } else {
        perror("TCP Handler: read error from GMS");
      }
      keep_tcp_handler_running = 0; // Stop the loop
      // Signal waiting threads about the error?
      // User's code might rely on semaphores, which might not work well here.
      // Let's signal the cond vars just in case, though user uses semaphores.
      break;
    }

    buffer[nbytes] = '\0'; // Null-terminate for safety

    // --- Parse GMS Message Header (User's assumption: Type only?) ---
    // User's code checked only buffer[0], let's keep that for now.
    if (nbytes < 1) {
      fprintf(stderr, "TCP Handler Error: Received too few bytes (< 1).\n");
      continue;
    }
    uint8_t msg_type = buffer[0]; // Διαβάζουμε τον τύπο

    // --- Έλεγχος Τύπου Μηνύματος ---
    if (msg_type == RESP_ACK_SUCCESS || msg_type == RESP_NACK_DUPLICATE_ID || msg_type == RESP_NACK_NOT_IN_GROUP) {
      // Είναι απάντηση σε JOIN/LEAVE - Βάλ' την στην ουρά
      printf("TCP Handler: Received JOIN/LEAVE response (type %u), enqueueing...\n", msg_type);
      if (enqueue_gms_response(buffer, nbytes) == 0) {
        // Κάνε signal ΜΟΝΟ αν η προσθήκη στην ουρά πέτυχε
        printf("TCP Handler: Signaling semaphore.\n");
        // User's semaphore logic based on size (fragile)
        if (nbytes >= 1 + sizeof(uint16_t)) { // Assuming ACK_SUCCESS has size >= 3
          sem_post(&join_mtx);
        } else if (nbytes == 1) { // Assuming NACK might be size 1? Or leave ACK?
          sem_post(&leave_mtx);   // This logic is likely incorrect / depends heavily on GMS
                                  // exact responses
        } else {
          // Fallback? Signal join anyway?
          sem_post(&join_mtx);
        }
      } else {
        fprintf(stderr, "TCP Handler Error: Failed to enqueue GMS response!\n");
      }
    } else if (msg_type == NOTIFY_MEMBER_JOINED || msg_type == NOTIFY_MEMBER_LEFT) {
      // --- Είναι Notification για Join/Leave - Επεξεργασία απευθείας (όπως πριν) ---
      printf("TCP Handler: Received Member Notification (type %u).\n", msg_type);
      unsigned char *current_ptr = buffer + 1;
      int remaining_bytes = nbytes - 1;
      char group_name[MAX_GROUP_LEN + 1];
      char member_id[MAX_ID_LEN + 1];
      char member_ip_str[INET_ADDRSTRLEN];
      int member_port;
      int parse_ok = 1;

      // --- Parsing Notification Data (όπως πριν) ---
      // ... (το ίδιο parsing logic με πριν για group_name, member_id, ip, port) ...
      if (remaining_bytes < sizeof(uint8_t)) {
        parse_ok = 0;
      }
      uint8_t group_name_len = 0;
      if (parse_ok) {
        group_name_len = *current_ptr++;
        remaining_bytes--;
      }
      if (!parse_ok || group_name_len == 0 || group_name_len > MAX_GROUP_LEN || remaining_bytes < group_name_len) {
        parse_ok = 0;
      }
      if (parse_ok) {
        memcpy(group_name, current_ptr, group_name_len);
        group_name[group_name_len] = '\0';
        current_ptr += group_name_len;
        remaining_bytes -= group_name_len;
      }
      if (parse_ok && remaining_bytes < sizeof(uint8_t)) {
        parse_ok = 0;
      }
      uint8_t member_id_len = 0;
      if (parse_ok) {
        member_id_len = *current_ptr++;
        remaining_bytes--;
      }
      if (!parse_ok || member_id_len == 0 || member_id_len > MAX_ID_LEN || remaining_bytes < member_id_len) {
        parse_ok = 0;
      }
      if (parse_ok) {
        memcpy(member_id, current_ptr, member_id_len);
        member_id[member_id_len] = '\0';
        current_ptr += member_id_len;
        remaining_bytes -= member_id_len;
      }
      if (parse_ok && remaining_bytes < sizeof(uint32_t) + sizeof(uint16_t)) {
        parse_ok = 0;
      }
      uint32_t member_ip_net = 0;
      uint16_t member_port_net = 0;
      if (parse_ok) {
        memcpy(&member_ip_net, current_ptr, sizeof(uint32_t));
        current_ptr += sizeof(uint32_t);
        memcpy(&member_port_net, current_ptr, sizeof(uint16_t));
      }
      if (!parse_ok) {
        fprintf(stderr, "TCP Handler Error: Failed to parse JOIN/LEAVE notification.\n");
        continue;
      }
      struct in_addr member_ip_addr;
      member_ip_addr.s_addr = member_ip_net;
      if (inet_ntop(AF_INET, &member_ip_addr, member_ip_str, sizeof(member_ip_str)) == NULL) {
        strcpy(member_ip_str, "?.?.?.?");
      }
      member_port = ntohs(member_port_net);
      // --- Τέλος Parsing ---

      printf("TCP Handler: Parsed Notification: Type=%u Group='%s' MemberID='%s' Addr=%s:%d\n", msg_type, group_name,
             member_id, member_ip_str, member_port);

      pthread_mutex_lock(&group_data_mutex);
      int target_group_index = -1;
      for (int i = 0; i < MAX_GROUPS; i++) {
        if (global_groups[i].is_active && strcmp(global_groups[i].group_name, group_name) == 0) {
          target_group_index = i;
          break;
        }
      }

      if (target_group_index != -1) {
        group_t *target_group = &global_groups[target_group_index];
        int member_index = -1;
        for (int i = 0; i < target_group->member_count; ++i) {
          if (strcmp(target_group->members[i].member_id, member_id) == 0) {
            member_index = i;
            break;
          }
        }

        if (msg_type == NOTIFY_MEMBER_JOINED) {
          // --- Add member (ίδιο με πριν) ---
          if (member_index == -1 && target_group->member_count < MAX_MEMBERS) {
            member_index = target_group->member_count;
            member_t *new_slot = &target_group->members[member_index];
            strncpy(new_slot->member_id, member_id, MAX_ID_LEN - 1);
            new_slot->member_id[MAX_ID_LEN - 1] = '\0';
            strncpy(new_slot->address, member_ip_str, MAX_ADDR_LEN - 1);
            new_slot->address[MAX_ADDR_LEN - 1] = '\0';
            new_slot->port = member_port;
            new_slot->expected_recv_seq = 0;
            new_slot->ooo_count = 0;
            for (int k = 0; k < OUT_OF_ORDER_BUFFER_SIZE; ++k)
              new_slot->ooo_buffer[k].active = 0;
            target_group->vector_clock[member_index] = 0; // Init VC entry
            target_group->member_count++;
            printf("TCP Handler: Added member '%s' to group '%s'. New count: %d\n", member_id, group_name,
                   target_group->member_count);
          }
        } else { // NOTIFY_MEMBER_LEFT
          // --- Remove member (ίδιο με πριν, συμπεριλαμβανομένου sequencer check) ---
          if (member_index != -1) {
            char *old_sequencer_id = get_sequencer_id(target_group);
            printf("TCP Handler: Removing member '%s' (index %d) from group '%s'.\n", member_id, member_index,
                   group_name);
            for (int k = member_index; k < target_group->member_count - 1; k++) {
              memcpy(&target_group->members[k], &target_group->members[k + 1], sizeof(member_t));
              target_group->vector_clock[k] = target_group->vector_clock[k + 1];
            }
            target_group->member_count--;
            if (target_group->member_count < MAX_MEMBERS) {
              target_group->vector_clock[target_group->member_count] = 0;
            }
            memset(&target_group->members[target_group->member_count], 0, sizeof(member_t));
            char *new_sequencer_id = get_sequencer_id(target_group);
            if (new_sequencer_id && strcmp(target_group->my_id_in_group, new_sequencer_id) == 0) {
              if (!old_sequencer_id || strcmp(old_sequencer_id, new_sequencer_id) != 0) {
                printf("TCP Handler: I ('%s') became the NEW sequencer for group "
                       "'%s'.\n",
                       target_group->my_id_in_group, group_name);
                target_group->next_global_seq = target_group->expected_global_seq;
                printf("             Set next_global_seq = expected_global_seq = "
                       "%lu\n",
                       (unsigned long)target_group->next_global_seq);
              }
            }
            if (old_sequencer_id)
              free(old_sequencer_id);
            if (new_sequencer_id)
              free(new_sequencer_id);
          }
        }
        // --- Enqueue Notification for Application (ίδιο με πριν) ---
        uint8_t subtype_for_queue = msg_type;
        unsigned char notify_buffer_for_queue[MAX_ID_LEN + MAX_ADDR_LEN + 10];
        unsigned char *nptr = notify_buffer_for_queue;
        int nlen = 0;
        uint8_t n_id_l = strlen(member_id);
        uint8_t n_ip_l = strlen(member_ip_str);
        uint16_t n_port_n = htons(member_port);
        size_t needed = 1 + 1 + n_id_l + 1 + n_ip_l + 2;
        if (needed < sizeof(notify_buffer_for_queue)) {
          *nptr++ = subtype_for_queue;
          nlen++;
          *nptr++ = n_id_l;
          nlen++;
          memcpy(nptr, member_id, n_id_l);
          nptr += n_id_l;
          nlen += n_id_l;
          *nptr++ = n_ip_l;
          nlen++;
          memcpy(nptr, member_ip_str, n_ip_l);
          nptr += n_ip_l;
          nlen += n_ip_l;
          memcpy(nptr, &n_port_n, sizeof(uint16_t));
          nlen += sizeof(uint16_t);
          pthread_mutex_lock(&target_group->msg_mutex);
          if (target_group->msg_count < GROUP_MSG_QUEUE_SIZE) {
            GroupQueueItem *q_item = &target_group->message_buffer[target_group->msg_tail];
            q_item->internal_type = MSG_BUFFER_TYPE_NOTIFY;
            q_item->len = nlen;
            memcpy(q_item->data, notify_buffer_for_queue, nlen);
            target_group->msg_tail = (target_group->msg_tail + 1) % GROUP_MSG_QUEUE_SIZE;
            target_group->msg_count++;
            pthread_cond_signal(&target_group->msg_cond);
          }
          pthread_mutex_unlock(&target_group->msg_mutex);
        }
        // --- Update Pending ACKs for LEFT member (ίδιο με πριν) ---
        if (msg_type == NOTIFY_MEMBER_LEFT && member_index != -1) {
          printf("TCP Handler: Updating pending ACKs for departed member '%s' in group "
                 "'%s'...\n",
                 member_id, group_name);
          pthread_mutex_lock(&target_group->pending_ack_mutex);
          for (int i = 0; i < PENDING_ACKS_SIZE; i++) {
            PendingAckInfo *p_ack = &target_group->pending_acks[i];
            if (p_ack->active == 1) {
              int target_found_at = -1;
              for (int j = 0; j < p_ack->target_member_count; j++) {
                if (p_ack->target_member_ids && p_ack->target_member_ids[j] &&
                    strcmp(p_ack->target_member_ids[j], member_id) == 0) {
                  target_found_at = j;
                  break;
                }
              }
              if (target_found_at != -1) {
                free(p_ack->target_member_ids[target_found_at]);
                for (int k = target_found_at; k < p_ack->target_member_count - 1; k++) {
                  p_ack->target_member_ids[k] = p_ack->target_member_ids[k + 1];
                }
                p_ack->target_member_ids[p_ack->target_member_count - 1] = NULL;
                p_ack->target_member_count--;
                printf("TCP Handler: Removed '%s' from targets for Seq=%u. New "
                       "counts T/A: %d/%d\n",
                       member_id, p_ack->seq_no, p_ack->target_member_count, p_ack->acks_received_count);
                if (p_ack->active == 1 && p_ack->target_member_count > 0 &&
                    p_ack->acks_received_count >= p_ack->target_member_count) {
                  printf("TCP Handler: Message Seq=%u fully ACKed after "
                         "departure.\n",
                         p_ack->seq_no);
                  clear_pending_ack_entry(p_ack);
                } else if (p_ack->active == 1 && p_ack->target_member_count == 0) {
                  printf("TCP Handler: Message Seq=%u has no targets left after "
                         "departure. Clearing.\n",
                         p_ack->seq_no);
                  clear_pending_ack_entry(p_ack);
                }
              }
            }
          }
          pthread_mutex_unlock(&target_group->pending_ack_mutex);
        }

      } // end if group found
      pthread_mutex_unlock(&group_data_mutex); // Unlock after handling JOIN/LEAVE notification

    }
    // ============================================================
    // === MODIFIED HANDLING FOR NOTIFY_MEMBER_FAILED ===
    // ============================================================
    else if (msg_type == NOTIFY_MEMBER_FAILED) {
      printf("TCP Handler: Received MEMBER_FAILED Notification (type %u).\n", msg_type);
      unsigned char *current_ptr = buffer + 1;
      int remaining_bytes = nbytes - 1;
      char group_name[MAX_GROUP_LEN + 1] = {0};
      char failed_member_id[MAX_ID_LEN + 1] = {0};
      int parse_ok = 1;

      // --- Parsing Group Name & Failed Member ID ---
      /* ... (same parsing logic as before) ... */
      if (remaining_bytes < sizeof(uint8_t)) {
        parse_ok = 0;
      }
      uint8_t group_name_len = 0;
      if (parse_ok) {
        group_name_len = *current_ptr++;
        remaining_bytes--;
      }
      if (!parse_ok || group_name_len == 0 || group_name_len > MAX_GROUP_LEN || remaining_bytes < group_name_len) {
        parse_ok = 0;
      }
      if (parse_ok) {
        memcpy(group_name, current_ptr, group_name_len);
        group_name[group_name_len] = '\0';
        current_ptr += group_name_len;
        remaining_bytes -= group_name_len;
      }
      if (parse_ok && remaining_bytes < sizeof(uint8_t)) {
        parse_ok = 0;
      }
      uint8_t member_id_len = 0;
      if (parse_ok) {
        member_id_len = *current_ptr++;
        remaining_bytes--;
      }
      if (!parse_ok || member_id_len == 0 || member_id_len > MAX_ID_LEN || remaining_bytes < member_id_len) {
        parse_ok = 0;
      }
      if (parse_ok) {
        memcpy(failed_member_id, current_ptr, member_id_len);
        failed_member_id[member_id_len] = '\0';
      }
      if (!parse_ok) {
        fprintf(stderr, "TCP Handler Error: Failed to parse FAILED notification.\n");
        continue;
      }
      // --- End Parsing ---

      printf("TCP Handler: Parsed FAILED Notification: Group='%s' FailedMemberID='%s'\n", group_name, failed_member_id);

      // --- Find Group & Handle Failure ---
      pthread_mutex_lock(&group_data_mutex); // Lock group data first
      group_t *target_group = NULL;
      for (int i = 0; i < MAX_GROUPS; i++) {
        if (global_groups[i].is_active && strcmp(global_groups[i].group_name, group_name) == 0) {
          target_group = &global_groups[i];
          break;
        }
      }

      if (target_group) {
        int is_vc_active_local;
        char current_failed_list[MAX_MEMBERS][MAX_ID_LEN]; // To pass to get_sequencer...
        int current_failed_count = 0;
        int failed_already_known = 0;
        char my_id_copy[MAX_ID_LEN + 1];
        strncpy(my_id_copy, target_group->my_id_in_group, MAX_ID_LEN);
        my_id_copy[MAX_ID_LEN] = '\0';

        pthread_mutex_lock(&target_group->vc_state_mutex); // Lock VC state

        is_vc_active_local = target_group->view_change_active;

        // Check if this failure is already known *for this VC instance*
        for (int i = 0; i < target_group->vc_failed_member_count; i++) {
          if (strcmp(target_group->vc_failed_member_list[i], failed_member_id) == 0) {
            failed_already_known = 1;
            break;
          }
        }

        if (failed_already_known) {
          printf("TCP Handler: Failure of '%s' already known for current VC. Ignoring "
                 "duplicate notification.\n",
                 failed_member_id);
          pthread_mutex_unlock(&target_group->vc_state_mutex); // Unlock VC state
          pthread_mutex_unlock(&group_data_mutex);             // Unlock group data
          continue;                                            // Skip processing
        }

        // Add the newly failed member to the list *before* determining coordinator
        if (target_group->vc_failed_member_count < MAX_MEMBERS) {
          strncpy(target_group->vc_failed_member_list[target_group->vc_failed_member_count], failed_member_id,
                  MAX_ID_LEN - 1);
          target_group->vc_failed_member_list[target_group->vc_failed_member_count][MAX_ID_LEN - 1] = '\0';
          target_group->vc_failed_member_count++;
          printf("TCP Handler: Added '%s' to VC failed list (new count: %d).\n", failed_member_id,
                 target_group->vc_failed_member_count);
        } else {
          fprintf(stderr, "TCP Handler Warning: VC failed member list full! Cannot add '%s'.\n", failed_member_id);
          // Continue processing, but this member won't be officially excluded
        }

        // Copy the *updated* current list for coordinator calculation
        current_failed_count = target_group->vc_failed_member_count;
        for (int i = 0; i < current_failed_count; ++i) {
          strncpy(current_failed_list[i], target_group->vc_failed_member_list[i], MAX_ID_LEN);
          current_failed_list[i][MAX_ID_LEN - 1] = '\0';
        }

        if (!is_vc_active_local) {
          // --- Start a NEW View Change ---
          printf("TCP Handler: Starting new view change for failed member '%s'.\n", failed_member_id);
          target_group->view_change_active = 1;
          target_group->view_change_stage = 1; // Stage 1: Started
                                               // Reset counters/state (list count already updated)
          target_group->sync_acks_received = 0;
          target_group->sync_acks_needed = 0;
          for (int k = 0; k < MAX_MEMBERS; ++k) {
            target_group->vc_received_states[k] = VC_STATE_SENTINEL;
            target_group->vc_state_reported[k] = 0;
          }
          pthread_mutex_unlock(&target_group->vc_state_mutex); // Unlock VC state

          // Determine Coordinator (Excluding the one failed member)
          // We hold group_data_mutex
          char *coordinator_id =
              get_sequencer_id_excluding_list(target_group, current_failed_list, current_failed_count);
          int i_am_coordinator = 0;
          if (coordinator_id) {
            i_am_coordinator = (strcmp(my_id_copy, coordinator_id) == 0);
            printf("TCP Handler: Coordinator for new VC is '%s'. Am I it? %s\n", coordinator_id,
                   i_am_coordinator ? "YES" : "NO");
            free(coordinator_id);
          } else {
            free(coordinator_id);
            fprintf(stderr, "TCP Handler: Could not find coordinator for new VC!\n");
          }

          pthread_mutex_unlock(&group_data_mutex); // Unlock group data

          if (i_am_coordinator) {
            // Initiate the VC START messages (pass only the first failed ID)
            initiate_failure_view_change(target_group, failed_member_id);
          } else {
            printf("TCP Handler: I am not the coordinator, waiting for START/RESTART.\n");
          }

        } else {
          // --- Failure DURING an Active View Change ---
          printf("TCP Handler: Received FAILED for '%s' while VC is active.\n", failed_member_id);

          // Determine the coordinator *of the ongoing VC*
          // Need the list *before* adding the current failure
          char previous_failed_list[MAX_MEMBERS][MAX_ID_LEN];
          int previous_failed_count = current_failed_count - 1;
          for (int i = 0; i < previous_failed_count; ++i) {
            strncpy(previous_failed_list[i], target_group->vc_failed_member_list[i], MAX_ID_LEN);
            previous_failed_list[i][MAX_ID_LEN - 1] = '\0';
          }

          // We hold group_data_mutex
          char *current_coordinator_id =
              get_sequencer_id_excluding_list(target_group, previous_failed_list, previous_failed_count);

          if (current_coordinator_id && strcmp(failed_member_id, current_coordinator_id) == 0) {
            // *** The CURRENT COORDINATOR FAILED! ***
            printf("TCP Handler: *** COORDINATOR '%s' FAILED DURING VIEW CHANGE! ***\n", current_coordinator_id);
            free(current_coordinator_id); // Free this ID

            // Reset state for restart (keep vc_active=1, list is already updated)
            target_group->view_change_stage = 1; // Back to stage 1
            target_group->sync_acks_received = 0;
            target_group->sync_acks_needed = 0;
            for (int k = 0; k < MAX_MEMBERS; ++k) {
              target_group->vc_received_states[k] = VC_STATE_SENTINEL;
              target_group->vc_state_reported[k] = 0;
            }
            // TODO: Clear pending control ACKs expected *from* the failed coordinator?
            pthread_mutex_lock(&target_group->vc_pending_ack_mutex);
            for (int i = 0; i < VC_PENDING_ACKS_SIZE; ++i) {
              if (target_group->vc_pending_acks[i].active &&
                  strcmp(target_group->vc_pending_acks[i].target_member_id, failed_member_id) == 0) {
                target_group->vc_pending_acks[i].active = 0;
              }
            }
            pthread_mutex_unlock(&target_group->vc_pending_ack_mutex);

            pthread_mutex_unlock(&target_group->vc_state_mutex); // Unlock VC state

            // Determine the NEW coordinator (excluding the UPDATED list)
            // We hold group_data_mutex
            char *new_coordinator_id =
                get_sequencer_id_excluding_list(target_group, current_failed_list, current_failed_count);
            int i_am_new_coordinator = 0;
            if (new_coordinator_id) {
              i_am_new_coordinator = (strcmp(my_id_copy, new_coordinator_id) == 0);
              printf("TCP Handler: New coordinator after failure is '%s'. Am I it? %s\n", new_coordinator_id,
                     i_am_new_coordinator ? "YES" : "NO");
              free(new_coordinator_id);
            } else {
              fprintf(stderr, "TCP Handler: Could not find new coordinator after "
                              "previous one failed!\n");
            }

            pthread_mutex_unlock(&group_data_mutex); // Unlock group data

            if (i_am_new_coordinator) {
              // Initiate the restart process
              printf("TCP Handler: I am the new coordinator, initiating restart...\n");
              send_view_change_restart_generalized(target_group); // New function
            } else {
              printf("TCP Handler: Coordinator failed, I am not the new one. Waiting "
                     "for RESTART.\n");
            }

          } else {
            // *** Another NON-COORDINATOR member failed during VC ***
            printf("TCP Handler: Non-coordinator '%s' failed during active VC. "
                   "Coordinator '%s' will handle implicitly.\n",
                   failed_member_id, current_coordinator_id ? current_coordinator_id : "UNKNOWN");
            if (current_coordinator_id)
              free(current_coordinator_id);

            // ******** ΑΡΧΗ ΑΛΛΑΓΗΣ ********

            // Check if *I* am the current coordinator processing this failure notification
            // Need to re-check coordinator status using the list BEFORE this new failure
            char *coord_check_id =
                get_sequencer_id_excluding_list(target_group, previous_failed_list, previous_failed_count);
            int i_am_the_coordinator = 0;
            if (coord_check_id) {
              i_am_the_coordinator = (strcmp(my_id_copy, coord_check_id) == 0);
              free(coord_check_id);
            }

            if (i_am_the_coordinator &&
                target_group->view_change_stage <
                    3) { // Only adjust if I am coord and state collection/VC is not already finalizing
              printf("  -> I am the coordinator. Adjusting needed states due to failure of %s.\n", failed_member_id);

              // Find the index of the newly failed member in the *current* member list
              // Note: group_data_mutex is HELD here
              int failed_member_idx = -1;
              for (int i = 0; i < target_group->member_count; ++i) {
                if (strcmp(target_group->members[i].member_id, failed_member_id) == 0) {
                  failed_member_idx = i;
                  break;
                }
              }

              if (failed_member_idx != -1 && failed_member_idx < MAX_MEMBERS) {
                // vc_state_mutex is HELD from outer block
                // Check if we were still waiting for state from this member
                if (!target_group->vc_state_reported[failed_member_idx]) {
                  printf("  -> Was waiting for state from failed member %s (index %d). Adjusting needed count.\n",
                         failed_member_id, failed_member_idx);
                  if (target_group->sync_acks_needed > 0) { // Avoid going below zero
                    target_group->sync_acks_needed--;
                  }
                  target_group->vc_state_reported[failed_member_idx] = 1; // Mark as handled/irrelevant
                  printf("  -> Adjusted sync_acks_needed to %d.\n", target_group->sync_acks_needed);
                } else {
                  printf("  -> State from failed member %s (index %d) was already reported. Needed count remains %d.\n",
                         failed_member_id, failed_member_idx, target_group->sync_acks_needed);
                }

                // *** RE-CHECK if enough states received NOW ***
                printf("  -> Re-checking state completion: Received=%d, Needed=%d\n", target_group->sync_acks_received,
                       target_group->sync_acks_needed);
                if (target_group->sync_acks_received >= target_group->sync_acks_needed) {
                  printf("  -> All required states now received after failure adjustment! Triggering confirmation.\n");
                  // Set flag to calculate/send confirm *after* releasing mutexes
                  target_group->view_change_stage = 3; // Move stage forward
                }
              } else {
                fprintf(
                    stderr,
                    "TCP Handler Warning: Failed member %s not found in current list during VC failure processing.\n",
                    failed_member_id);
              }
            }
            // ******** ΤΕΛΟΣ ΑΛΛΑΓΗΣ ********
            pthread_mutex_lock(&target_group->vc_pending_ack_mutex);
            for (int i = 0; i < VC_PENDING_ACKS_SIZE; ++i) {
              // Clear ACKs *expected from* the failed non-coordinator
              if (target_group->vc_pending_acks[i].active &&
                  strcmp(target_group->vc_pending_acks[i].target_member_id, failed_member_id) == 0) {
                target_group->vc_pending_acks[i].active = 0;
              }
            }
            pthread_mutex_unlock(&target_group->vc_pending_ack_mutex);

            pthread_mutex_unlock(&target_group->vc_state_mutex); // Unlock VC state
            pthread_mutex_unlock(&group_data_mutex);             // Unlock group data
          }
        }
      } else {
        pthread_mutex_unlock(&group_data_mutex); // Unlock group data if group not found
        fprintf(stderr, "TCP Handler: Received FAILED notification for unknown/inactive group '%s'.\n", group_name);
      }
    } // End handling NOTIFY_MEMBER_FAILED
    // ============================================================
    // === END MODIFIED HANDLING ===
    // ============================================================
    else if (msg_type == MSG_TYPE_PING) {
      // User's PING/PONG logic
      // printf("TCP Handler: Received PING from GMS. Sending PONG\n");
      uint8_t pong = MSG_TYPE_PONG;                    // Assuming PONG type is 21
      send(gms_tcp_socket, &pong, sizeof(uint8_t), 0); // Ignore send errors for simplicity
    } else {
      fprintf(stderr, "TCP Handler Warning: Received unknown message type (%u).\n", msg_type);
    }

    // TODO: Handle multiple messages received in one read() call.

  } // end while keep_running

  printf("TCP Handler thread exiting.\n");
  return NULL;
}

/*
 * Checks the unstable_data_buffer for messages that have received their
 * global sequence number and match the expected_global_seq.
 * Performs a final causal check before moving to the delivery queue.
 * If found and causally ready, moves them to the final message_buffer.
 * Continues checking until the next expected message is not found/ready
 * or the final message buffer is full.
 */
static void try_deliver_totally_ordered(group_t *target_group) {
  printf("[DELIVER %s] >>> try_deliver_totally_ordered CALLED (Current ExpectedGSeq=%lu)\n",
         target_group->my_id_in_group, (unsigned long)target_group->expected_global_seq);
  fflush(stdout);

  int delivered_something = 0;
  int check_again = 1;

  while (check_again) {
    check_again = 0;
    UnstableDataMsg *deliverable_msg_ptr = NULL;
    int deliverable_msg_index = -1;
    uint64_t current_expected_gseq = target_group->expected_global_seq; // Read volatile value once per loop

    printf("[DELIVER %s]  >>> Inside while loop, searching for GSeq=%lu\n", target_group->my_id_in_group,
           (unsigned long)current_expected_gseq);
    fflush(stdout);

    // Lock unstable buffer ONLY to find the message pointer/index
    pthread_mutex_lock(&target_group->unstable_mutex);
    printf("[DELIVER %s]  Locked unstable_mutex (for search).\n", target_group->my_id_in_group);
    fflush(stdout);

    if (target_group->unstable_count == 0) {
      printf("[DELIVER %s]  Unstable buffer empty, nothing to search.\n", target_group->my_id_in_group);
      fflush(stdout);
      pthread_mutex_unlock(&target_group->unstable_mutex);
      break; // Exit while loop
    }

    // Find the message with the expected global sequence number
    for (int i = 0; i < UNSTABLE_BUFFER_SIZE; i++) {
      if (target_group->unstable_data_buffer[i].active &&
          target_group->unstable_data_buffer[i].global_sequence_number == current_expected_gseq) {
        deliverable_msg_ptr = &target_group->unstable_data_buffer[i];
        deliverable_msg_index = i;
        printf("[DELIVER %s]  >>> Found potential message at unstable_idx=%d: Sender=%s, LocalSeq=%u, GlobalSeq=%lu, "
               "data_missing=%d\n",
               target_group->my_id_in_group, i, deliverable_msg_ptr->sender_id, deliverable_msg_ptr->sequence_number,
               (unsigned long)deliverable_msg_ptr->global_sequence_number, deliverable_msg_ptr->data_missing);
        fflush(stdout);
        break; // Found the one we are looking for
      }
    }
    // Keep pointer & index, but we might need to unlock/relock later

    if (deliverable_msg_ptr != NULL) {
      // Check data_missing flag while unstable_mutex is held
      int is_placeholder = deliverable_msg_ptr->data_missing;

      if (is_placeholder) {
        printf("[DELIVER %s]  >>> Entry at unstable_idx=%d IS a PLACEHOLDER (data_missing=1). Waiting for data.\n",
               target_group->my_id_in_group, deliverable_msg_index);
        fflush(stdout);
        check_again = 0;                                     // Stop checking
        pthread_mutex_unlock(&target_group->unstable_mutex); // Unlock unstable
        printf("[DELIVER %s]  Unlocked unstable_mutex (placeholder found).\n", target_group->my_id_in_group);
        fflush(stdout);
      } else {
        // Data is present. Need group_data_mutex for causal check.
        // Unlock unstable briefly to allow consistent locking order.
        pthread_mutex_unlock(&target_group->unstable_mutex);
        printf("[DELIVER %s]  Unlocked unstable_mutex (before final causal check).\n", target_group->my_id_in_group);
        fflush(stdout);

        pthread_mutex_lock(&group_data_mutex); // Lock group data FIRST
        printf("[DELIVER %s]  Locked group_data_mutex (for final causal check).\n", target_group->my_id_in_group);
        fflush(stdout);

        // Re-lock unstable to re-verify message state and perform check/delivery atomically
        pthread_mutex_lock(&target_group->unstable_mutex);
        printf("[DELIVER %s]  Re-Locked unstable_mutex (for final causal check/delivery).\n",
               target_group->my_id_in_group);
        fflush(stdout);

        // Re-validate pointer and index, and that it's still active and correct GSeq and has data
        // (State could have changed slightly, though unlikely for this specific message)
        if (deliverable_msg_index != -1 && // Index must be valid
            target_group->unstable_data_buffer[deliverable_msg_index].active &&
            target_group->unstable_data_buffer[deliverable_msg_index].global_sequence_number == current_expected_gseq &&
            target_group->unstable_data_buffer[deliverable_msg_index].data_missing == 0) {
          deliverable_msg_ptr =
              &target_group->unstable_data_buffer[deliverable_msg_index]; // Re-get pointer just in case
          printf("[DELIVER %s]  >>> Re-validated entry at unstable_idx=%d has data. Performing FINAL causal check...\n",
                 target_group->my_id_in_group, deliverable_msg_index);
          fflush(stdout);

          // *** Perform FINAL Causal Check ***
          // Both group_data_mutex and unstable_mutex are HELD now
          int final_causally_ready = 1;
          int sender_idx = -1;
          int current_member_count = target_group->member_count;
          for (int i = 0; i < current_member_count; ++i) {
            if (strcmp(target_group->members[i].member_id, deliverable_msg_ptr->sender_id) == 0) {
              sender_idx = i;
              break;
            }
          }
          if (sender_idx != -1) {
            printf("     [DELIVER %s] Comparing Message VC vs Local VC (SenderIdx=%d):\n", target_group->my_id_in_group,
                   sender_idx);
            fflush(stdout);
            printf("         Msg VC: [");
            for (int k = 0; k < current_member_count; k++)
              printf("%d ", deliverable_msg_ptr->received_vector_clock[k]);
            printf("]\n");
            fflush(stdout);
            printf("         Local VC: [");
            for (int k = 0; k < current_member_count; k++)
              printf("%d ", target_group->vector_clock[k]);
            printf("]\n");
            fflush(stdout);
            for (int k = 0; k < current_member_count; k++) {
              if (k < MAX_MEMBERS) {
                if (k == sender_idx)
                  continue;
                if (deliverable_msg_ptr->received_vector_clock[k] > target_group->vector_clock[k]) {
                  final_causally_ready = 0;
                  printf(
                      "     [DELIVER %s] FINAL Causal Check FAIL: GSeq=%lu, Index=%d, MsgVC[%d]=%d > LocalVC[%d]=%d\n",
                      target_group->my_id_in_group, (unsigned long)current_expected_gseq, k, k,
                      deliverable_msg_ptr->received_vector_clock[k], k, target_group->vector_clock[k]);
                  fflush(stdout);
                  break;
                }
              }
            }
          } else {
            printf("     [DELIVER %s] Sender '%s' for GSeq=%lu not found in current members. Skipping final causal "
                   "check (delivering based on total order agreement).\n",
                   target_group->my_id_in_group, deliverable_msg_ptr->sender_id, (unsigned long)current_expected_gseq);
            fflush(stdout);
            final_causally_ready = 1;
          }

          if (final_causally_ready) {
            printf("[DELIVER %s]  >>> FINAL Causal Check PASS for GSeq=%lu.\n", target_group->my_id_in_group,
                   (unsigned long)current_expected_gseq);
            fflush(stdout);

            pthread_mutex_lock(&target_group->msg_mutex); // Lock final queue

            // *** NEW: Check and Increment expected_recv_seq if applicable ***
            if (sender_idx != -1) { // Only if sender is still known
              // Check if this message's local sequence number is the one we were expecting locally
              if (deliverable_msg_ptr->sequence_number == target_group->members[sender_idx].expected_recv_seq) {
                printf("      [DELIVER %s] Updating local sequence: Incrementing expected_recv_seq for sender %s "
                       "('%s') from %u to %u.\n",
                       target_group->my_id_in_group, deliverable_msg_ptr->sender_id,
                       target_group->members[sender_idx].member_id, // Confirming ID matches index
                       target_group->members[sender_idx].expected_recv_seq,
                       target_group->members[sender_idx].expected_recv_seq + 1);
                fflush(stdout);
                target_group->members[sender_idx].expected_recv_seq++;
              } else {
                printf("      [DELIVER %s] Local sequence check: Delivered LocalSeq=%u != ExpectedRecvSeq=%u for "
                       "sender %s. No increment needed here.\n",
                       target_group->my_id_in_group, deliverable_msg_ptr->sequence_number,
                       target_group->members[sender_idx].expected_recv_seq, deliverable_msg_ptr->sender_id);
                fflush(stdout);
              }
            }
            // *** END NEW ***

            if (target_group->msg_count < GROUP_MSG_QUEUE_SIZE) {
              printf("[DELIVER %s]      Moving GSeq=%lu to final delivery queue (current count %d).\n",
                     target_group->my_id_in_group, (unsigned long)current_expected_gseq, target_group->msg_count);
              fflush(stdout);
              GroupQueueItem *deliver_slot = &target_group->message_buffer[target_group->msg_tail];
              // Copy data
              deliver_slot->internal_type = MSG_BUFFER_TYPE_DATA;
              if (deliverable_msg_ptr->len >= 0 && deliverable_msg_ptr->len <= UDP_BUFFER_SIZE) {
                memcpy(deliver_slot->data, deliverable_msg_ptr->data, deliverable_msg_ptr->len);
                deliver_slot->len = deliverable_msg_ptr->len;
              } else {
                fprintf(
                    stderr,
                    " [DELIVER %s] WARNING: Invalid length %d in unstable message GSeq=%lu during delivery attempt.\n",
                    target_group->my_id_in_group, deliverable_msg_ptr->len, (unsigned long)current_expected_gseq);
                deliver_slot->len = 0;
              }
              deliver_slot->sender_addr = deliverable_msg_ptr->sender_addr;
              deliver_slot->sequence_number = deliverable_msg_ptr->sequence_number;
              strncpy(deliver_slot->sender_id, deliverable_msg_ptr->sender_id, MAX_ID_LEN - 1);
              deliver_slot->sender_id[MAX_ID_LEN - 1] = '\0';
              memcpy(deliver_slot->received_vector_clock, deliverable_msg_ptr->received_vector_clock,
                     sizeof(deliverable_msg_ptr->received_vector_clock));
              deliver_slot->global_sequence_number = deliverable_msg_ptr->global_sequence_number;
              // Update final queue state
              target_group->msg_tail = (target_group->msg_tail + 1) % GROUP_MSG_QUEUE_SIZE;
              target_group->msg_count++;
              delivered_something = 1;

              // Update local state (Expected GSeq and VC) - group_data_mutex IS HELD
              printf("[DELIVER %s]      Updating state: Incrementing ExpectedGSeq from %lu to %lu.\n",
                     target_group->my_id_in_group, (unsigned long)target_group->expected_global_seq,
                     (unsigned long)target_group->expected_global_seq + 1);
              fflush(stdout);
              target_group->expected_global_seq++;

              printf("[DELIVER %s]      Updating Local VC based on delivered message GSeq=%lu.\n",
                     target_group->my_id_in_group, (unsigned long)current_expected_gseq);
              fflush(stdout);
              printf("         Local VC Before: [");
              for (int k = 0; k < current_member_count; k++)
                printf("%d ", target_group->vector_clock[k]);
              printf("]\n");
              fflush(stdout);
              printf("         Msg VC:        [");
              for (int k = 0; k < current_member_count; k++)
                printf("%d ", deliver_slot->received_vector_clock[k]);
              printf("]\n");
              fflush(stdout);
              for (int k = 0; k < current_member_count; k++) {
                if (deliver_slot->received_vector_clock[k] > target_group->vector_clock[k]) {
                  target_group->vector_clock[k] = deliver_slot->received_vector_clock[k];
                }
              }
              printf("         Local VC After:  [");
              for (int k = 0; k < current_member_count; k++)
                printf("%d ", target_group->vector_clock[k]);
              printf("]\n");
              fflush(stdout);

              // Mark as inactive in unstable buffer (unstable_mutex IS HELD)
              deliverable_msg_ptr->active = 0;
              target_group->unstable_count--;
              printf("[DELIVER %s]      Removed GSeq=%lu from unstable buffer (Index=%d). New unstable count: %d.\n",
                     target_group->my_id_in_group, (unsigned long)current_expected_gseq, deliverable_msg_index,
                     target_group->unstable_count);
              fflush(stdout);

              pthread_mutex_unlock(&target_group->msg_mutex); // Unlock msg queue
              check_again = 1;                                // Loop again for next GSeq
              printf("[DELIVER %s]    >>> Delivered GSeq=%lu. Looping again to check for GSeq=%lu.\n",
                     target_group->my_id_in_group, (unsigned long)current_expected_gseq,
                     (unsigned long)target_group->expected_global_seq);
              fflush(stdout);

            } else { /* Final queue full */
              fprintf(stderr, "[DELIVER %s] ERROR: Final message buffer full, cannot deliver GlobalSeq=%lu\n",
                      target_group->my_id_in_group, (unsigned long)current_expected_gseq);
              fflush(stderr);
              pthread_mutex_unlock(&target_group->msg_mutex);
              deliverable_msg_ptr->active = 0;
              target_group->unstable_count--; // Remove from unstable anyway
              check_again = 0;                // Stop
            }
          } else { // Failed final causal check
            printf("[DELIVER %s]  >>> FINAL Causal Check FAILED for GSeq=%lu. Delivery delayed.\n",
                   target_group->my_id_in_group, (unsigned long)current_expected_gseq);
            fflush(stdout);
            check_again = 0; // Stop
          }
        } else {
          // Message was found initially, but changed state (e.g. became inactive) after re-locking.
          printf("[DELIVER %s]  >>> Message at unstable_idx=%d changed state after re-locking. Retrying outer loop.\n",
                 target_group->my_id_in_group, deliverable_msg_index);
          fflush(stdout);
          check_again = 1; // Let the outer loop retry finding the GSeq
        }
        // Unlock both mutexes acquired in this block
        pthread_mutex_unlock(&target_group->unstable_mutex);
        pthread_mutex_unlock(&group_data_mutex);
        printf("[DELIVER %s]  Unlocked unstable & group_data mutexes (after check/delivery block).\n",
               target_group->my_id_in_group);
        fflush(stdout);
      } // End else (data is present block)
    } else { // Message not found in initial search
      printf("[DELIVER %s]  >>> Message for expected GSeq=%lu NOT FOUND in unstable buffer.\n",
             target_group->my_id_in_group, (unsigned long)current_expected_gseq);
      fflush(stdout);
      check_again = 0; // Stop
      // Unlock the mutexes acquired at the start of the loop
      pthread_mutex_unlock(&target_group->unstable_mutex);
      pthread_mutex_unlock(&group_data_mutex);
      printf("[DELIVER %s]  Unlocked unstable & group_data mutexes (message not found).\n",
             target_group->my_id_in_group);
      fflush(stdout);
    }
  } // end while(check_again)

  if (delivered_something) {
    printf("[DELIVER %s] >>> Delivered messages, signaling msg_cond.\n", target_group->my_id_in_group);
    fflush(stdout);
    pthread_cond_signal(&target_group->msg_cond);
  } else {
    printf("[DELIVER %s] >>> No messages delivered in this call.\n", target_group->my_id_in_group);
    fflush(stdout);
  }
  printf("[DELIVER %s] <<< try_deliver_totally_ordered EXITING\n", target_group->my_id_in_group);
  fflush(stdout);
}

// Function: handle_ack_message (Modified with Target Check)

static void handle_ack_message(group_t *target_group, uint32_t ack_sequence_num, const char *sender_id,
                               uint8_t acked_msg_type) {
  printf(" UDP Recv Helper: Handling ACK LocalSeq=%u from %s for group '%s' (Acked Type: %u)\n", ack_sequence_num,
         sender_id, target_group->group_name, acked_msg_type);
  fflush(stdout); // Ensure log visibility

  pthread_mutex_lock(&target_group->pending_ack_mutex);
  int found_pending_entry = -1;
  PendingAckInfo *p_ack = NULL; // Initialize p_ack

  // Find the pending entry matching the acknowledged local sequence number AND type
  for (int i = 0; i < PENDING_ACKS_SIZE; i++) {
    if (target_group->pending_acks[i].active && target_group->pending_acks[i].seq_no == ack_sequence_num &&
        target_group->pending_acks[i].original_msg_type == acked_msg_type) // Check type!
    {
      p_ack = &target_group->pending_acks[i]; // Get pointer to entry
      found_pending_entry = i;
      break;
    }
  }

  if (found_pending_entry != -1 && p_ack != NULL) { // Check if p_ack is valid

    // *** NEW CHECK: Verify sender was an original target ***
    int was_target = 0;
    if (p_ack->target_member_ids != NULL) { // Check if target list exists
      for (int j = 0; j < p_ack->target_member_count; j++) {
        if (p_ack->target_member_ids[j] != NULL && // Check individual ID pointer
            strcmp(p_ack->target_member_ids[j], sender_id) == 0) {
          was_target = 1;
          break;
        }
      }
    }

    if (!was_target) {
      fprintf(stderr,
              "  -> WARNING: Received ACK for LocalSeq=%u Type=%u from %s who was NOT an original target or target "
              "list invalid. Ignoring.\n",
              p_ack->seq_no, acked_msg_type, sender_id);
      fflush(stderr);
    } else {
      // Sender was a target, proceed with normal ACK processing
      int already_acked = 0;
      if (p_ack->acked_member_ids != NULL) { // Check if acked list exists
        for (int j = 0; j < p_ack->acks_received_count; j++) {
          if (p_ack->acked_member_ids[j] != NULL && // Check individual ID pointer
              strcmp(p_ack->acked_member_ids[j], sender_id) == 0) {
            already_acked = 1;
            printf("  -> Duplicate ACK detected for LocalSeq=%u Type=%u from %s.\n", p_ack->seq_no, acked_msg_type,
                   sender_id); // Log duplicates
            fflush(stdout);
            break;
          }
        }
      }

      if (!already_acked) {
        // Realloc logic (kept for now)
        if (p_ack->acked_member_ids == NULL || p_ack->acks_received_count >= p_ack->acked_member_cap) {
          int new_cap = (p_ack->acked_member_cap == 0) ? INITIAL_MEMBER_LIST_CAPACITY : p_ack->acked_member_cap * 2;
          printf("  -> Reallocating acked_member_ids for LocalSeq=%u (Type %u) from %d to %d\n", p_ack->seq_no,
                 acked_msg_type, p_ack->acked_member_cap, new_cap);
          fflush(stdout);
          char **temp_list = realloc(p_ack->acked_member_ids, new_cap * sizeof(char *));
          if (!temp_list) {
            perror("handle_ack_message: realloc acked_member_ids failed");
            fflush(stderr);
          } else {
            p_ack->acked_member_ids = temp_list;
            for (int k = p_ack->acked_member_cap; k < new_cap; ++k) {
              p_ack->acked_member_ids[k] = NULL;
            }
            p_ack->acked_member_cap = new_cap;
          }
        }

        // Add the ID if capacity allows and list is valid
        if (p_ack->acked_member_ids != NULL && p_ack->acks_received_count < p_ack->acked_member_cap) {
          p_ack->acked_member_ids[p_ack->acks_received_count] = strdup(sender_id);
          if (!p_ack->acked_member_ids[p_ack->acks_received_count]) {
            perror("handle_ack_message: strdup failed for acked member id");
            fflush(stderr);
          } else {
            p_ack->acks_received_count++; // Increment count ONLY if added successfully
            printf("  -> ACK count for LocalSeq=%u Type=%u is now %d/%d\n", p_ack->seq_no, acked_msg_type,
                   p_ack->acks_received_count, p_ack->target_member_count);
            fflush(stdout);
          }
        } else {
          fprintf(stderr,
                  "  -> Could not add sender %s to acked list for LocalSeq=%u (Type %u) (capacity %d reached or list "
                  "NULL)\n",
                  sender_id, ack_sequence_num, acked_msg_type, p_ack->acked_member_cap);
          fflush(stderr);
        }

        // Check if all expected ACKs have been received
        if (p_ack->target_member_count > 0 && p_ack->acks_received_count >= p_ack->target_member_count) {
          printf("  -> All ACKs received for LocalSeq=%u Type=%u in group '%s'. Clearing entry.\n", p_ack->seq_no,
                 acked_msg_type, target_group->group_name);
          fflush(stdout);
          clear_pending_ack_entry(p_ack); // Clean up the entry
          p_ack = NULL;                   // Avoid use-after-free
        }
      } // end if !already_acked
    } // end if was_target
  } else {
    printf("  -> Received ACK for unknown/inactive LocalSeq=%u Type=%u from %s.\n", ack_sequence_num, acked_msg_type,
           sender_id);
    fflush(stdout);
  }
  pthread_mutex_unlock(&target_group->pending_ack_mutex);
}

/*
 * Handles incoming DATA_UNSTABLE messages (executed by all members).
 * Sends ACK (only if expected/duplicate), checks reliability & causality.
 * If OK:
 * - Checks if a placeholder exists (SA arrived first). If yes, fills it.
 * - If no placeholder, buffers in a new unstable slot (marking GSeq as UNASSIGNED).
 * - Updates state (expected seq, VC), checks OOO buffer, tries to deliver.
 * If not OK: Buffers in ooo_buffer or discards duplicate.
 */
static void handle_unstable_data(group_t *target_group, uint32_t sequence_num, const char *sender_id,
                                 const int *received_vector_clock, const unsigned char *payload_ptr, int payload_len,
                                 const struct sockaddr_in *sender_addr) {

  // --- ACK Sending Logic MOVED to after validation ---

  pthread_mutex_lock(&group_data_mutex); // Lock for accessing member list and VCs
  char my_id[MAX_ID_LEN + 1] = {0};      // Initialize for safety
  int member_count_now = 0;
  int sender_index = -1;
  int send_ack = 0; // Flag to indicate if ACK should be sent

  if (!target_group->is_active) { // Check if group became inactive between recv and handling
    pthread_mutex_unlock(&group_data_mutex);
    printf(" [h_unstable] << Group '%s' inactive, ignoring DATA_UNSTABLE LocalSeq=%u.\n", target_group->group_name,
           sequence_num);
    fflush(stdout);
    return;
  }

  // Copy own ID needed for potential ACK sending later
  strncpy(my_id, target_group->my_id_in_group, MAX_ID_LEN);
  my_id[MAX_ID_LEN] = '\0';
  member_count_now = target_group->member_count; // Need member count for VC checks later

  // Find sender index
  for (int i = 0; i < target_group->member_count; ++i) {
    if (strcmp(target_group->members[i].member_id, sender_id) == 0) {
      sender_index = i;
      break;
    }
  }

  // Basic logging of entry
  printf(" [h_unstable %s] >> Handling DATA_UNSTABLE LocalSeq=%u from %s (PayloadLen=%d)\n",
         target_group->my_id_in_group, sequence_num, sender_id, payload_len);
  fflush(stdout);

  // Only proceed if the sender is a known member of the group
  if (sender_index != -1) {
    member_t *sender_info = &target_group->members[sender_index];
    int causally_ready = 0;
    int buffer_action = 0;              // 0=Discard, 1=Buffer Unstable, 2=Buffer OOO (Reliability)
    int update_state_and_check_ooo = 0; // Flag to update state and check OOO

    // --- Reliability Check (Sequence Number) ---
    if (sequence_num == sender_info->expected_recv_seq) {
      send_ack = 1; // *** OK to send ACK for expected message ***

      // --- Causality Check ---
      causally_ready = 1;
      printf("  [h_unstable] Reliability OK (LocalSeq=%u). Checking causality...\n", sequence_num);
      fflush(stdout);
      printf("     Received VC: [");
      for (int k = 0; k < member_count_now; k++)
        printf("%d ", received_vector_clock[k]);
      printf("]\n");
      fflush(stdout);
      printf("     Local VC:    [");
      for (int k = 0; k < member_count_now; k++)
        printf("%d ", target_group->vector_clock[k]);
      printf("]\n");
      fflush(stdout);

      for (int k = 0; k < member_count_now; k++) {
        if (k < MAX_MEMBERS) { // Bounds check
          if (k == sender_index)
            continue;
          if (received_vector_clock[k] > target_group->vector_clock[k]) {
            causally_ready = 0;
            printf("  [h_unstable] Causality FAIL at index %d: ReceivedVC[%d]=%d > LocalVC[%d]=%d\n", k, k,
                   received_vector_clock[k], k, target_group->vector_clock[k]);
            fflush(stdout);
            break;
          }
        }
      }

      if (causally_ready) {
        printf("  [h_unstable] Causality OK for LocalSeq=%u.\n", sequence_num);
        fflush(stdout);
        buffer_action = 1;              // Buffer in unstable
        update_state_and_check_ooo = 1; // OK to update state after buffering
      } else {
        printf("  [h_unstable] Causality Pending for LocalSeq=%u. Will buffer unstable.\n", sequence_num);
        fflush(stdout);
        buffer_action = 1; // Buffer in unstable, but don't update state yet
      }
    } else if (sequence_num < sender_info->expected_recv_seq) {
      // Duplicate - discard
      printf("  [h_unstable] Discarding duplicate DATA_UNSTABLE (LocalSeq=%u < Expected=%u) from %s\n", sequence_num,
             sender_info->expected_recv_seq, sender_id);
      fflush(stdout);
      buffer_action = 0; // Discard
      send_ack = 1;      // *** Still ACK duplicates to let sender clear pending entry ***
    } else {             // sequence_num > sender_info->expected_recv_seq
      // Out-of-order (reliability) -> Buffer in OOO
      printf("  [h_unstable %s] Received out-of-order DATA_UNSTABLE (LocalSeq=%u > Expected=%u) from %s. Trying OOO "
             "buffer...\n",
             target_group->my_id_in_group, sequence_num, sender_info->expected_recv_seq, sender_id);
      fflush(stdout);
      buffer_action = 2; // Buffer OOO
      send_ack = 0;      // *** Do NOT ACK out-of-order messages immediately ***
    }
    // Note: group_data_mutex is HELD at this point

    // --- Perform Buffering action ---
    int buffered_unstable_successfully = 0;

    if (buffer_action == 1) { // Buffer in unstable buffer
      pthread_mutex_lock(&target_group->unstable_mutex);
      int target_slot = -1;
      int is_placeholder = 0;
      // Check for existing placeholder first
      for (int k = 0; k < UNSTABLE_BUFFER_SIZE; ++k) {
        if (target_group->unstable_data_buffer[k].active && target_group->unstable_data_buffer[k].data_missing == 1 &&
            strcmp(target_group->unstable_data_buffer[k].sender_id, sender_id) == 0 &&
            target_group->unstable_data_buffer[k].sequence_number == sequence_num) {
          target_slot = k;
          is_placeholder = 1;
          printf("  [h_unstable] Found existing placeholder at unstable_idx=%d for LocalSeq=%u.\n", target_slot,
                 sequence_num);
          fflush(stdout);
          break;
        }
      }
      // If no placeholder found, find a new inactive slot
      if (target_slot == -1) {
        for (int k = 0; k < UNSTABLE_BUFFER_SIZE; ++k) {
          if (!target_group->unstable_data_buffer[k].active) {
            target_slot = k;
            break;
          }
        }
      }

      if (target_slot != -1) {
        if (is_placeholder || target_group->unstable_count < UNSTABLE_BUFFER_SIZE) {
          UnstableDataMsg *unstable_item = &target_group->unstable_data_buffer[target_slot];
          if (payload_len >= 0 && payload_len <= UDP_BUFFER_SIZE) {
            memcpy(unstable_item->data, payload_ptr, payload_len);
            unstable_item->len = payload_len;
            unstable_item->sender_addr = *sender_addr;
            memcpy(unstable_item->received_vector_clock, received_vector_clock,
                   sizeof(unstable_item->received_vector_clock));
            if (is_placeholder) {
              unstable_item->data_missing = 0;
              printf("  [h_unstable] Filled placeholder at index %d. GSeq=%lu.\n", target_slot,
                     (unsigned long)unstable_item->global_sequence_number);
              fflush(stdout);
            } else { // New entry
              unstable_item->active = 1;
              unstable_item->data_missing = 0;
              strncpy(unstable_item->sender_id, sender_id, MAX_ID_LEN - 1);
              unstable_item->sender_id[MAX_ID_LEN - 1] = '\0';
              unstable_item->sequence_number = sequence_num;
              unstable_item->global_sequence_number = UNASSIGNED_GLOBAL_SEQ;
              target_group->unstable_count++;
              printf("  [h_unstable] Buffered new LocalSeq=%u into unstable buffer at index %d. Unstable count: %d\n",
                     sequence_num, target_slot, target_group->unstable_count);
              fflush(stdout);
            }
            buffered_unstable_successfully = 1;
          } else {
            fprintf(stderr, " [h_unstable] ERROR: Invalid payload length (%d) for unstable msg LocalSeq=%u.\n",
                    payload_len, sequence_num);
            fflush(stderr);
            if (!is_placeholder && target_slot != -1)
              target_group->unstable_data_buffer[target_slot].active = 0;
          }
        } else {
          fprintf(stderr, " [h_unstable] ERROR: Unstable buffer full (count check)! Cannot buffer LocalSeq=%u\n",
                  sequence_num);
          fflush(stderr);
        }
      } else {
        fprintf(stderr, " [h_unstable] ERROR: Unstable buffer full (no free slots)! Cannot buffer LocalSeq=%u\n",
                sequence_num);
        fflush(stderr);
        update_state_and_check_ooo = 0;
      }
      pthread_mutex_unlock(&target_group->unstable_mutex);

    } else if (buffer_action == 2) { // Buffer in OOO (Reliability)
      int already_buffered = 0;
      for (int k = 0; k < OUT_OF_ORDER_BUFFER_SIZE; ++k) {
        if (sender_info->ooo_buffer[k].active && sender_info->ooo_buffer[k].sequence_number == sequence_num &&
            strcmp(sender_info->ooo_buffer[k].sender_id, sender_id) == 0) {
          already_buffered = 1;
          printf("  [h_unstable] OOO LocalSeq=%u already buffered.\n", sequence_num);
          fflush(stdout);
          break;
        }
      }
      if (!already_buffered) {
        if (sender_info->ooo_count < OUT_OF_ORDER_BUFFER_SIZE) {
          int ooo_slot = -1;
          for (int k = 0; k < OUT_OF_ORDER_BUFFER_SIZE; ++k) {
            if (!sender_info->ooo_buffer[k].active) {
              ooo_slot = k;
              break;
            }
          }
          if (ooo_slot != -1) {
            OutOfOrderMsg *ooo_msg = &sender_info->ooo_buffer[ooo_slot];
            if (payload_len >= 0 && payload_len <= UDP_BUFFER_SIZE) {
              memcpy(ooo_msg->data, payload_ptr, payload_len);
              ooo_msg->len = payload_len;
              ooo_msg->sender_addr = *sender_addr;
              ooo_msg->sequence_number = sequence_num;
              strncpy(ooo_msg->sender_id, sender_id, MAX_ID_LEN - 1);
              ooo_msg->sender_id[MAX_ID_LEN - 1] = '\0';
              memcpy(ooo_msg->received_vector_clock, received_vector_clock, sizeof(ooo_msg->received_vector_clock));
              ooo_msg->original_msg_type = TYPE_DATA_UNSTABLE;
              ooo_msg->active = 1;
              sender_info->ooo_count++;
              printf("  [h_unstable %s] Buffered OOO LocalSeq=%u into ooo_buffer[%d] for sender %s. OOO count: %d\n",
                     target_group->my_id_in_group, sequence_num, ooo_slot, sender_id, sender_info->ooo_count);
              fflush(stdout);
            } else {
              fprintf(stderr, " [h_unstable] ERROR: Invalid payload len %d for OOO buffer.\n", payload_len);
              fflush(stderr);
            }
          } else {
            fprintf(stderr, " [h_unstable] ERROR: OOO buffer count inconsistency for sender %s.\n", sender_id);
            fflush(stderr);
          }
        } else {
          fprintf(stderr, " [h_unstable] ERROR: OOO buffer for %s is full! Cannot buffer LocalSeq=%u\n", sender_id,
                  sequence_num);
          fflush(stderr);
        }
      }
    }
    // Else (buffer_action == 0): Discard duplicate

    // --- Update State & Check OOO Buffer (if applicable) ---
    if (update_state_and_check_ooo && buffered_unstable_successfully) {
      printf("  [h_unstable %s] State Update Block Entered for LocalSeq=%u from %s.\n", target_group->my_id_in_group,
             sequence_num, sender_id);
      fflush(stdout);
      sender_info->expected_recv_seq++;
      printf("    %s -> Incremented expected_recv_seq for %s to %u.\n", target_group->my_id_in_group, sender_id,
             sender_info->expected_recv_seq);
      fflush(stdout);
      printf("     -> Merging VC. Local VC before: [");
      for (int k = 0; k < member_count_now; k++)
        printf("%d ", target_group->vector_clock[k]);
      printf("]\n");
      fflush(stdout);
      for (int k = 0; k < member_count_now; k++) {
        if (k < MAX_MEMBERS && received_vector_clock[k] > target_group->vector_clock[k]) {
          target_group->vector_clock[k] = received_vector_clock[k];
        }
      }
      printf("     -> Local VC after merge:  [");
      for (int k = 0; k < member_count_now; k++)
        printf("%d ", target_group->vector_clock[k]);
      printf("]\n");
      fflush(stdout);
      printf("     -> Checking OOO buffer for %s (count: %d) for newly ready messages (expecting %u)...\n", sender_id,
             sender_info->ooo_count, sender_info->expected_recv_seq);
      fflush(stdout);

      // --- Check OOO Buffer Logic (Complete) ---
      int processed_ooo_in_loop = 1;
      while (processed_ooo_in_loop && sender_info->ooo_count > 0) {
        processed_ooo_in_loop = 0;
        int found_ooo_idx = -1;
        OutOfOrderMsg *ooo_msg_to_process = NULL;
        for (int k = 0; k < OUT_OF_ORDER_BUFFER_SIZE; ++k) {
          if (sender_info->ooo_buffer[k].active &&
              sender_info->ooo_buffer[k].sequence_number == sender_info->expected_recv_seq) {
            int ooo_causally_ready = 1;
            printf("        [OOO Check] Found OOO msg at index %d (Seq=%u). Checking causality...\n", k,
                   sender_info->expected_recv_seq);
            fflush(stdout);
            printf("            OOO VC: [");
            for (int vc_k = 0; vc_k < member_count_now; vc_k++)
              printf("%d ", sender_info->ooo_buffer[k].received_vector_clock[vc_k]);
            printf("]\n");
            fflush(stdout);
            printf("            Local VC: [");
            for (int vc_k = 0; vc_k < member_count_now; vc_k++)
              printf("%d ", target_group->vector_clock[vc_k]);
            printf("]\n");
            fflush(stdout);
            for (int vc_k = 0; vc_k < member_count_now; vc_k++) {
              if (vc_k < MAX_MEMBERS) {
                if (vc_k == sender_index)
                  continue;
                if (sender_info->ooo_buffer[k].received_vector_clock[vc_k] > target_group->vector_clock[vc_k]) {
                  ooo_causally_ready = 0;
                  printf("        [OOO Check] Causality FAIL at index %d: OOO_VC[%d]=%d > Local_VC[%d]=%d.\n", k, vc_k,
                         sender_info->ooo_buffer[k].received_vector_clock[vc_k], vc_k,
                         target_group->vector_clock[vc_k]);
                  fflush(stdout);
                  break;
                }
              }
            }
            if (ooo_causally_ready) {
              printf("        [OOO Check] Causality PASS for OOO msg at index %d (Seq=%u).\n", k,
                     sender_info->expected_recv_seq);
              fflush(stdout);
              found_ooo_idx = k;
              ooo_msg_to_process = &sender_info->ooo_buffer[k];
              break;
            } else {
              printf("        [OOO Check] Causality FAIL for OOO msg at index %d (Seq=%u). Leaving in OOO buffer.\n", k,
                     sender_info->expected_recv_seq);
              fflush(stdout);
              processed_ooo_in_loop = 0;
              break;
            }
          }
        }
        if (found_ooo_idx != -1 && ooo_msg_to_process) {
          printf("        [OOO Check] Processing ready OOO message (Index=%d, Seq=%u).\n", found_ooo_idx,
                 ooo_msg_to_process->sequence_number);
          fflush(stdout);
          int ooo_moved_to_unstable = 0;
          pthread_mutex_lock(&target_group->unstable_mutex);
          int target_unstable_slot = -1;
          int placeholder_unstable_idx = -1;
          for (int ph_idx = 0; ph_idx < UNSTABLE_BUFFER_SIZE; ++ph_idx) {
            if (target_group->unstable_data_buffer[ph_idx].active &&
                target_group->unstable_data_buffer[ph_idx].data_missing == 1 &&
                strcmp(target_group->unstable_data_buffer[ph_idx].sender_id, ooo_msg_to_process->sender_id) == 0 &&
                target_group->unstable_data_buffer[ph_idx].sequence_number == ooo_msg_to_process->sequence_number) {
              placeholder_unstable_idx = ph_idx;
              printf("            [OOO Check->Unstable] Found placeholder at unstable_idx=%d for OOO msg Seq=%u.\n",
                     placeholder_unstable_idx, ooo_msg_to_process->sequence_number);
              fflush(stdout);
              break;
            }
          }
          if (placeholder_unstable_idx != -1) {
            target_unstable_slot = placeholder_unstable_idx;
          } else {
            for (int k = 0; k < UNSTABLE_BUFFER_SIZE; ++k) {
              if (!target_group->unstable_data_buffer[k].active) {
                target_unstable_slot = k;
                break;
              }
            }
          }
          if (target_unstable_slot != -1) {
            if (placeholder_unstable_idx != -1 || target_group->unstable_count < UNSTABLE_BUFFER_SIZE) {
              UnstableDataMsg *unstable_item = &target_group->unstable_data_buffer[target_unstable_slot];
              memcpy(unstable_item->data, ooo_msg_to_process->data, ooo_msg_to_process->len);
              unstable_item->len = ooo_msg_to_process->len;
              unstable_item->sender_addr = ooo_msg_to_process->sender_addr;
              unstable_item->sequence_number = ooo_msg_to_process->sequence_number;
              strncpy(unstable_item->sender_id, ooo_msg_to_process->sender_id, MAX_ID_LEN - 1);
              unstable_item->sender_id[MAX_ID_LEN - 1] = '\0';
              memcpy(unstable_item->received_vector_clock, ooo_msg_to_process->received_vector_clock,
                     sizeof(ooo_msg_to_process->received_vector_clock));
              if (placeholder_unstable_idx != -1) {
                unstable_item->data_missing = 0;
                printf("            [OOO Check->Unstable] Filled placeholder %d with OOO data (Seq=%u, GSeq=%lu).\n",
                       target_unstable_slot, unstable_item->sequence_number,
                       (unsigned long)unstable_item->global_sequence_number);
                fflush(stdout);
              } else {
                unstable_item->active = 1;
                unstable_item->data_missing = 0;
                unstable_item->global_sequence_number = UNASSIGNED_GLOBAL_SEQ;
                target_group->unstable_count++;
                printf("            [OOO Check->Unstable] Used new slot %d for OOO data (Seq=%u). Count=%d.\n",
                       target_unstable_slot, unstable_item->sequence_number, target_group->unstable_count);
                fflush(stdout);
              }
              ooo_moved_to_unstable = 1;
            } else {
              fprintf(stderr, " [h_unstable] ERROR: OOO Check - Unstable buffer full (count check)!\n");
              fflush(stderr);
            }
          } else {
            fprintf(stderr, " [h_unstable] ERROR: OOO Check - Unstable buffer full (no free slots)!\n");
            fflush(stderr);
          }
          pthread_mutex_unlock(&target_group->unstable_mutex);
          if (ooo_moved_to_unstable) {
            sender_info->expected_recv_seq++;
            printf("        [OOO Check] Incremented expected_recv_seq for %s to %u (after OOO processing).\n",
                   sender_id, sender_info->expected_recv_seq);
            fflush(stdout);
            printf("        [OOO Check] Merging OOO msg VC. Local VC before: [");
            for (int k = 0; k < member_count_now; k++)
              printf("%d ", target_group->vector_clock[k]);
            printf("]\n");
            fflush(stdout);
            for (int k = 0; k < member_count_now; k++) {
              if (k < MAX_MEMBERS && ooo_msg_to_process->received_vector_clock[k] > target_group->vector_clock[k]) {
                target_group->vector_clock[k] = ooo_msg_to_process->received_vector_clock[k];
              }
            }
            printf("        [OOO Check] Local VC after OOO merge: [");
            for (int k = 0; k < member_count_now; k++)
              printf("%d ", target_group->vector_clock[k]);
            printf("]\n");
            fflush(stdout);
            ooo_msg_to_process->active = 0;
            sender_info->ooo_count--;
            printf("        [OOO Check] Deactivated OOO slot %d. OOO count for %s now %d.\n", found_ooo_idx, sender_id,
                   sender_info->ooo_count);
            fflush(stdout);
            processed_ooo_in_loop = 1;
          } else {
            processed_ooo_in_loop = 0;
          }
        } else {
          processed_ooo_in_loop = 0;
          printf("        [OOO Check] No more ready OOO messages found for %s (expecting %u).\n", sender_id,
                 sender_info->expected_recv_seq);
          fflush(stdout);
        }
      } // end while(processed_ooo_in_loop)
      printf("     -> Exited OOO processing loop for %s.\n", sender_id);
      fflush(stdout);
    } // end if (update_state_and_check_ooo && buffered_unstable_successfully)

    // --- Unlock Global Mutex ---
    pthread_mutex_unlock(&group_data_mutex); // Unlock group data AFTER potential state update

    // --- Send ACK **ONLY IF** Flagged (after validation and potential state update) ---
    if (send_ack && strlen(my_id) > 0) {
      size_t ack_header_len =
          sizeof(uint8_t) * 3 + strlen(target_group->group_name) + strlen(my_id) + sizeof(uint8_t) + sizeof(uint32_t);
      if (ack_header_len < UDP_BUFFER_SIZE) {
        unsigned char ack_buffer[ack_header_len];
        unsigned char *ack_ptr = ack_buffer;
        uint8_t type = TYPE_ACK;
        uint8_t gn_len = strlen(target_group->group_name);
        uint8_t my_id_len = strlen(my_id);
        uint32_t ack_seq_net = htonl(sequence_num);
        uint8_t acked_type = ACKED_TYPE_DATA_UNSTABLE;
        memcpy(ack_ptr, &type, sizeof(uint8_t));
        ack_ptr++;
        memcpy(ack_ptr, &gn_len, sizeof(uint8_t));
        ack_ptr++;
        memcpy(ack_ptr, target_group->group_name, gn_len);
        ack_ptr += gn_len;
        memcpy(ack_ptr, &my_id_len, sizeof(uint8_t));
        ack_ptr++;
        memcpy(ack_ptr, my_id, my_id_len);
        ack_ptr += my_id_len;
        memcpy(ack_ptr, &acked_type, sizeof(uint8_t));
        ack_ptr++;
        memcpy(ack_ptr, &ack_seq_net, sizeof(uint32_t));
        sendto(global_udp_socket, ack_buffer, ack_header_len, 0, (struct sockaddr *)sender_addr,
               sizeof(struct sockaddr_in));
        atomic_fetch_add(&proto_ack_sent_count, 1);
        printf("  [h_unstable] Sent ACK for DATA_UNSTABLE (LocalSeq=%u) to sender %s (Reason: Expected or Duplicate)\n",
               sequence_num, sender_id);
        fflush(stdout);
      } else {
        fprintf(stderr, " [h_unstable] ERROR: Cannot send ACK for DATA_UNSTABLE (header too large).\n");
        fflush(stderr);
      }
    } else if (!send_ack) {
      printf("  [h_unstable] Suppressed ACK for Out-of-Order DATA_UNSTABLE (LocalSeq=%u) from sender %s\n",
             sequence_num, sender_id);
      fflush(stdout);
    }

    // --- Try Delivery ---
    if (buffer_action != 0) { // Only attempt delivery if we didn't discard
      printf("  [h_unstable] Calling try_deliver_totally_ordered after processing LocalSeq=%u.\n", sequence_num);
      fflush(stdout);
      try_deliver_totally_ordered(target_group);
    }

  } else { // Sender not found in group
    pthread_mutex_unlock(&group_data_mutex);
    fprintf(stderr, " [h_unstable] WARNING: Received DATA_UNSTABLE from unknown sender '%s'. Discarding.\n", sender_id);
    fflush(stderr);
  }

  printf(" [h_unstable] << Exiting handle_unstable_data for LocalSeq=%u from %s\n", sequence_num, sender_id);
  fflush(stdout);
}

/*
 * Handles incoming SEQ_REQUEST messages (executed only by the sequencer).
 * Assigns the next global sequence number, updates own VC,
 * creates a SEQ_ASSIGNMENT message, adds it to own pending ACKs,
 * and reliably multicasts the SEQ_ASSIGNMENT message to the group.
 */
static void handle_sequencer_request(group_t *target_group, uint32_t original_sequence_num,
                                     const char *original_sender_id, int original_sender_id_len, const char *group_name,
                                     int target_group_handle) {
  printf(" UDP Recv Helper: Sequencer handling SEQ_REQUEST OrigLocalSeq=%u from %s\n", original_sequence_num,
         original_sender_id);

  uint64_t assigned_global_seq;
  int member_count_copy;
  member_t members_copy[MAX_MEMBERS];
  unsigned char *assignment_buffer = NULL;
  size_t assignment_total_len = 0;
  int pending_slot = -1;

  // Lock needed for accessing group state (next_global_seq, VC, members)
  pthread_mutex_lock(&group_data_mutex);

  // Check if group is still active and we are still the sequencer
  if (!target_group->is_active) {
    pthread_mutex_unlock(&group_data_mutex);
    return;
  }
  char *current_sequencer_id_check = get_sequencer_id(target_group);
  if (!current_sequencer_id_check || strcmp(target_group->my_id_in_group, current_sequencer_id_check) != 0) {
    pthread_mutex_unlock(&group_data_mutex);
    if (current_sequencer_id_check)
      if (current_sequencer_id_check)
        free(current_sequencer_id_check);
    return;
  }
  free(current_sequencer_id_check);

  // 1. Assign Global Sequence Number
  assigned_global_seq = target_group->next_global_seq++;

  // 2. Update own Vector Clock (for the sequencing event - optional but good practice)
  // for(int i=0; i<target_group->member_count; ++i){
  // if(strcmp(target_group->members[i].member_id, target_group->my_id_in_group) == 0) {
  // my_member_index = i; break; } } if(my_member_index != -1) {
  // target_group->vector_clock[my_member_index]++; }

  // Copy member list for safe iteration after unlock
  member_count_copy = target_group->member_count;
  memcpy(members_copy, target_group->members, sizeof(member_t) * member_count_copy);

  pthread_mutex_unlock(&group_data_mutex); // Unlock global mutex

  // 3. Create the SEQ_ASSIGNMENT message buffer
  uint8_t new_msg_type = TYPE_SEQ_ASSIGNMENT;
  uint8_t group_name_len_val = strlen(group_name);
  uint8_t sequencer_id_len = strlen(target_group->my_id_in_group); // Sender is the sequencer
  uint8_t orig_sender_id_len_val = original_sender_id_len;
  uint32_t orig_local_seq_net = htonl(original_sequence_num);
  uint64_t global_seq_net = htobe64(assigned_global_seq);

  // Header: Type(1) + GroupLen(1) + Group + SenderLen(1) + Sender(Sequencer) + OrigSenderLen(1) +
  // OrigSender + OrigLocalSeq(4) + GlobalSeq(8)
  assignment_total_len = 1 + 1 + group_name_len_val + 1 + sequencer_id_len + 1 + orig_sender_id_len_val +
                         sizeof(uint32_t) + sizeof(uint64_t);

  if (assignment_total_len <= UDP_BUFFER_SIZE) { // Should always be true for this small message
    assignment_buffer = malloc(assignment_total_len);
    if (assignment_buffer) {
      unsigned char *d_ptr = assignment_buffer;
      // Serialize Header for SEQ_ASSIGNMENT
      memcpy(d_ptr, &new_msg_type, sizeof(uint8_t));
      d_ptr += sizeof(uint8_t);
      memcpy(d_ptr, &group_name_len_val, sizeof(uint8_t));
      d_ptr += sizeof(uint8_t);
      memcpy(d_ptr, group_name, group_name_len_val);
      d_ptr += group_name_len_val;
      memcpy(d_ptr, &sequencer_id_len, sizeof(uint8_t));
      d_ptr += sizeof(uint8_t);
      memcpy(d_ptr, target_group->my_id_in_group, sequencer_id_len);
      d_ptr += sequencer_id_len; // Sequencer's ID
      memcpy(d_ptr, &orig_sender_id_len_val, sizeof(uint8_t));
      d_ptr += sizeof(uint8_t);
      memcpy(d_ptr, original_sender_id, orig_sender_id_len_val);
      d_ptr += orig_sender_id_len_val; // Original sender ID
      memcpy(d_ptr, &orig_local_seq_net, sizeof(uint32_t));
      d_ptr += sizeof(uint32_t); // Original local seq
      memcpy(d_ptr, &global_seq_net, sizeof(uint64_t));
      d_ptr += sizeof(uint64_t); // Global seq no
    } else {
      perror("malloc failed for assignment buffer");
    }
  } else {
    fprintf(stderr, "Sequencer Error: SEQ_ASSIGNMENT message header too large?!\n");
  }

  // 4. Add to sequencer's pending ACKs (if buffer allocated)
  if (assignment_buffer) {
    pthread_mutex_lock(&target_group->pending_ack_mutex);
    pending_slot = -1;
    for (int i = 0; i < PENDING_ACKS_SIZE; ++i) {
      if (!target_group->pending_acks[i].active) {
        pending_slot = i;
        break;
      }
    }
    if (pending_slot == -1) {
      fprintf(stderr, "Sequencer: Pending ACK buffer full!\n");
      free(assignment_buffer);
      assignment_buffer = NULL;
    }
    if (pending_slot != -1) {
      PendingAckInfo *p_ack = &target_group->pending_acks[pending_slot];
      clear_pending_ack_entry(p_ack);
      // Use original sequence number to track ACKs for this assignment
      p_ack->seq_no = original_sequence_num;
      p_ack->active = 1;
      p_ack->sent_time = time(NULL);
      p_ack->message_data = assignment_buffer; // Store the assignment message itself
      p_ack->message_len = assignment_total_len;
      p_ack->original_msg_type = TYPE_SEQ_ASSIGNMENT; // Mark the type
      p_ack->target_member_count = member_count_copy; // From copied state
      p_ack->acks_received_count = 0;
      p_ack->target_member_cap = member_count_copy > 0 ? member_count_copy : 1;
      p_ack->acked_member_cap = INITIAL_MEMBER_LIST_CAPACITY;
      p_ack->target_member_ids = malloc(p_ack->target_member_cap * sizeof(char *));
      p_ack->acked_member_ids = malloc(p_ack->acked_member_cap * sizeof(char *));
      if (!p_ack->target_member_ids || !p_ack->acked_member_ids) {
        clear_pending_ack_entry(p_ack);
        assignment_buffer = NULL;
      } // Frees buffer too
      else {
        for (int k = 0; k < p_ack->target_member_cap; ++k)
          p_ack->target_member_ids[k] = NULL;
        for (int k = 0; k < p_ack->acked_member_cap; ++k)
          p_ack->acked_member_ids[k] = NULL;
        int current_target_count = 0;
        int strdup_failed = 0;
        for (int i = 0; i < member_count_copy; ++i) { // Use the copied member list
          if (current_target_count < p_ack->target_member_cap) {
            p_ack->target_member_ids[current_target_count] = strdup(members_copy[i].member_id);
            if (!p_ack->target_member_ids[current_target_count]) {
              strdup_failed = 1;
              break;
            }
            current_target_count++;
          } else {
            strdup_failed = 1;
            break;
          }
        }
        if (strdup_failed) {
          clear_pending_ack_entry(p_ack);
          assignment_buffer = NULL;
        } else {
          p_ack->target_member_count = current_target_count;
        }
      }
    }
    pthread_mutex_unlock(&target_group->pending_ack_mutex);
  }

  // 5. Reliable Multicast του SEQ_ASSIGNMENT (αν η καταγραφή πέτυχε)
  if (assignment_buffer) {
    printf("Sequencer: Multicasting SEQ_ASSIGNMENT (GlobalSeq=%lu) for OrigSender=%s "
           "OrigLocalSeq=%u...\n",
           (unsigned long)assigned_global_seq, original_sender_id, original_sequence_num);
    int mc_members_sent_to = 0;
    int mc_errors = 0;
    for (int i = 0; i < member_count_copy; i++) {
      member_t *dest = &members_copy[i];
      if (dest->address[0] == '\0' || dest->port <= 0)
        continue;
      struct sockaddr_in dest_addr;
      memset(&dest_addr, 0, sizeof(dest_addr));
      dest_addr.sin_family = AF_INET;
      dest_addr.sin_port = htons(dest->port);
      if (inet_pton(AF_INET, dest->address, &dest_addr.sin_addr) <= 0)
        continue;
      ssize_t sent_bytes = sendto(global_udp_socket, assignment_buffer, assignment_total_len, 0,
                                  (struct sockaddr *)&dest_addr, sizeof(dest_addr));
      atomic_fetch_add(&proto_seq_assignment_sent_count, 1);
      if (sent_bytes < 0) {
        perror("Sequencer assignment multicast sendto failed");
        mc_errors++;
      } else {
        mc_members_sent_to++;
      }
    }
    // assignment_buffer is NOT freed here, it's held by PendingAckInfo
    if (mc_errors > 0 && mc_members_sent_to == 0) {
      // If multicast failed completely, cleanup pending entry
      pthread_mutex_lock(&target_group->pending_ack_mutex);
      if (pending_slot != -1)
        clear_pending_ack_entry(&target_group->pending_acks[pending_slot]);
      pthread_mutex_unlock(&target_group->pending_ack_mutex);
    }
  }
}

/*
 * Handles incoming SEQ_ASSIGNMENT messages (executed by all members).
 * Sends ACK to sequencer immediately.
 * === MODIFIED LOGIC ===
 * Tries to find matching unstable message (with data).
 * - If found: Assigns Global Seq. Calls try_deliver.
 * - If not found (or placeholder exists): Checks OOO buffer for the data.
 * - If data found in OOO: Moves data to unstable buffer, assigns Global Seq. Calls try_deliver.
 * - If data not found in OOO: Creates/updates placeholder in unstable buffer.
 */
static void handle_sequence_assignment(group_t *target_group, const char *original_sender_id,
                                       uint32_t original_sequence_num, uint64_t global_sequence_num,
                                       const char *sequencer_id, const struct sockaddr_in *sequencer_addr) {
  printf(
      " UDP Recv Helper: Handling SEQ_ASSIGNMENT GlobalSeq=%lu for OrigSender=%s OrigLocalSeq=%u from Sequencer=%s\n",
      (unsigned long)global_sequence_num, original_sender_id, original_sequence_num, sequencer_id);

  // 1. Send ACK back to the sequencer IMMEDIATELY (Code remains the same as before)
  char my_id[MAX_ID_LEN + 1];
  pthread_mutex_lock(&group_data_mutex); // Lock needed to read my_id_in_group safely
  strncpy(my_id, target_group->my_id_in_group, MAX_ID_LEN);
  my_id[MAX_ID_LEN] = '\0';
  // *** Check if group is active ***
  if (!target_group->is_active) {
    pthread_mutex_unlock(&group_data_mutex);
    fprintf(stderr, "UDP Recv Helper: Group '%s' inactive while handling SEQ_ASSIGNMENT. Ignoring.\n",
            target_group->group_name);
    return; // Cannot proceed if group inactive
  }
  pthread_mutex_unlock(&group_data_mutex); // Unlock after reading my_id

  if (strlen(my_id) > 0) {
    size_t ack_header_len =
        sizeof(uint8_t) * 3 + strlen(target_group->group_name) + strlen(my_id) + sizeof(uint8_t) + sizeof(uint32_t);
    if (ack_header_len < UDP_BUFFER_SIZE) {
      unsigned char ack_buffer[ack_header_len];
      unsigned char *ack_ptr = ack_buffer;
      uint8_t type = TYPE_ACK;
      uint8_t gn_len = strlen(target_group->group_name);
      uint8_t my_id_len = strlen(my_id);
      uint32_t ack_seq_net = htonl(original_sequence_num);
      uint8_t acked_type = ACKED_TYPE_SEQ_ASSIGNMENT;
      memcpy(ack_ptr, &type, sizeof(uint8_t));
      ack_ptr++;
      memcpy(ack_ptr, &gn_len, sizeof(uint8_t));
      ack_ptr++;
      memcpy(ack_ptr, target_group->group_name, gn_len);
      ack_ptr += gn_len;
      memcpy(ack_ptr, &my_id_len, sizeof(uint8_t));
      ack_ptr++;
      memcpy(ack_ptr, my_id, my_id_len);
      ack_ptr += my_id_len;
      memcpy(ack_ptr, &acked_type, sizeof(uint8_t));
      ack_ptr++;
      memcpy(ack_ptr, &ack_seq_net, sizeof(uint32_t));
      sendto(global_udp_socket, ack_buffer, ack_header_len, 0, (struct sockaddr *)sequencer_addr,
             sizeof(struct sockaddr_in));
      atomic_fetch_add(&proto_ack_sent_count, 1);
      printf("  -> Sent ACK for SEQ_ASSIGNMENT (OrigLocalSeq=%u) to sequencer %s\n", original_sequence_num,
             sequencer_id);
    } else {
      fprintf(stderr, "UDP Recv Helper: Cannot send ACK for SEQ_ASSIGNMENT (header too large).\n");
    }
  } else {
    fprintf(stderr, "UDP Recv Helper: Cannot send ACK for SEQ_ASSIGNMENT (own ID unknown).\n");
  }

  // *** NEW: Clear Pending Sequence Request (Code remains the same as before) ***
  int cleared_pending_req = 0;
  pthread_mutex_lock(&group_data_mutex);
  int is_my_message = (strcmp(target_group->my_id_in_group, original_sender_id) == 0);
  pthread_mutex_unlock(&group_data_mutex);

  if (is_my_message) {
    pthread_mutex_lock(&target_group->pending_seq_assignment_mutex);
    for (int i = 0; i < PENDING_SEQ_ASSIGNMENTS_SIZE; i++) {
      PendingSeqAssignmentInfo *p_assign = &target_group->pending_seq_assignments[i];
      if (p_assign->active && p_assign->original_local_seq_no == original_sequence_num &&
          strcmp(p_assign->original_sender_id, original_sender_id) == 0) {
        printf("  -> Received SEQ_ASSIGNMENT for my own message, clearing pending request tracker for LocalSeq=%u "
               "(Slot %d).\n",
               original_sequence_num, i);
        p_assign->active = 0;
        p_assign->retry_count = 0;
        p_assign->group_name[0] = '\0';
        p_assign->original_sender_id[0] = '\0';
        cleared_pending_req = 1;
        break;
      }
    }
    pthread_mutex_unlock(&target_group->pending_seq_assignment_mutex);
    if (!cleared_pending_req) {
      printf("  -> Received SEQ_ASSIGNMENT for my LocalSeq=%u, but no matching pending request found (or already "
             "cleared).\n",
             original_sequence_num);
    }
  }
  // *** END NEW Clear Pending ***

  // 2. === MODIFIED: Find matching unstable message or check OOO before creating placeholder ===
  int found_in_unstable = 0;
  int moved_from_ooo = 0;
  int created_placeholder = 0;
  int unstable_target_slot = -1; // Index in unstable buffer where message was found/placed

  // *** Acquire locks in correct order: group_data -> unstable ***
  pthread_mutex_lock(&group_data_mutex);
  // Re-check group active status under lock
  if (!target_group->is_active) {
    pthread_mutex_unlock(&group_data_mutex);
    fprintf(stderr, "UDP Recv Helper: Group '%s' became inactive before processing SEQ_ASSIGNMENT. Ignoring.\n",
            target_group->group_name);
    return;
  }

  pthread_mutex_lock(&target_group->unstable_mutex);

  // First, try to find an existing entry WITH DATA in unstable buffer
  for (int i = 0; i < UNSTABLE_BUFFER_SIZE; i++) {
    UnstableDataMsg *unstable_msg = &target_group->unstable_data_buffer[i];
    if (unstable_msg->active && unstable_msg->data_missing == 0 &&       // Must have data
        unstable_msg->global_sequence_number == UNASSIGNED_GLOBAL_SEQ && // Waiting for GSeq
        unstable_msg->sequence_number == original_sequence_num &&
        strcmp(unstable_msg->sender_id, original_sender_id) == 0) {
      unstable_msg->global_sequence_number = global_sequence_num;
      found_in_unstable = 1;
      unstable_target_slot = i;
      printf("  -> Assigned GlobalSeq=%lu to existing unstable message (with data) from %s (LocalSeq=%u) at index %d\n",
             (unsigned long)global_sequence_num, original_sender_id, original_sequence_num, i);
      break;
    }
  }

  // If not found with data in unstable, check OOO buffer
  if (!found_in_unstable) {
    member_t *sender_info = NULL;

    // Find sender index (group_data_mutex is HELD)
    for (int i = 0; i < target_group->member_count; ++i) {
      if (strcmp(target_group->members[i].member_id, original_sender_id) == 0) {
        sender_info = &target_group->members[i];
        break;
      }
    }

    if (sender_info) {
      int ooo_found_idx = -1;
      OutOfOrderMsg *ooo_msg = NULL;

      // Search OOO buffer for the specific message
      for (int k = 0; k < OUT_OF_ORDER_BUFFER_SIZE; ++k) {
        if (sender_info->ooo_buffer[k].active && sender_info->ooo_buffer[k].sequence_number == original_sequence_num) {
          ooo_found_idx = k;
          ooo_msg = &sender_info->ooo_buffer[k];
          printf("  -> Found matching data in OOO buffer (slot %d) for %s, LocalSeq=%u\n", k, original_sender_id,
                 original_sequence_num);
          break;
        }
      }

      if (ooo_msg) {
        // Data found in OOO! Try to move it to unstable buffer.
        int free_unstable_slot = -1;
        for (int i = 0; i < UNSTABLE_BUFFER_SIZE; i++) {
          if (!target_group->unstable_data_buffer[i].active) {
            free_unstable_slot = i;
            break;
          }
          // Also check if a placeholder already exists for this exact message
          // (e.g., if SA arrived twice before data was processed from OOO)
          else if (target_group->unstable_data_buffer[i].active &&
                   target_group->unstable_data_buffer[i].data_missing == 1 &&
                   target_group->unstable_data_buffer[i].sequence_number == original_sequence_num &&
                   strcmp(target_group->unstable_data_buffer[i].sender_id, original_sender_id) == 0) {
            free_unstable_slot = i; // Overwrite existing placeholder
            printf("  -> Found existing placeholder at unstable index %d to overwrite.\n", i);
            break;
          }
        }

        if (free_unstable_slot != -1) {
          if (!target_group->unstable_data_buffer[free_unstable_slot].active) {
            // Only increment count if using a truly free slot, not overwriting placeholder
            if (target_group->unstable_count >= UNSTABLE_BUFFER_SIZE) {
              fprintf(stderr, "UDP Recv Helper Error: Unstable buffer count mismatch while moving from OOO!\n");
              free_unstable_slot = -1; // Prevent move
            } else {
              target_group->unstable_count++; // Increment unstable count
            }
          } else {
            // Overwriting placeholder, count doesn't change
          }

          if (free_unstable_slot != -1) {
            UnstableDataMsg *unstable_item = &target_group->unstable_data_buffer[free_unstable_slot];

            // Copy data from OOO message
            memcpy(unstable_item->data, ooo_msg->data, ooo_msg->len);
            unstable_item->len = ooo_msg->len;
            unstable_item->sender_addr = ooo_msg->sender_addr;
            unstable_item->sequence_number = ooo_msg->sequence_number;
            strncpy(unstable_item->sender_id, ooo_msg->sender_id, MAX_ID_LEN - 1);
            unstable_item->sender_id[MAX_ID_LEN - 1] = '\0';
            memcpy(unstable_item->received_vector_clock, ooo_msg->received_vector_clock,
                   sizeof(unstable_item->received_vector_clock));

            // Assign the global sequence number and mark data as present
            unstable_item->global_sequence_number = global_sequence_num;
            unstable_item->data_missing = 0;
            unstable_item->active = 1;

            // Remove from OOO buffer
            ooo_msg->active = 0;
            sender_info->ooo_count--;

            moved_from_ooo = 1;
            unstable_target_slot = free_unstable_slot; // Store index where it was placed

            printf("  -> Moved data from OOO (slot %d) to unstable buffer (slot %d) with GlobalSeq=%lu\n",
                   ooo_found_idx, unstable_target_slot, (unsigned long)global_sequence_num);

            // NOTE: We are *not* fully replicating the state update logic from
            // handle_unstable_data here (merging VC, checking subsequent OOO)
            // for simplicity. We rely on try_deliver_totally_ordered to handle that
            // when it finds this now-complete entry.
          }

        } else {
          fprintf(stderr, "UDP Recv Helper Error: Unstable buffer full, cannot move data from OOO for GlobalSeq=%lu!\n",
                  (unsigned long)global_sequence_num);
          // Data remains in OOO, SA is effectively lost for now.
        }
      }
    } // end if(sender_info)
  } // end if (!found_in_unstable)

  // If data wasn't found in unstable AND wasn't moved from OOO, create/update placeholder
  if (!found_in_unstable && !moved_from_ooo) {
    int placeholder_slot = -1;
    // Check if placeholder already exists
    for (int i = 0; i < UNSTABLE_BUFFER_SIZE; i++) {
      UnstableDataMsg *unstable_msg = &target_group->unstable_data_buffer[i];
      if (unstable_msg->active && unstable_msg->data_missing == 1 && // It's a placeholder
          unstable_msg->sequence_number == original_sequence_num &&
          strcmp(unstable_msg->sender_id, original_sender_id) == 0) {
        placeholder_slot = i;
        // Update GSeq just in case of duplicate SA? Or ignore? Let's ignore for now.
        printf("  -> Placeholder already exists for GlobalSeq=%lu (OrigSender=%s, OrigLocalSeq=%u) at index %d. "
               "Ignoring duplicate SA?\n",
               (unsigned long)unstable_msg->global_sequence_number, original_sender_id, original_sequence_num, i);
        created_placeholder = 1;  // Indicate placeholder exists
        unstable_target_slot = i; // Store index
        break;
      }
    }

    // If placeholder doesn't exist, create it
    if (placeholder_slot == -1) {
      for (int i = 0; i < UNSTABLE_BUFFER_SIZE; i++) {
        if (!target_group->unstable_data_buffer[i].active) {
          placeholder_slot = i;
          break;
        }
      }

      if (placeholder_slot != -1 && target_group->unstable_count < UNSTABLE_BUFFER_SIZE) {
        UnstableDataMsg *placeholder = &target_group->unstable_data_buffer[placeholder_slot];
        placeholder->active = 1;
        placeholder->data_missing = 1; // Mark as placeholder
        strncpy(placeholder->sender_id, original_sender_id, MAX_ID_LEN - 1);
        placeholder->sender_id[MAX_ID_LEN - 1] = '\0';
        placeholder->sequence_number = original_sequence_num;
        placeholder->global_sequence_number = global_sequence_num; // Store the GSeq
        placeholder->len = 0;
        memset(&placeholder->sender_addr, 0, sizeof(placeholder->sender_addr));
        memset(placeholder->received_vector_clock, 0, sizeof(placeholder->received_vector_clock));
        memset(placeholder->data, 0, UDP_BUFFER_SIZE);

        target_group->unstable_count++;
        created_placeholder = 1;
        unstable_target_slot = placeholder_slot; // Store index
        printf("  %s -> DATA not found locally. Created PLACEHOLDER at index %d for GlobalSeq=%lu (OrigSender=%s, "
               "OrigLocalSeq=%u). Unstable count: %d\n",
               target_group->my_id_in_group, placeholder_slot, (unsigned long)global_sequence_num, original_sender_id,
               original_sequence_num, target_group->unstable_count);
      } else {
        fprintf(stderr,
                "UDP Recv Helper: Unstable buffer full! Cannot create placeholder for GlobalSeq=%lu (OrigSender=%s, "
                "OrigLocalSeq=%u)\n",
                (unsigned long)global_sequence_num, original_sender_id, original_sequence_num);
        // Lost assignment!
      }
    }
  }

  // *** Unlock mutexes ***
  pthread_mutex_unlock(&target_group->unstable_mutex);
  pthread_mutex_unlock(&group_data_mutex);

  // 3. Try to deliver messages if we actually updated/moved data (not just placeholder)
  if (found_in_unstable || moved_from_ooo) {
    printf("  -> Triggering delivery check because GSeq was assigned or data moved from OOO.\n");
    try_deliver_totally_ordered(target_group);
  } else if (created_placeholder) {
    printf("  -> Placeholder created/found, delivery check skipped until data arrives.\n");
  }

  printf("<<< Exiting handle_sequence_assignment for GlobalSeq=%lu\n", (unsigned long)global_sequence_num);
}

/*
 * Thread που ακούει στο global_udp_socket για εισερχόμενα UDP μηνύματα.
 * Κάνει parse την κεφαλίδα και καλεί την κατάλληλη βοηθητική συνάρτηση.
 */
void *udp_receiver_func(void *arg) {
  unsigned char udp_buffer[UDP_BUFFER_SIZE + 1];
  struct sockaddr_in sender_addr;
  socklen_t sender_len = sizeof(sender_addr);
  ssize_t nbytes;

  printf("UDP Receiver thread started (listens on socket %d).\n", global_udp_socket);

  while (keep_udp_receiver_running) {
    memset(&sender_addr, 0, sizeof(sender_addr));
    nbytes = recvfrom(global_udp_socket, udp_buffer, UDP_BUFFER_SIZE, 0, (struct sockaddr *)&sender_addr, &sender_len);

    if (!keep_udp_receiver_running)
      break;
    if (nbytes < 1) {
      if (nbytes < 0 && errno != EINTR)
        perror("UDP Receiver recvfrom error");
      continue;
    }

    // --- Parse Header ---
    unsigned char *ptr = udp_buffer;
    int remaining = nbytes;
    int parse_error = 0;
    uint8_t msg_type = 0;
    char group_name[MAX_GROUP_LEN + 1] = {0};
    char sender_id[MAX_ID_LEN + 1] = {0};
    uint32_t sequence_num = 0;
    uint32_t ack_sequence_num = 0;
    int received_vector_clock[MAX_MEMBERS];
    memset(received_vector_clock, 0, sizeof(received_vector_clock));
    char original_sender_id[MAX_ID_LEN + 1] = {0};
    uint32_t original_sequence_num = 0;
    uint64_t global_sequence_num = UNASSIGNED_GLOBAL_SEQ;
    uint64_t state_expected_global_seq = 0; // For STATE_RESPONSE
    int state_vector_clock[MAX_MEMBERS];
    memset(state_vector_clock, 0, sizeof(state_vector_clock)); // For STATE_RESPONSE
    char failed_member_id_from_start[MAX_ID_LEN + 1] = {0};    // For VIEW_CHANGE_START
    uint64_t agreed_gseq_from_confirm = 0;                     // For VIEW_CHANGE_CONFIRM
    uint8_t acked_msg_type = 0;                                // For VIEW_CHANGE_ACK
    // Variables for RESTART parsing
    uint8_t num_failed_parsed = 0;
    char parsed_failed_list[MAX_MEMBERS][MAX_ID_LEN];
    int parsed_failed_count = 0;
    memset(parsed_failed_list, 0, sizeof(parsed_failed_list)); // Clear parse buffer
    uint8_t received_acked_type = 0; // <<< ΝΕΟ >>> Μεταβλητή για τον τύπο που επιβεβαιώνεται από το ACK

    // 1. Message Type
    if (remaining < sizeof(uint8_t)) {
      parse_error = 1;
    }
    if (!parse_error) {
      msg_type = *ptr++;
      remaining--;
    } else
      continue;
    // 2. Group Name
    uint8_t group_name_len = 0;
    if (remaining < sizeof(uint8_t)) {
      parse_error = 1;
    }
    if (!parse_error) {
      group_name_len = *ptr++;
      remaining--;
    }
    if (!parse_error && (group_name_len == 0 || group_name_len > MAX_GROUP_LEN)) {
      parse_error = 1;
    }
    if (!parse_error && remaining < group_name_len) {
      parse_error = 1;
    }
    if (!parse_error) {
      memcpy(group_name, ptr, group_name_len);
      group_name[group_name_len] = '\0';
      ptr += group_name_len;
      remaining -= group_name_len;
    } else {
      continue;
    }
    // 3. Sender ID (of this UDP packet)
    uint8_t sender_id_len = 0;
    if (remaining < sizeof(uint8_t)) {
      parse_error = 1;
    }
    if (!parse_error) {
      sender_id_len = *ptr++;
      remaining--;
    }
    if (!parse_error && (sender_id_len == 0 || sender_id_len > MAX_ID_LEN)) {
      parse_error = 1;
    }
    if (!parse_error && remaining < sender_id_len) {
      parse_error = 1;
    }
    if (!parse_error) {
      memcpy(sender_id, ptr, sender_id_len);
      sender_id[sender_id_len] = '\0';
      ptr += sender_id_len;
      remaining -= sender_id_len;
    } else {
      continue;
    }

    // --- Parsing based on Type ---
    if (msg_type == TYPE_ACK) {
      // ΝΕΑ Δομή ACK: Type(1)+GroupLen(1)+Group+SenderLen(1)+Sender+AckedType(1)+AckSeq(4)

      // <<< ΝΕΟ >>> Parsing του AckedMsgType (ΠΡΙΝ το AckSeq)
      if (!parse_error && remaining < sizeof(uint8_t)) { // Έλεγχος αν υπάρχει byte για τον τύπο
        fprintf(stderr, "UDP Recv Error (ACK): Not enough bytes for AckedType.\n");
        parse_error = 1;
      }
      if (!parse_error) {
        received_acked_type = *ptr; // Διάβασε το byte
        ptr++;                      // Προχώρα τον δείκτη
        remaining--;                // Μείωσε τα bytes

        // Προαιρετικός έλεγχος εγκυρότητας
        if (received_acked_type != ACKED_TYPE_DATA_UNSTABLE && received_acked_type != ACKED_TYPE_SEQ_ASSIGNMENT) {
          fprintf(stderr, "UDP Recv Warning (ACK): Received ACK with unknown AckedType %u from %s\n",
                  received_acked_type, sender_id);
          // parse_error = 1; // ή απλά αγνόησέ το
        }
      }
      // <<< ΤΕΛΟΣ ΝΕΟΥ PARSING >>>

      // Parsing του AckSeq (Υπάρχων κώδικας)
      if (!parse_error && remaining < sizeof(uint32_t)) { // Έλεγχος αν υπάρχουν bytes για το seq num
        fprintf(stderr, "UDP Recv Error (ACK): Not enough bytes for AckSeq after AckedType.\n");
        parse_error = 1;
      }
      if (!parse_error) {
        uint32_t seq_net;
        memcpy(&seq_net, ptr, sizeof(uint32_t));
        ptr += sizeof(uint32_t);
        remaining -= sizeof(uint32_t);
        ack_sequence_num = ntohl(seq_net);
      }
    } else if (msg_type == TYPE_DATA_UNSTABLE) {
      // Header: Type(1) + GroupLen(1) + Group + SenderLen(1) + Sender + LocalSeq(4) + VC(N*4)
      // + Payload
      if (remaining < sizeof(uint32_t)) {
        parse_error = 1;
      } // LocalSeq
      if (!parse_error) {
        uint32_t seq_net;
        memcpy(&seq_net, ptr, sizeof(uint32_t));
        ptr += sizeof(uint32_t);
        remaining -= sizeof(uint32_t);
        sequence_num = ntohl(seq_net);
      }
      int vector_clock_bytes = sizeof(int) * MAX_MEMBERS;
      if (!parse_error && remaining < vector_clock_bytes) {
        parse_error = 1;
      } // VC
      if (!parse_error) {
        for (int i = 0; i < MAX_MEMBERS; i++) {
          int vc_entry_net;
          memcpy(&vc_entry_net, ptr, sizeof(int));
          ptr += sizeof(int);
          remaining -= sizeof(int);
          received_vector_clock[i] = ntohl(vc_entry_net);
        }
      }
    } else if (msg_type == TYPE_SEQ_REQUEST) {
      // Header: Type(1) + GroupLen(1) + Group + SenderLen(1) + Sender(Original) +
      // LocalSeq(4)(Original) sender_id parsed above IS the original sender here.
      if (remaining < sizeof(uint32_t)) {
        parse_error = 1;
      } // OrigLocalSeq
      if (!parse_error) {
        uint32_t seq_net;
        memcpy(&seq_net, ptr, sizeof(uint32_t));
        ptr += sizeof(uint32_t);
        remaining -= sizeof(uint32_t);
        sequence_num = ntohl(seq_net);
      } // Store original local seq in sequence_num
    } else if (msg_type == TYPE_SEQ_ASSIGNMENT) {
      // Header: Type(1) + GroupLen(1) + Group + SenderLen(1) + Sender(Sequencer) +
      // OrigSenderLen(1) + OrigSender + OrigLocalSeq(4) + GlobalSeq(8)
      uint8_t orig_sender_id_len = 0;
      if (remaining < sizeof(uint8_t)) {
        parse_error = 1;
      }
      if (!parse_error) {
        orig_sender_id_len = *ptr++;
        remaining--;
      }
      if (!parse_error && (orig_sender_id_len == 0 || orig_sender_id_len > MAX_ID_LEN)) {
        parse_error = 1;
      }
      if (!parse_error && remaining < orig_sender_id_len) {
        parse_error = 1;
      }
      if (!parse_error) {
        memcpy(original_sender_id, ptr, orig_sender_id_len);
        original_sender_id[orig_sender_id_len] = '\0';
        ptr += orig_sender_id_len;
        remaining -= orig_sender_id_len;
      }

      if (!parse_error && remaining < sizeof(uint32_t)) {
        parse_error = 1;
      } // OrigLocalSeq
      if (!parse_error) {
        uint32_t seq_net;
        memcpy(&seq_net, ptr, sizeof(uint32_t));
        ptr += sizeof(uint32_t);
        remaining -= sizeof(uint32_t);
        original_sequence_num = ntohl(seq_net);
      }

      if (!parse_error && remaining < sizeof(uint64_t)) {
        parse_error = 1;
      } // GlobalSeq
      if (!parse_error) {
        uint64_t gseq_net;
        memcpy(&gseq_net, ptr, sizeof(uint64_t));
        ptr += sizeof(uint64_t);
        remaining -= sizeof(uint64_t);
        global_sequence_num = be64toh(gseq_net);
      }
      // The sender_id parsed earlier is the Sequencer's ID for this message type
      // The sequence_num parsed earlier is not used for this type
    } else if (msg_type == TYPE_STATE_REQUEST) { /* No more fields needed after sender ID */
    } else if (msg_type == TYPE_STATE_RESPONSE) {
      // Header: Type(1) + GroupLen(1) + Group + SenderLen(1) + Sender(Sequencer) +
      // ExpectedGlobalSeq(8) + VectorClock(N*4)
      if (remaining < sizeof(uint64_t)) {
        parse_error = 1;
      } // ExpectedGlobalSeq
      if (!parse_error) {
        uint64_t exp_gseq_net;
        memcpy(&exp_gseq_net, ptr, sizeof(uint64_t));
        ptr += sizeof(uint64_t);
        remaining -= sizeof(uint64_t);
        state_expected_global_seq = be64toh(exp_gseq_net);
      }
      int state_vc_bytes = sizeof(int) * MAX_MEMBERS;
      if (!parse_error && remaining < state_vc_bytes) {
        parse_error = 1;
      } // VectorClock
      if (!parse_error) {
        for (int i = 0; i < MAX_MEMBERS; i++) {
          int vc_entry_net;
          memcpy(&vc_entry_net, ptr, sizeof(int));
          ptr += sizeof(int);
          remaining -= sizeof(int);
          state_vector_clock[i] = ntohl(vc_entry_net);
        }
      }
    }
    // === PARSING ΓΙΑ TYPE_VIEW_CHANGE_START ===
    else if (msg_type == TYPE_VIEW_CHANGE_START) {
      // Header: Type(1) + GroupLen(1) + GroupName + SenderLen(1) + SenderID(Coordinator) +
      // FailedIDLen(1) + FailedID
      uint8_t failed_id_len = 0;
      if (remaining < sizeof(uint8_t)) {
        parse_error = 1;
      } // FailedIDLen
      if (!parse_error) {
        failed_id_len = *ptr++;
        remaining--;
      }
      if (!parse_error && (failed_id_len == 0 || failed_id_len > MAX_ID_LEN)) {
        parse_error = 1;
      } // Validate len
      if (!parse_error && remaining < failed_id_len) {
        parse_error = 1;
      } // Check remaining bytes
      if (!parse_error) {
        memcpy(failed_member_id_from_start, ptr, failed_id_len);
        failed_member_id_from_start[failed_id_len] = '\0';
        ptr += failed_id_len; // Consume bytes
        remaining -= failed_id_len;
      }
      // sender_id parsed earlier is the coordinator for this message type
    }
    // ==========================================
    // === PARSING ΓΙΑ TYPE_VIEW_CHANGE_STATE ===
    else if (msg_type == TYPE_VIEW_CHANGE_STATE) {
      // Header: Type(1) + GroupLen(1) + GroupName + SenderLen(1) + SenderID +
      // State_ExpectedGSeq(8)
      if (remaining < sizeof(uint64_t)) {
        parse_error = 1;
      } // State_ExpectedGSeq
      if (!parse_error) {
        uint64_t gseq_net;
        memcpy(&gseq_net, ptr, sizeof(uint64_t));
        ptr += sizeof(uint64_t);
        remaining -= sizeof(uint64_t);
        state_expected_global_seq = be64toh(gseq_net); // Use the same variable as STATE_RESPONSE for convenience
      }
      // sender_id parsed earlier is the member reporting its state
    }
    // ===========================================
    // === PARSING ΓΙΑ TYPE_VIEW_CHANGE_CONFIRM ===
    else if (msg_type == TYPE_VIEW_CHANGE_CONFIRM) {
      // Header: Type(1) + GroupLen(1) + GroupName + SenderLen(1) + SenderID(Coordinator) +
      // AgreedGSeq(8)
      if (remaining < sizeof(uint64_t)) {
        parse_error = 1;
      } // AgreedGSeq
      if (!parse_error) {
        uint64_t gseq_net;
        memcpy(&gseq_net, ptr, sizeof(uint64_t));
        ptr += sizeof(uint64_t);
        remaining -= sizeof(uint64_t);
        agreed_gseq_from_confirm = be64toh(gseq_net);
      }
      // sender_id parsed earlier is the coordinator ID
    }
    // ============================================
    // === PARSING ΓΙΑ TYPE_VIEW_CHANGE_ACK ===
    else if (msg_type == TYPE_VIEW_CHANGE_ACK) {
      // Header: Type(1) + GroupLen(1) + GroupName + SenderLen(1) + SenderID(Acker) +
      // AckedMsgType(1)
      if (remaining < sizeof(uint8_t)) {
        parse_error = 1;
      } // AckedMsgType
      if (!parse_error) {
        acked_msg_type = *ptr++;
        remaining--;
        // Check if acked_msg_type is valid? (START, STATE, CONFIRM)
        if (acked_msg_type != TYPE_VIEW_CHANGE_START && acked_msg_type != TYPE_VIEW_CHANGE_STATE &&
            acked_msg_type != TYPE_VIEW_CHANGE_CONFIRM) {
          parse_error = 1;
        }
      }
      // sender_id parsed earlier is the member sending the ACK
    }
    // =========================================
    else if (msg_type == TYPE_VIEW_CHANGE_RESTART) {
      // Parse FailedCount
      if (remaining < sizeof(uint8_t)) {
        parse_error = 1;
      }
      if (!parse_error) {
        num_failed_parsed = *ptr++;
        remaining--;
      }
      // Parse each Failed ID (Len + ID)
      if (!parse_error) {
        printf("UDP Recv: Parsing RESTART message with %u failed members listed.\n", num_failed_parsed);
        for (int i = 0; i < num_failed_parsed && i < MAX_MEMBERS; i++) {
          uint8_t failed_id_len = 0;
          if (remaining < sizeof(uint8_t)) {
            parse_error = 1;
            break;
          }
          failed_id_len = *ptr++;
          remaining--;
          if (failed_id_len == 0 || failed_id_len > MAX_ID_LEN) {
            parse_error = 1;
            fprintf(stderr, "UDP Recv Error: Invalid failed ID length %u in RESTART.\n", failed_id_len);
            break;
          }
          if (remaining < failed_id_len) {
            parse_error = 1;
            fprintf(stderr, "UDP Recv Error: Insufficient data for failed ID in RESTART.\n");
            break;
          }
          memcpy(parsed_failed_list[parsed_failed_count], ptr, failed_id_len);
          parsed_failed_list[parsed_failed_count][failed_id_len] = '\0';
          ptr += failed_id_len;
          remaining -= failed_id_len;
          printf("  -> Parsed failed ID: %s\n", parsed_failed_list[parsed_failed_count]);
          parsed_failed_count++;
        }
        if (!parse_error && parsed_failed_count != num_failed_parsed) {
          fprintf(stderr, "UDP Recv Warning: Parsed failed count mismatch in RESTART (%d != %u).\n",
                  parsed_failed_count, num_failed_parsed);
        }
      }
    }
    // =============================================
    else {
      parse_error = 1; // Unknown message type
    }

    unsigned char *payload_ptr = ptr;
    int payload_len = remaining; // Payload only exists for DATA_UNSTABLE
    if (parse_error) {
      fprintf(stderr, "UDP Receiver: Parse error for msg type %u.\n", msg_type);
      continue;
    }

    // --- Εκτύπωση Όλων των Parsed Header Fields ---
    // char sender_ip_str[INET_ADDRSTRLEN];
    // inet_ntop(AF_INET, &sender_addr.sin_addr, sender_ip_str, sizeof(sender_ip_str));
    // printf("---------------- Parsed Header -----------------\n");
    // printf("  Source: %s:%d\n", sender_ip_str, ntohs(sender_addr.sin_port));
    // printf("  Msg Type: %u (", msg_type);
    // switch(msg_type) {
    //     case TYPE_ACK: printf("ACK)\n"); break;
    //     case TYPE_DATA_UNSTABLE: printf("DATA_UNSTABLE)\n"); break;
    //     case TYPE_SEQ_REQUEST: printf("SEQ_REQUEST)\n"); break;
    //     case TYPE_SEQ_ASSIGNMENT: printf("SEQ_ASSIGNMENT)\n"); break;
    //     default: printf("UNKNOWN)\n"); break;
    // }
    // printf("  Group Name: '%s'\n", group_name);
    // printf("  Sender ID (UDP Packet): '%s'\n", sender_id);

    // if (msg_type == TYPE_ACK) {
    //     printf("  ACK Sequence Num: %u\n", ack_sequence_num);
    // } else if (msg_type == TYPE_DATA_UNSTABLE) {
    //     printf("  Local Sequence Num: %u\n", sequence_num);
    //     printf("  Received Vector Clock: [");
    //     for (int i = 0; i < MAX_MEMBERS; i++) {
    //         printf("%d%s", received_vector_clock[i], (i == MAX_MEMBERS - 1) ? "" : ", ");
    //     }
    //     printf("]\n");
    // } else if (msg_type == TYPE_SEQ_REQUEST) {
    //     // sender_id is original sender, sequence_num is original local seq
    //     printf("  Original Sender ID: '%s'\n", sender_id);
    //     printf("  Original Local Seq: %u\n", sequence_num);
    // } else if (msg_type == TYPE_SEQ_ASSIGNMENT) {
    //     // sender_id is sequencer
    //     printf("  Sequencer ID: '%s'\n", sender_id);
    //     printf("  Original Sender ID: '%s'\n", original_sender_id);
    //     printf("  Original Local Seq: %u\n", original_sequence_num);
    //     printf("  Assigned Global Seq: %lu\n", (unsigned long)global_sequence_num);
    // }
    // printf("--------------------------------------------------\n");
    // --- Τέλος Εκτύπωσης ---

    // --- Find Group and Role ---
    int target_group_handle = -1;
    group_t *target_group = NULL; // Pointer to the target group struct

    pthread_mutex_lock(&group_data_mutex);

    // Find the group handle
    int found_active_group = -1;
    int found_inactive_group_for_ack = -1;

    for (int g = 0; g < MAX_GROUPS; g++) {
      if (strcmp(global_groups[g].group_name, group_name) == 0) {
        if (global_groups[g].is_active) {
          found_active_group = g;
          break; // Prioritize active group
        } else if (msg_type == TYPE_ACK) {
          // For ACKs, remember the first inactive match found
          if (found_inactive_group_for_ack == -1) {
            found_inactive_group_for_ack = g;
          }
        }
      }
    }

    if (found_active_group != -1) {
      target_group_handle = found_active_group;
      target_group = &global_groups[target_group_handle]; // Get pointer to active group

      // Determine role ONLY if group is active
      char my_id_copy[MAX_ID_LEN + 1];
      strncpy(my_id_copy, target_group->my_id_in_group, MAX_ID_LEN);
      my_id_copy[MAX_ID_LEN] = '\0';

    } else if (msg_type == TYPE_ACK && found_inactive_group_for_ack != -1) {
      // ACK for an inactive group we were part of
      target_group_handle = found_inactive_group_for_ack;
      target_group = &global_groups[target_group_handle]; // Get pointer to inactive group
    }

    // Check if we found a suitable group
    if (target_group_handle == -1) {
      fprintf(stderr, "UDP Recv: Group '%s' not found or inactive for msg type %u.\n", group_name, msg_type);
      pthread_mutex_unlock(&group_data_mutex); // Unlock after getting group pointer and role info
      continue;                                // Ignore message
    }
    // --- IMPORTANT: Check if group is active for non-ACK messages ---
    if (!target_group->is_active && !(msg_type == TYPE_ACK || msg_type == TYPE_VIEW_CHANGE_ACK)) {
      fprintf(stderr, "UDP Recv: Group '%s' is inactive. Ignoring non-ACK msg type %u.\n", group_name, msg_type);
      pthread_mutex_unlock(&group_data_mutex); // Unlock after getting group pointer and role info
      continue;                                // Ignore message for inactive group unless it's an ACK
    }
    pthread_mutex_unlock(&group_data_mutex); // Unlock after getting group pointer and role info
    // From here on, use target_group pointer and i_am_sequencer flag
    // Remember to free current_sequencer_id at the end of the main if/else if block
    // --- Τέλος Ενότητας Εύρεσης Group & Ρόλου ---

    // --- Handle Message ---
    if (msg_type == TYPE_ACK) {
      handle_ack_message(target_group, ack_sequence_num, sender_id, received_acked_type);
    } else if (msg_type == TYPE_DATA_UNSTABLE) {
      handle_unstable_data(target_group, sequence_num, sender_id, received_vector_clock, payload_ptr, payload_len,
                           &sender_addr);
    } else if (msg_type == TYPE_SEQ_REQUEST) {
      pthread_mutex_lock(&group_data_mutex); // Lock needed for get_sequencer_id and role check
      char *current_sequencer_id = get_sequencer_id(target_group);
      int i_am_sequencer = (current_sequencer_id && strcmp(target_group->my_id_in_group, current_sequencer_id) == 0);
      pthread_mutex_unlock(&group_data_mutex); // Unlock after check

      if (i_am_sequencer) {
        // sender_id is original sender, sequence_num is original local seq
        handle_sequencer_request(target_group, sequence_num, sender_id, sender_id_len, group_name, target_group_handle);
      } else {
        fprintf(stderr, "UDP Recv: Non-sequencer received SEQ_REQUEST. Discarding.\n");
      }
      if (current_sequencer_id)
        free(current_sequencer_id);
    } else if (msg_type == TYPE_SEQ_ASSIGNMENT) {
      // sender_id is sequencer, original_sender_id/original_sequence_num identify the message
      handle_sequence_assignment(target_group, original_sender_id, original_sequence_num, global_sequence_num,
                                 sender_id, &sender_addr);
    } else if (msg_type == TYPE_STATE_REQUEST) {
      // *** ΔΙΟΡΘΩΣΗ: Απάντηση από οποιονδήποτε, όχι μόνο τον sequencer ***
      printf("UDP Recv: Received STATE_REQUEST from %s\n", sender_id);
      pthread_mutex_lock(&group_data_mutex); // Lock needed for get_sequencer_id and role check
      // Read current state (expected global seq, VC) - MUTEX IS HELD
      uint64_t current_expected_gseq = target_group->expected_global_seq;
      int current_vc[MAX_MEMBERS];
      memcpy(current_vc, target_group->vector_clock, sizeof(current_vc));
      char my_current_id[MAX_ID_LEN + 1]; // Need own ID for the response
      strncpy(my_current_id, target_group->my_id_in_group, MAX_ID_LEN);
      my_current_id[MAX_ID_LEN] = '\0';
      pthread_mutex_unlock(&group_data_mutex); // Unlock before sending response

      // Build STATE_RESPONSE message
      uint8_t resp_type = TYPE_STATE_RESPONSE;
      uint8_t resp_grp_len = strlen(group_name);
      uint8_t resp_my_id_len = strlen(my_current_id); // Send own ID as sender
      uint64_t resp_exp_gseq_net = htobe64(current_expected_gseq);
      int resp_vc_bytes = sizeof(int) * MAX_MEMBERS;
      size_t resp_len = 1 + 1 + resp_grp_len + 1 + resp_my_id_len + sizeof(uint64_t) + resp_vc_bytes;
      unsigned char *resp_buf = NULL;

      if (resp_len <= UDP_BUFFER_SIZE) {
        resp_buf = malloc(resp_len);
        if (resp_buf) {
          unsigned char *r_ptr = resp_buf;
          memcpy(r_ptr, &resp_type, 1);
          r_ptr++;
          memcpy(r_ptr, &resp_grp_len, 1);
          r_ptr++;
          memcpy(r_ptr, group_name, resp_grp_len);
          r_ptr += resp_grp_len;
          memcpy(r_ptr, &resp_my_id_len, 1);
          r_ptr++;
          memcpy(r_ptr, my_current_id, resp_my_id_len);
          r_ptr += resp_my_id_len; // Send own ID
          memcpy(r_ptr, &resp_exp_gseq_net, sizeof(uint64_t));
          r_ptr += sizeof(uint64_t);
          for (int i = 0; i < MAX_MEMBERS; ++i) {
            int vc_net = htonl(current_vc[i]);
            memcpy(r_ptr, &vc_net, sizeof(int));
            r_ptr += sizeof(int);
          }

          // Send response back to the requester
          sendto(global_udp_socket, resp_buf, resp_len, 0, (struct sockaddr *)&sender_addr, sender_len);
          atomic_fetch_add(&proto_state_response_sent_count, 1);
          printf("UDP Recv: Sent STATE_RESPONSE to %s (ExpectedGSeq=%lu)\n", sender_id,
                 (unsigned long)current_expected_gseq);
          free(resp_buf);
        } else {
          perror("malloc for STATE_RESPONSE failed");
        }
      } else {
        fprintf(stderr, "Error: STATE_RESPONSE too large!\n");
      }

    } else if (msg_type == TYPE_STATE_RESPONSE) {
      // Received state response
      printf("UDP Recv: Received STATE_RESPONSE from %s (ExpectedGSeq=%lu)\n", sender_id,
             (unsigned long)state_expected_global_seq);
      pthread_mutex_lock(&group_data_mutex); // Lock needed for get_sequencer_id and role check
      // Update local state - MUTEX IS HELD
      target_group->expected_global_seq = state_expected_global_seq;
      // Merge received VC into local VC
      for (int k = 0; k < target_group->member_count; k++) { // Iterate up to current member count
        if (k < MAX_MEMBERS) {                               // Bounds check for safety
          if (state_vector_clock[k] > target_group->vector_clock[k]) {
            target_group->vector_clock[k] = state_vector_clock[k];
          }
        }
      }
      // *** Η ΠΡΟΣΘΗΚΗ ΣΑΣ ΕΔΩ ***
      printf("  State Transfer: Vector Clock AFTER MERGE: [");
      for (int k = 0; k < target_group->member_count; k++) { // Loop again to print the updated state
        if (k < MAX_MEMBERS) {
          // Print Member ID (for context) and the merged VC value
          printf(" %s(%d):%d", target_group->members[k].member_id, k, target_group->vector_clock[k]);
        }
      }
      printf(" ]\n");
      fflush(stdout); // Make sure the output is flushed immediately
      // ***************************
      // Initialize expected_recv_seq based on the received+merged VC
      printf("  State Transfer: Initializing expected_recv_seq based on received VC:\n");
      for (int k = 0; k < target_group->member_count; k++) {
        if (k < MAX_MEMBERS) { // Bounds check for safety
          target_group->members[k].expected_recv_seq = target_group->vector_clock[k];
          printf("    -> Member %d ('%s'): expected_recv_seq = %u\n", k, target_group->members[k].member_id,
                 target_group->members[k].expected_recv_seq);
        }
      }
      // --- Συγχρονισμός next_global_seq αν είμαστε ο sequencer ---
      char *current_sequencer_id = get_sequencer_id(target_group);
      if (current_sequencer_id && strcmp(target_group->my_id_in_group, current_sequencer_id) == 0) {
        // I am the sequencer for this group after the join/state transfer
        target_group->next_global_seq = target_group->expected_global_seq;
        printf("  State Transfer: I am the sequencer. Set next_global_seq to %lu\n",
               (unsigned long)target_group->next_global_seq);
      }
      if (current_sequencer_id)
        free(current_sequencer_id);
      // -----------------------------------------------------------
      pthread_mutex_unlock(&group_data_mutex); // Unlock after updating state

      // Signal grp_join that state transfer is complete
      pthread_mutex_lock(&target_group->state_transfer_mutex);
      target_group->state_transfer_complete = 1;
      pthread_cond_signal(&target_group->state_transfer_cond);
      pthread_mutex_unlock(&target_group->state_transfer_mutex);

      // After receiving state, check if any buffered OOO messages are now ready
      pthread_mutex_lock(&group_data_mutex);
      for (int sender_idx = 0; sender_idx < target_group->member_count; ++sender_idx) {
        member_t *sender_info = &target_group->members[sender_idx];
        if (sender_info->ooo_count > 0) {
          printf("  State Transfer: Checking OOO buffer for sender %s after state update\n", sender_info->member_id);
          // This check logic is similar to the one at the end of handle_unstable_data
          int processed_ooo = 1;
          while (processed_ooo) {
            processed_ooo = 0;
            int found_ooo_idx = -1;
            OutOfOrderMsg *ooo_msg_to_process = NULL;
            for (int k = 0; k < OUT_OF_ORDER_BUFFER_SIZE; ++k) {
              if (sender_info->ooo_buffer[k].active &&
                  sender_info->ooo_buffer[k].sequence_number == sender_info->expected_recv_seq) {
                int ooo_causally_ready = 1;
                // Need current member count for causal check bound
                int current_member_count_inner = target_group->member_count;
                for (int vc_k = 0; vc_k < current_member_count_inner; vc_k++) {
                  if (vc_k != sender_idx &&
                      sender_info->ooo_buffer[k].received_vector_clock[vc_k] > target_group->vector_clock[vc_k]) {
                    ooo_causally_ready = 0;
                    break;
                  }
                }
                if (ooo_causally_ready) {
                  found_ooo_idx = k;
                  ooo_msg_to_process = &sender_info->ooo_buffer[k];
                  break;
                } else {
                  break;
                }
              }
            }
            if (found_ooo_idx != -1 && ooo_msg_to_process) {
              int ooo_buffered_unstable = 0;
              pthread_mutex_lock(&target_group->unstable_mutex);
              if (target_group->unstable_count < UNSTABLE_BUFFER_SIZE) {
                int unstable_slot = -1;
                for (int k = 0; k < UNSTABLE_BUFFER_SIZE; ++k) {
                  if (!target_group->unstable_data_buffer[k].active) {
                    unstable_slot = k;
                    break;
                  }
                }
                if (unstable_slot != -1) {
                  UnstableDataMsg *unstable_item = &target_group->unstable_data_buffer[unstable_slot];
                  /* ... copy data from ooo_msg_to_process to unstable_item ... */
                  memcpy(unstable_item->data, ooo_msg_to_process->data, ooo_msg_to_process->len);
                  unstable_item->len = ooo_msg_to_process->len;
                  unstable_item->sender_addr = ooo_msg_to_process->sender_addr;
                  unstable_item->sequence_number = ooo_msg_to_process->sequence_number;
                  strncpy(unstable_item->sender_id, ooo_msg_to_process->sender_id, MAX_ID_LEN - 1);
                  unstable_item->sender_id[MAX_ID_LEN - 1] = '\0';
                  memcpy(unstable_item->received_vector_clock, ooo_msg_to_process->received_vector_clock,
                         sizeof(unstable_item->received_vector_clock)); // Correct size
                  unstable_item->global_sequence_number = UNASSIGNED_GLOBAL_SEQ;
                  unstable_item->active = 1;
                  target_group->unstable_count++;
                  ooo_buffered_unstable = 1;
                  printf("  State Transfer: Moved OOO LocalSeq=%u to unstable "
                         "buffer.\n",
                         unstable_item->sequence_number);
                } else { /* Unstable Buffer full */
                }
              } else { /* Unstable Buffer full */
              }
              pthread_mutex_unlock(&target_group->unstable_mutex);

              if (ooo_buffered_unstable) {
                sender_info->expected_recv_seq++;
                // Merge VC from the OOO message
                for (int k = 0; k < target_group->member_count; k++) {
                  if (k < MAX_MEMBERS) { // Bounds check
                    if (ooo_msg_to_process->received_vector_clock[k] > target_group->vector_clock[k]) {
                      target_group->vector_clock[k] = ooo_msg_to_process->received_vector_clock[k];
                    }
                  }
                }
                ooo_msg_to_process->active = 0;
                sender_info->ooo_count--;
                processed_ooo = 1; // Re-check OOO for this sender
              } else {
                processed_ooo = 0;
              }
            } else {
              processed_ooo = 0;
            }
          } // end while(processed_ooo)
        } // end if (ooo_count > 0)
      } // end for each member
      pthread_mutex_unlock(&group_data_mutex);

    }
    // == ΧΕΙΡΙΣΜΟΣ ΓΙΑ TYPE_VIEW_CHANGE_START ===
    else if (msg_type == TYPE_VIEW_CHANGE_START) {
      // Received the official start signal from the coordinator (new sequencer)
      // sender_id holds the coordinator's ID
      // failed_member_id_from_start holds the ID of the failed member
      printf("UDP Recv: Received VIEW_CHANGE_START for group '%s' from coordinator '%s', "
             "regarding failed member '%s'.\n",
             group_name, sender_id, failed_member_id_from_start);

      // Send ACK back to the coordinator (sender_id)
      pthread_mutex_lock(&group_data_mutex); // Need own ID
      char my_id[MAX_ID_LEN + 1];
      strncpy(my_id, target_group->my_id_in_group, MAX_ID_LEN);
      my_id[MAX_ID_LEN] = '\0';
      pthread_mutex_unlock(&group_data_mutex);
      if (strlen(my_id) > 0) {
        uint8_t ack_msg_type = TYPE_VIEW_CHANGE_ACK;
        uint8_t ack_gn_len = strlen(group_name);
        uint8_t ack_sender_len = strlen(my_id);          // I am sending the ACK
        uint8_t ack_acked_type = TYPE_VIEW_CHANGE_START; // Acknowledging a START msg
        size_t ack_len = 1 + 1 + ack_gn_len + 1 + ack_sender_len + 1;
        unsigned char ack_buf[ack_len];
        unsigned char *ack_ptr = ack_buf;
        memcpy(ack_ptr, &ack_msg_type, 1);
        ack_ptr++;
        memcpy(ack_ptr, &ack_gn_len, 1);
        ack_ptr++;
        memcpy(ack_ptr, group_name, ack_gn_len);
        ack_ptr += ack_gn_len;
        memcpy(ack_ptr, &ack_sender_len, 1);
        ack_ptr++;
        memcpy(ack_ptr, my_id, ack_sender_len);
        ack_ptr += ack_sender_len;
        memcpy(ack_ptr, &ack_acked_type, 1);
        sendto(global_udp_socket, ack_buf, ack_len, 0, (struct sockaddr *)&sender_addr, sender_len);
        atomic_fetch_add(&proto_vc_ack_sent_count, 1);
        printf("  -> Sent VIEW_CHANGE_ACK for START to %s\n", sender_id);
      }
      // =============================
      int proceed_to_send_state_flag = 0;
      char coordinator_id_copy[MAX_ID_LEN + 1];

      pthread_mutex_lock(&target_group->vc_state_mutex);
      if (!target_group->view_change_active) {
        printf("  -> Activating view change locally based on START message.\n");
        target_group->view_change_active = 1;
        target_group->view_change_stage = 1; // Mark as started
        // Initialize failed list with the first failure
        target_group->vc_failed_member_count = 0;
        if (target_group->vc_failed_member_count < MAX_MEMBERS) {
          strncpy(target_group->vc_failed_member_list[target_group->vc_failed_member_count],
                  failed_member_id_from_start, MAX_ID_LEN - 1);
          target_group->vc_failed_member_list[target_group->vc_failed_member_count][MAX_ID_LEN - 1] = '\0';
          target_group->vc_failed_member_count++;
        }
        target_group->sync_acks_received = 0;
        target_group->sync_acks_needed = 0; // Coordinator will set needed
        proceed_to_send_state_flag = 1;
      } else if (target_group->vc_failed_member_count > 0 &&
                 strcmp(target_group->vc_failed_member_list[0], failed_member_id_from_start) == 0) {
        // VC already active for this failure, START confirms official start
        printf("  -> View change already active for this failure, START message confirms.\n");
        if (target_group->view_change_stage < 2) { // Only advance stage if not already past state sending
          target_group->view_change_stage = 2;     // Ready to send state
        }
        proceed_to_send_state_flag = 1;
      } else {
        fprintf(stderr,
                "UDP Recv Warning: Received VIEW_CHANGE_START for '%s' while already "
                "handling different failure. Ignoring START.\n",
                failed_member_id_from_start);
        proceed_to_send_state_flag = 0;
      }
      pthread_mutex_unlock(&target_group->vc_state_mutex);

      if (proceed_to_send_state_flag) {
        strncpy(coordinator_id_copy, sender_id, MAX_ID_LEN);
        coordinator_id_copy[MAX_ID_LEN] = '\0';
        printf("  -> Preparing to send state to coordinator '%s'.\n", coordinator_id_copy);
        send_view_change_state(target_group, coordinator_id_copy);
      }
    }
    // =================================================
    // == ΧΕΙΡΙΣΜΟΣ ΓΙΑ TYPE_VIEW_CHANGE_STATE ===
    // Inside udp_receiver_func, replace the existing block for TYPE_VIEW_CHANGE_STATE
    else if (msg_type == TYPE_VIEW_CHANGE_STATE) {
      // Received state from another member (sender_id)
      // state_expected_global_seq holds their reported state
      printf("UDP Recv: Received VIEW_CHANGE_STATE for group '%s' from member '%s' (Reported "
             "GSeq: %lu).\n",
             group_name, sender_id, (unsigned long)state_expected_global_seq);

      // Send ACK back to the member who sent the state (sender_id)
      pthread_mutex_lock(&group_data_mutex); // Need own ID
      char my_id[MAX_ID_LEN + 1];
      strncpy(my_id, target_group->my_id_in_group, MAX_ID_LEN);
      my_id[MAX_ID_LEN] = '\0';
      pthread_mutex_unlock(&group_data_mutex);
      if (strlen(my_id) > 0) {
        uint8_t ack_msg_type = TYPE_VIEW_CHANGE_ACK;
        uint8_t ack_gn_len = strlen(group_name);
        uint8_t ack_sender_len = strlen(my_id);          // I am sending the ACK
        uint8_t ack_acked_type = TYPE_VIEW_CHANGE_STATE; // Acknowledging a STATE msg
        size_t ack_len = 1 + 1 + ack_gn_len + 1 + ack_sender_len + 1;
        // Avoid large stack allocation if MAX_GROUP_LEN or MAX_ID_LEN is huge
        if (ack_len < UDP_BUFFER_SIZE) {
          unsigned char ack_buf[ack_len];
          unsigned char *ack_ptr = ack_buf;
          memcpy(ack_ptr, &ack_msg_type, 1);
          ack_ptr++;
          memcpy(ack_ptr, &ack_gn_len, 1);
          ack_ptr++;
          memcpy(ack_ptr, group_name, ack_gn_len);
          ack_ptr += ack_gn_len;
          memcpy(ack_ptr, &ack_sender_len, 1);
          ack_ptr++;
          memcpy(ack_ptr, my_id, ack_sender_len);
          ack_ptr += ack_sender_len;
          memcpy(ack_ptr, &ack_acked_type, 1);
          // Send ACK to the original sender of the STATE message
          sendto(global_udp_socket, ack_buf, ack_len, 0, (struct sockaddr *)&sender_addr, sender_len);
          printf("  -> Sent VIEW_CHANGE_ACK for STATE to %s\n", sender_id);
        } else {
          fprintf(stderr, "UDP Recv Error: VIEW_CHANGE_ACK message too large.\n");
        }
      }
      // =============================

      int i_am_coordinator = 0;
      int send_confirm_flag = 0;
      uint64_t final_agreed_gseq = 0;
      char local_failed_list[MAX_MEMBERS][MAX_ID_LEN]; // Local copy of failed list
      int local_failed_count = 0;
      int local_sync_acks_needed = 0;              // Local copy of needed ACKs
      uint64_t local_received_states[MAX_MEMBERS]; // Local copy for calculation
      uint64_t local_my_expected_gseq = 0;         // Local copy of own state

      // --- Check if VC is active and get failed list ---
      pthread_mutex_lock(&target_group->vc_state_mutex);
      int vc_is_active = target_group->view_change_active;
      if (vc_is_active) {
        local_failed_count = target_group->vc_failed_member_count;
        for (int i = 0; i < local_failed_count; ++i) {
          strncpy(local_failed_list[i], target_group->vc_failed_member_list[i], MAX_ID_LEN);
          local_failed_list[i][MAX_ID_LEN - 1] = '\0';
        }
        local_sync_acks_needed = target_group->sync_acks_needed; // Get needed count
      }
      pthread_mutex_unlock(&target_group->vc_state_mutex);

      if (!vc_is_active) {
        fprintf(stderr, "UDP Recv Warning: Received VIEW_CHANGE_STATE but view change is "
                        "not active. Ignoring.\n");
        continue; // Skip rest of processing for this message
      }

      // --- Check if I am the coordinator for the current view change ---
      pthread_mutex_lock(&group_data_mutex);
      char *current_coordinator_id =
          get_sequencer_id_excluding_list(target_group, local_failed_list, local_failed_count);
      if (current_coordinator_id) {
        i_am_coordinator = (strcmp(target_group->my_id_in_group, current_coordinator_id) == 0);
        free(current_coordinator_id);
      }
      // Find the index of the sender while group_data_mutex is held
      int sender_idx = -1;
      for (int i = 0; i < target_group->member_count; i++) {
        if (strcmp(target_group->members[i].member_id, sender_id) == 0) {
          sender_idx = i;
          break;
        }
      }
      pthread_mutex_unlock(&group_data_mutex);

      if (i_am_coordinator) {
        printf("  -> I am the coordinator. Processing received state from '%s'.\n", sender_id);

        pthread_mutex_lock(&target_group->vc_state_mutex); // Lock again to update state

        if (sender_idx != -1 && sender_idx < MAX_MEMBERS) { // Check bounds for index
          // Check if we haven't already recorded state from this sender *for this VC
          // instance*
          if (!target_group->vc_state_reported[sender_idx]) {
            target_group->vc_state_reported[sender_idx] = 1;
            target_group->vc_received_states[sender_idx] = state_expected_global_seq;
            target_group->sync_acks_received++;

            // *** ΝΕΟΣ ΕΠΑΝΥΠΟΛΟΓΙΣΜΟΣ sync_acks_needed ***
            int actual_needed_acks = 0;
            pthread_mutex_lock(&group_data_mutex); // Lock required to access current member list
            char my_id_for_calc[MAX_ID_LEN + 1];
            strncpy(my_id_for_calc, target_group->my_id_in_group, MAX_ID_LEN);
            my_id_for_calc[MAX_ID_LEN] = '\0';

            for (int i = 0; i < target_group->member_count; ++i) {
              int is_failed = 0;
              // Use the failed list currently stored in target_group->vc_failed_member_list under vc_state_mutex
              for (int f = 0; f < target_group->vc_failed_member_count; ++f) {
                if (target_group->vc_failed_member_list[f][0] != '\0' &&
                    strcmp(target_group->members[i].member_id, target_group->vc_failed_member_list[f]) == 0) {
                  is_failed = 1;
                  break;
                }
              }
              int is_self = (strcmp(target_group->members[i].member_id, my_id_for_calc) == 0);

              if (!is_failed && !is_self) {
                actual_needed_acks++;
              }
            }
            pthread_mutex_unlock(&group_data_mutex); // Unlock group data

            // Ενημέρωσε την τιμή που χρησιμοποιείται για τον έλεγχο
            target_group->sync_acks_needed = actual_needed_acks;
            printf("  -> Recalculated needed states: %d\n", actual_needed_acks);
            // *** ΤΕΛΟΣ ΕΠΑΝΥΠΟΛΟΓΙΣΜΟΥ ***

            printf("  -> Recorded state from '%s' (index %d). Total states received: "
                   "%d / %d\n",
                   sender_id, sender_idx, target_group->sync_acks_received,
                   local_sync_acks_needed); // Use local_sync_acks_needed

            // Check if all states have been received
            if (target_group->sync_acks_received >= local_sync_acks_needed) {
              printf("  -> All states received. Preparing to calculate minimum GSeq "
                     "and confirm.\n");
              // Copy received states and own state locally for calculation outside
              // lock
              memcpy(local_received_states, target_group->vc_received_states, sizeof(local_received_states));
              local_my_expected_gseq = target_group->expected_global_seq; // Get own state

              target_group->view_change_stage = 3; // Stage 3: Confirm Sent / Flushing
              send_confirm_flag = 1;               // Set flag to send confirm outside mutex
            }
          } else {
            printf("  -> Duplicate state received from '%s'. Ignoring.\n", sender_id);
          }
        } else {
          fprintf(stderr,
                  "UDP Recv Warning: Received VIEW_CHANGE_STATE from sender '%s' not "
                  "found in member list or index out of bounds (%d).\n",
                  sender_id, sender_idx);
        }
        pthread_mutex_unlock(&target_group->vc_state_mutex); // Unlock VC state

        // Calculate min GSeq and send confirmation if flag is set (outside VC state mutex)
        if (send_confirm_flag) {
          printf("  -> Calculating minimum GSeq...\n");
          uint64_t min_gseq = local_my_expected_gseq; // Start with my own state
          printf("    - My state (Coordinator): %lu\n", (unsigned long)min_gseq);

          pthread_mutex_lock(&group_data_mutex); // Lock to iterate members safely
          for (int i = 0; i < target_group->member_count; i++) {
            // Check if member is failed
            int is_failed = 0;
            for (int f = 0; f < local_failed_count; ++f) {
              if (local_failed_list[f][0] != '\0' &&
                  strcmp(target_group->members[i].member_id, local_failed_list[f]) == 0) {
                is_failed = 1;
                break;
              }
            }
            // Skip self and failed members
            if (is_failed || strcmp(target_group->members[i].member_id, target_group->my_id_in_group) == 0) {
              continue;
            }

            // Check if this member reported state (using local copy of reported flags -
            // NEED TO COPY FLAGS TOO!) Let's re-read reported flags under
            // vc_state_mutex *before* this calculation loop OR rely on the fact that we
            // only reach here if all *needed* ACKs were received. Assuming
            // vc_state_reported was checked implicitly by sync_acks_received >=
            // sync_acks_needed check.

            // Use the locally copied state for this member index 'i'
            // We need to map member index 'i' to the index used in vc_received_states
            // if they differ. Assuming they are the same index for now.
            if (i < MAX_MEMBERS) {                                 // Bounds check
              if (local_received_states[i] != VC_STATE_SENTINEL) { // Check if state was actually received
                printf("    - State from '%s' (index %d): %lu\n", target_group->members[i].member_id, i,
                       (unsigned long)local_received_states[i]);
                if (local_received_states[i] < min_gseq) {
                  min_gseq = local_received_states[i];
                }
              } else {
                // This should ideally not happen if all needed states were
                // received. Log warning.
                fprintf(stderr,
                        "  -> Warning: State for member '%s' (index %d) was "
                        "expected but not found in local copy.\n",
                        target_group->members[i].member_id, i);
              }
            }
          }
          pthread_mutex_unlock(&group_data_mutex);

          // Store the final agreed GSeq (Need vc_state_mutex again)
          pthread_mutex_lock(&target_group->vc_state_mutex);
          target_group->agreed_delivery_gseq = min_gseq;
          pthread_mutex_unlock(&target_group->vc_state_mutex);

          final_agreed_gseq = min_gseq; // Use calculated value
          printf("  -> Agreement reached! Agreed Delivery GSeq = %lu.\n", (unsigned long)final_agreed_gseq);

          // Send confirmation
          send_view_change_confirm(target_group, final_agreed_gseq);
        } // end if send_confirm_flag

      } else {
        // I am not the coordinator, shouldn't receive STATE messages directly
        fprintf(stderr, "UDP Recv Warning: Received VIEW_CHANGE_STATE but I am not the "
                        "coordinator. Ignoring.\n");
      }
    }
    // End of corrected TYPE_VIEW_CHANGE_STATE handler
    // ==================================================
    // == ΧΕΙΡΙΣΜΟΣ ΓΙΑ TYPE_VIEW_CHANGE_CONFIRM ===
    else if (msg_type == TYPE_VIEW_CHANGE_CONFIRM) {
      // Received confirmation from the coordinator (sender_id)
      // agreed_gseq_from_confirm holds the agreed GSeq
      printf("UDP Recv: Received VIEW_CHANGE_CONFIRM for group '%s' from coordinator '%s' "
             "(AgreedGSeq=%lu).\n",
             group_name, sender_id, (unsigned long)agreed_gseq_from_confirm);

      // Send ACK back to the coordinator (sender_id)
      pthread_mutex_lock(&group_data_mutex); // Need own ID
      char my_id[MAX_ID_LEN + 1];
      strncpy(my_id, target_group->my_id_in_group, MAX_ID_LEN);
      my_id[MAX_ID_LEN] = '\0';
      pthread_mutex_unlock(&group_data_mutex);
      if (strlen(my_id) > 0) {
        uint8_t ack_msg_type = TYPE_VIEW_CHANGE_ACK;
        uint8_t ack_gn_len = strlen(group_name);
        uint8_t ack_sender_len = strlen(my_id);            // I am sending the ACK
        uint8_t ack_acked_type = TYPE_VIEW_CHANGE_CONFIRM; // Acknowledging a CONFIRM msg
        size_t ack_len = 1 + 1 + ack_gn_len + 1 + ack_sender_len + 1;
        unsigned char ack_buf[ack_len];
        unsigned char *ack_ptr = ack_buf;
        memcpy(ack_ptr, &ack_msg_type, 1);
        ack_ptr++;
        memcpy(ack_ptr, &ack_gn_len, 1);
        ack_ptr++;
        memcpy(ack_ptr, group_name, ack_gn_len);
        ack_ptr += ack_gn_len;
        memcpy(ack_ptr, &ack_sender_len, 1);
        ack_ptr++;
        memcpy(ack_ptr, my_id, ack_sender_len);
        ack_ptr += ack_sender_len;
        memcpy(ack_ptr, &ack_acked_type, 1);
        // Send ACK to the original sender of the CONFIRM message (the coordinator)
        sendto(global_udp_socket, ack_buf, ack_len, 0, (struct sockaddr *)&sender_addr, sender_len);
        printf("  -> Sent VIEW_CHANGE_ACK for CONFIRM to %s\n", sender_id);
      }
      // =============================

      pthread_mutex_lock(&target_group->vc_state_mutex);
      // Check if view change is active and we are expecting confirmation (stage 1 or 2)
      if (target_group->view_change_active && target_group->view_change_stage < 3) {
        // TODO: Optionally verify sender_id is the expected coordinator?
        target_group->agreed_delivery_gseq = agreed_gseq_from_confirm;
        target_group->view_change_stage = 3; // Stage 3: Confirm Received / Flushing
        printf("  -> Stored AgreedGSeq=%lu. Stage set to %d. Proceeding to deliver stable "
               "messages.\n",
               (unsigned long)target_group->agreed_delivery_gseq, target_group->view_change_stage);
        // Call the next step (outside mutex)
        pthread_mutex_unlock(&target_group->vc_state_mutex);
        process_stable_messages_for_delivery(target_group, agreed_gseq_from_confirm);
      } else if (target_group->view_change_active && target_group->view_change_stage >= 3) {
        printf("  -> Duplicate or late VIEW_CHANGE_CONFIRM received. Ignoring.\n");
        pthread_mutex_unlock(&target_group->vc_state_mutex);
      } else {
        fprintf(stderr,
                "UDP Recv Warning: Received VIEW_CHANGE_CONFIRM but view change is not "
                "active or in unexpected stage (%d). Ignoring.\n",
                target_group->view_change_stage);
        pthread_mutex_unlock(&target_group->vc_state_mutex);
      }
    } else if (msg_type == TYPE_VIEW_CHANGE_ACK) {
      // == ΧΕΙΡΙΣΜΟΣ ΓΙΑ TYPE_VIEW_CHANGE_ACK ===
      // Received an ACK for a previously sent START, STATE, or CONFIRM message
      // sender_id is the ID of the member who sent the ACK
      // acked_msg_type indicates which message type is being acknowledged
      printf("UDP Recv: Received VIEW_CHANGE_ACK for group '%s' from member '%s' (Acking "
             "Type: %u).\n",
             group_name, sender_id, acked_msg_type);

      pthread_mutex_lock(&target_group->vc_pending_ack_mutex);
      int found_pending = 0;
      for (int i = 0; i < VC_PENDING_ACKS_SIZE; i++) {
        PendingControlAck *p_ctrl_ack = &target_group->vc_pending_acks[i];
        // Check if active, matches the sender who sent the ACK, and matches the message
        // type being ACKed
        if (p_ctrl_ack->active == 1 && strcmp(p_ctrl_ack->target_member_id, sender_id) == 0 &&
            p_ctrl_ack->message_type_sent == acked_msg_type) {
          printf("  -> Found matching pending control ACK entry. Deactivating.\n");
          p_ctrl_ack->active = 0; // Mark as acknowledged
          // Optionally clear other fields if needed, or let retransmission thread ignore
          // inactive entries
          found_pending = 1;
          // Since we track ACK per recipient per message type, we can break here.
          // If we needed to track ACKs for multiple messages of the same type to the same
          // target, we would need a more specific identifier (e.g., a sequence number for
          // control messages).
          break;
        }
      }
      pthread_mutex_unlock(&target_group->vc_pending_ack_mutex);

      if (!found_pending) {
        printf("  -> Received potentially duplicate or unexpected VIEW_CHANGE_ACK from %s "
               "for type %u.\n",
               sender_id, acked_msg_type);
      }
      // =================================================
    }
    // == ΧΕΙΡΙΣΜΟΣ ΓΙΑ TYPE_VIEW_CHANGE_RESTART ===
    else if (msg_type == TYPE_VIEW_CHANGE_RESTART) {
      // sender_id is the new coordinator (C')
      printf("UDP Recv: Received VIEW_CHANGE_RESTART from new coordinator '%s'. Parsed %d "
             "failed IDs.\n",
             sender_id, parsed_failed_count);

      // Send ACK back to the new coordinator (sender_id)
      pthread_mutex_lock(&group_data_mutex); // Need own ID
      char my_id[MAX_ID_LEN + 1];
      strncpy(my_id, target_group->my_id_in_group, MAX_ID_LEN);
      my_id[MAX_ID_LEN] = '\0';
      pthread_mutex_unlock(&group_data_mutex);
      if (strlen(my_id) > 0) {
        uint8_t ack_msg_type = TYPE_VIEW_CHANGE_ACK;
        uint8_t ack_gn_len = strlen(group_name);
        uint8_t ack_sender_len = strlen(my_id);            // I am sending the ACK
        uint8_t ack_acked_type = TYPE_VIEW_CHANGE_RESTART; // Acknowledging a RESTART msg
        size_t ack_len = 1 + 1 + ack_gn_len + 1 + ack_sender_len + 1;
        unsigned char ack_buf[ack_len];
        unsigned char *ack_ptr = ack_buf;
        memcpy(ack_ptr, &ack_msg_type, 1);
        ack_ptr++;
        memcpy(ack_ptr, &ack_gn_len, 1);
        ack_ptr++;
        memcpy(ack_ptr, group_name, ack_gn_len);
        ack_ptr += ack_gn_len;
        memcpy(ack_ptr, &ack_sender_len, 1);
        ack_ptr++;
        memcpy(ack_ptr, my_id, ack_sender_len);
        ack_ptr += ack_sender_len;
        memcpy(ack_ptr, &ack_acked_type, 1);
        sendto(global_udp_socket, ack_buf, ack_len, 0, (struct sockaddr *)&sender_addr, sender_len);
        printf("  -> Sent VIEW_CHANGE_ACK for RESTART to %s\n", sender_id);
      }

      // Update local VC state
      pthread_mutex_lock(&target_group->vc_state_mutex);
      printf("  -> Resetting local VC state for restart.\n");
      target_group->view_change_active = 1; // Ensure active
      target_group->view_change_stage = 2;  // Ready to send state
      // Update the failed list based on the received list
      target_group->vc_failed_member_count = parsed_failed_count;
      for (int i = 0; i < parsed_failed_count && i < MAX_MEMBERS; ++i) {
        strncpy(target_group->vc_failed_member_list[i], parsed_failed_list[i], MAX_ID_LEN);
        target_group->vc_failed_member_list[i][MAX_ID_LEN - 1] = '\0';
      }
      // Clear remaining slots in list
      for (int i = parsed_failed_count; i < MAX_MEMBERS; ++i)
        target_group->vc_failed_member_list[i][0] = '\0';

      target_group->sync_acks_received = 0;
      target_group->sync_acks_needed = 0;
      for (int k = 0; k < MAX_MEMBERS; ++k) {
        target_group->vc_received_states[k] = VC_STATE_SENTINEL;
        target_group->vc_state_reported[k] = 0;
      }
      // TODO: Clear pending control ACKs related to members in the failed list? More complex.
      pthread_mutex_unlock(&target_group->vc_state_mutex);

      // Send state to the NEW coordinator (sender_id)
      printf("  -> Preparing to send state to NEW coordinator '%s'.\n", sender_id);
      send_view_change_state(target_group, sender_id);
    }
    // ==================================================
    else {
      fprintf(stderr, "UDP Recv: Received unknown message type %u. Discarding.\n", msg_type);
    }

  } // end while keep_running

  printf("UDP Receiver thread exiting.\n");
  return NULL;
}

// --- Υλοποίηση grp_init (Πλήρης) ---

/*
 * Αρχικοποιεί τη βιβλιοθήκη, συνδέεται μόνιμα TCP με GMS, δημιουργεί UDP socket,
 * αρχικοποιεί την ουρά απαντήσεων και ξεκινά το background thread για TCP μηνύματα.
 */
int grp_init(char *gms_addr, int gms_port) {
  if (library_initialized) {
    fprintf(stderr, "Warning: Library already initialized.\n");
    return 0;
  }

  printf("Initializing group communication library...\n");

  // 1. Αρχικοποίηση του πίνακα των groups
  printf("Initializing global groups array (max %d groups)...\n", MAX_GROUPS);
  for (int i = 0; i < MAX_GROUPS; i++) {
    global_groups[i].is_active = 0;
    memset(global_groups[i].group_name, 0, MAX_ID_LEN);
    memset(global_groups[i].my_id_in_group, 0, MAX_ID_LEN);
    global_groups[i].member_count = 0;
    global_groups[i].next_send_seq = 0;
    memset(global_groups[i].vector_clock, 0, sizeof(global_groups[i].vector_clock));
    // Αρχικοποίηση ουράς μηνυμάτων UDP για αυτό το group
    global_groups[i].msg_head = 0;
    global_groups[i].msg_tail = 0;
    global_groups[i].msg_count = 0;
    if (pthread_mutex_init(&global_groups[i].msg_mutex, NULL) != 0) {
      perror("Failed to initialize group message mutex");
      // Cleanup προηγούμενων groups; Πιο πολύπλοκο... Ας τερματίσουμε.
      return -1;
    }
    if (pthread_cond_init(&global_groups[i].msg_cond, NULL) != 0) {
      perror("Failed to initialize group message condition variable");
      // Cleanup...
      pthread_mutex_destroy(&global_groups[i].msg_mutex);
      return -1;
    }
    // --- Αρχικοποίηση πίνακα Pending ACKs ---
    for (int k = 0; k < PENDING_ACKS_SIZE; k++) {
      global_groups[i].pending_acks[k].active = 0;
      clear_pending_ack_entry(&global_groups[i].pending_acks[k]);
    }
    if (pthread_mutex_init(&global_groups[i].pending_ack_mutex, NULL) != 0) {
      perror("Failed to initialize pending ACK mutex");
      return -1;
    }
    // ------------------------------------------
    // --- Αρχικοποίηση Numbering-Sequencer State ---
    global_groups[i].next_global_seq = 0;
    global_groups[i].expected_global_seq = 0;
    global_groups[i].unstable_count = 0;
    for (int k = 0; k < UNSTABLE_BUFFER_SIZE; ++k) {
      global_groups[i].unstable_data_buffer[k].active = 0;
    }
    if (pthread_mutex_init(&global_groups[i].unstable_mutex, NULL) != 0) {
      return -1;
    }
    // -------------------------------------------
    for (int j = 0; j < MAX_MEMBERS; j++) {
      memset(global_groups[i].members[j].member_id, 0, MAX_ID_LEN);
      memset(global_groups[i].members[j].address, 0, MAX_ADDR_LEN);
      global_groups[i].members[j].port = -1;
      global_groups[i].members[j].expected_recv_seq = 0; // <-- Αρχικοποίηση
      global_groups[i].members[j].ooo_count = 0;
      for (int k = 0; k < OUT_OF_ORDER_BUFFER_SIZE; ++k)
        global_groups[i].members[j].ooo_buffer[k].active = 0;
    }
    // --- Αρχικοποίηση State Transfer Sync ---
    global_groups[i].state_transfer_complete = 0;
    if (pthread_mutex_init(&global_groups[i].state_transfer_mutex, NULL) != 0) {
      return -1;
    }
    if (pthread_cond_init(&global_groups[i].state_transfer_cond, NULL) != 0) {
      pthread_mutex_destroy(&global_groups[i].state_transfer_mutex);
      return -1;
    }
    // -------------------------------------------
    // === NEW: Initialize View Change State ===
    global_groups[i].view_change_active = 0;
    global_groups[i].view_change_stage = 0;
    global_groups[i].agreed_delivery_gseq = 0;
    global_groups[i].sync_acks_needed = 0;
    global_groups[i].sync_acks_received = 0;
    global_groups[i].vc_failed_member_count = 0;
    memset(global_groups[i].vc_failed_member_list, 0, sizeof(global_groups[i].vc_failed_member_list));
    for (int k = 0; k < MAX_MEMBERS; k++) {
      global_groups[i].vc_received_states[k] = VC_STATE_SENTINEL;
      global_groups[i].vc_state_reported[k] = 0;
    }
    if (pthread_mutex_init(&global_groups[i].vc_state_mutex, NULL) != 0) {
      perror("Failed to initialize view change state mutex"); /* Cleanup */
      return -1;
    }

    // Initialize VC Control ACK tracking
    for (int k = 0; k < VC_PENDING_ACKS_SIZE; k++) {
      global_groups[i].vc_pending_acks[k].active = 0;
    }
    if (pthread_mutex_init(&global_groups[i].vc_pending_ack_mutex, NULL) != 0) {
      perror("Failed to initialize VC pending ACK mutex"); /* Cleanup */
      return -1;
    }
    // =======================================
    // === NEW: Initialize Pending SEQ_ASSIGNMENT State ===
    for (int k = 0; k < PENDING_SEQ_ASSIGNMENTS_SIZE; k++) {
      global_groups[i].pending_seq_assignments[k].active = 0;
      // Optionally clear other fields if needed, though active=0 is sufficient
      global_groups[i].pending_seq_assignments[k].original_local_seq_no = 0;
      global_groups[i].pending_seq_assignments[k].original_sender_id[0] = '\0';
      global_groups[i].pending_seq_assignments[k].group_name[0] = '\0';
      global_groups[i].pending_seq_assignments[k].retry_count = 0;
    }
    if (pthread_mutex_init(&global_groups[i].pending_seq_assignment_mutex, NULL) != 0) {
      perror("Failed to initialize pending sequence assignment mutex"); /* Cleanup */
      // Add cleanup for previously initialized mutexes/condvars if necessary
      return -1;
    }
    // ==============================================
    for (int k = 0; k < UNSTABLE_BUFFER_SIZE; k++) {
      global_groups[i].unstable_data_buffer[k].global_sequence_number = UNASSIGNED_GLOBAL_SEQ;
    }
  }

  // 2. Αρχικοποίηση της Ουράς Απαντήσεων GMS kai udp
  init_gms_queue();

  // 3. Δημιουργία του global UDP socket
  printf("Creating global UDP socket...\n");
  global_udp_socket = socket(AF_INET, SOCK_DGRAM, 0);
  if (global_udp_socket < 0) {
    fprintf(stderr, "Error: Failed to create global UDP socket.\n");
    destroy_gms_queue(); // Cleanup queue
    return -1;
  }

  // 4. Bind του UDP socket
  memset(&my_udp_addr, 0, sizeof(my_udp_addr));
  my_udp_addr.sin_family = AF_INET;
  my_udp_addr.sin_addr.s_addr = htonl(INADDR_ANY);
  my_udp_addr.sin_port = htons(0);
  if (bind(global_udp_socket, (struct sockaddr *)&my_udp_addr, sizeof(my_udp_addr)) < 0) {
    fprintf(stderr, "Error: Failed to bind global UDP socket.\n");
    close(global_udp_socket);
    global_udp_socket = -1;
    destroy_gms_queue();
    return -1;
  }
  socklen_t len = sizeof(my_udp_addr);
  if (getsockname(global_udp_socket, (struct sockaddr *)&my_udp_addr, &len) < 0) {
    fprintf(stderr, "Error: getsockname failed for UDP socket.\n");
    close(global_udp_socket);
    global_udp_socket = -1;
    destroy_gms_queue();
    return -1;
  }
  printf("Global UDP socket bound to port %d\n", ntohs(my_udp_addr.sin_port));

  // 5. Δημιουργία της global *μόνιμης* TCP σύνδεσης με τον GMS
  printf("Establishing global TCP connection to GMS at %s:%d...\n", gms_addr, gms_port);
  gms_tcp_socket = socket(AF_INET, SOCK_STREAM, 0);
  if (gms_tcp_socket < 0) {
    fprintf(stderr, "Error: Failed to create TCP socket for GMS.\n");
    close(global_udp_socket);
    global_udp_socket = -1;
    destroy_gms_queue();
    return -1;
  }
  struct sockaddr_in gms_server_addr;
  memset(&gms_server_addr, 0, sizeof(gms_server_addr));
  gms_server_addr.sin_family = AF_INET;
  gms_server_addr.sin_port = htons(gms_port);
  if (inet_pton(AF_INET, gms_addr, &gms_server_addr.sin_addr) <= 0) {
    fprintf(stderr, "Error: Invalid GMS address format.\n");
    close(gms_tcp_socket);
    close(global_udp_socket);
    gms_tcp_socket = -1;
    global_udp_socket = -1;
    destroy_gms_queue();
    return -1;
  }
  if (connect(gms_tcp_socket, (struct sockaddr *)&gms_server_addr, sizeof(gms_server_addr)) < 0) {
    fprintf(stderr, "Error: Failed to connect to GMS.\n");
    close(gms_tcp_socket);
    close(global_udp_socket);
    gms_tcp_socket = -1;
    global_udp_socket = -1;
    destroy_gms_queue();
    return -1;
  }
  printf("Successfully connected to GMS via TCP (socket %d). This connection remains open.\n", gms_tcp_socket);

  // arxikopoihsh shmatoforwn
  sem_init(&join_mtx, 0, 0);
  sem_init(&leave_mtx, 0, 0);

  // 6. Εκκίνηση του TCP Handler Thread kai udp receiver
  printf("Starting TCP Handler thread...\n");
  keep_tcp_handler_running = 1;
  if (pthread_create(&tcp_handler_thread_id, NULL, tcp_handler_func, NULL) != 0) {
    perror("Error creating TCP handler thread");
    close(gms_tcp_socket);
    close(global_udp_socket);
    gms_tcp_socket = -1;
    global_udp_socket = -1;
    destroy_gms_queue();
    return -1;
  }
  // --- Εκκίνηση UDP Receiver Thread ---
  keep_udp_receiver_running = 1; // <-- flag
  if (pthread_create(&udp_receiver_thread_id, NULL, udp_receiver_func, NULL) != 0) {
    perror("Error creating UDP receiver thread");
    // Σταμάτα το TCP thread που ήδη ξεκίνησε
    keep_tcp_handler_running = 0;
    shutdown(gms_tcp_socket, SHUT_RDWR); // Ξύπνα το recv του TCP handler
    close(gms_tcp_socket);
    pthread_join(tcp_handler_thread_id, NULL); // Περίμενε να τερματίσει
    close(global_udp_socket);
    gms_tcp_socket = -1;
    global_udp_socket = -1;
    destroy_gms_queue();
    return -1;
  }
  // --- Εκκίνηση Retransmission Thread ---
  keep_retransmission_thread_running = 1;
  if (pthread_create(&retransmission_thread_id, NULL, retransmission_thread_func, NULL) != 0) {
    perror("Error creating Retransmission thread");
    // Σταμάτα τα άλλα threads που ήδη ξεκίνησαν
    keep_tcp_handler_running = 0;
    shutdown(gms_tcp_socket, SHUT_RDWR);
    close(gms_tcp_socket);
    pthread_join(tcp_handler_thread_id, NULL);
    keep_udp_receiver_running = 0;
    shutdown(global_udp_socket, SHUT_RD);
    pthread_join(udp_receiver_thread_id, NULL);
    close(global_udp_socket);
    gms_tcp_socket = -1;
    global_udp_socket = -1;
    destroy_gms_queue();
    return -1;
  }
  printf("TCP Handler, UDP receiver and Retransmission threads created successfully.\n");

  for (int i = 0; i < MAX_GROUPS; i++) {
    active_handles[i] = -1; // Αρχικοποίηση με μη έγκυρη τιμή
  }

  library_initialized = 1;
  printf("Group communication library initialized successfully.\n");
  return 0; // Επιτυχία
}

// --- Τροποποιημένη grp_join (Λαμβάνει από Ουρά & Κάνει Parsing) ---

/*
 * Προσθέτει το process σε ένα group. Στέλνει JOIN (binary),
 * περιμένει σήμα από τον handler, διαβάζει την απάντηση από την ουρά,
 * την επεξεργάζεται, και εκτυπώνει τα μέλη του group αν πετύχει.
 */
int grp_join(char *grpname, char *myid) {
  if (!library_initialized) {
    fprintf(stderr, "Error: Library not initialized.\n");
    return -1;
  }
  uint8_t id_len = (myid == NULL) ? 0 : strlen(myid);
  uint8_t group_len = (grpname == NULL) ? 0 : strlen(grpname);
  if (id_len == 0 || id_len > MAX_ID_LEN || group_len == 0 || group_len > MAX_ID_LEN) {
    fprintf(stderr, "Error: Invalid or too long group name or member ID.\n");
    return -1;
  }

  printf("grp_join called for group '%s' with id '%s'.\n", grpname, myid);

  int assigned_group_index = -1;
  int join_result = -1;
  unsigned char response_buffer[GMS_BUFFER_SIZE]; // Buffer για να διαβάσουμε από την ουρά
  int response_len;
  member_t members_from_gms[MAX_MEMBERS]; // Temp storage for GMS members

  pthread_mutex_lock(&group_data_mutex); // Κλείδωμα για έλεγχο/ενημέρωση global_groups

  // 1. Έλεγχος αν υπάρχει ελεύθερο slot και αν το group υπάρχει ήδη
  int free_slot = -1;
  for (int i = 0; i < MAX_GROUPS; i++) {
    if (global_groups[i].is_active) {
      if (strcmp(global_groups[i].group_name, grpname) == 0) {
        printf("This process has already joined this group '%s'!\n", grpname);
        pthread_mutex_unlock(&group_data_mutex);
        return -1;
      }
    } else if (free_slot == -1) {
      free_slot = i;
    }
  }
  if (free_slot == -1) {
    fprintf(stderr, "Error: No available group slots.\n");
    pthread_mutex_unlock(&group_data_mutex);
    return -1;
  }
  printf("Found free slot for group at index %d.\n", free_slot);
  // Κράτα το όνομα στο slot για μελλοντική χρήση/έλεγχο
  strncpy(global_groups[free_slot].group_name, grpname, MAX_ID_LEN - 1);
  global_groups[free_slot].group_name[MAX_ID_LEN - 1] = '\0';
  // Δεν το μαρκάρουμε ως active ακόμα

  // 2. Εύρεση IP Client (Placeholder)
  char my_ip_addr_str[MAX_ADDR_LEN];
  // TODO: Υλοποίησε μια συνάρτηση για να βρεις τη σωστή IP.
  strcpy(my_ip_addr_str, "127.0.0.1");             // --- ΠΡΟΣΟΧΗ: PLACEHOLDER ---
  uint16_t my_udp_port_net = my_udp_addr.sin_port; // Είναι ήδη σε network byte order

  // 3. Σύνταξη μηνύματος JOIN (binary)
  size_t msg_len = sizeof(uint8_t) * 3 + sizeof(uint16_t) + id_len + group_len;
  unsigned char request_buffer[msg_len];
  unsigned char *ptr = request_buffer;
  uint8_t msg_type = MSG_TYPE_JOIN;
  memcpy(ptr, &msg_type, sizeof(uint8_t));
  ptr += sizeof(uint8_t);
  memcpy(ptr, &id_len, sizeof(uint8_t));
  ptr += sizeof(uint8_t);
  memcpy(ptr, &group_len, sizeof(uint8_t));
  ptr += sizeof(uint8_t);
  memcpy(ptr, &my_udp_port_net, sizeof(uint16_t));
  ptr += sizeof(uint16_t);
  memcpy(ptr, myid, id_len);
  ptr += id_len;
  memcpy(ptr, grpname, group_len);
  ptr += group_len;

  // 4. Αποστολή μηνύματος JOIN
  if (send(gms_tcp_socket, request_buffer, msg_len, 0) < 0) {
    perror("Error sending JOIN request to GMS");
    memset(global_groups[free_slot].group_name, 0, MAX_ID_LEN); // Καθάρισε το όνομα
    pthread_mutex_unlock(&group_data_mutex);
    return -1;
  }

  while ((response_len = dequeue_gms_response(response_buffer, sizeof(response_buffer))) == 0) {
    printf("downing the sem\n");
    pthread_mutex_unlock(&group_data_mutex);
    sem_wait(&join_mtx);
    pthread_mutex_lock(&group_data_mutex);
    printf("sem was upped(not by join)\n");
  }

  // 7. Parsing της απάντησης (που έγινε dequeue)
  if (response_len > 0) {
    unsigned char *current_ptr = response_buffer;
    int remaining_bytes = response_len;
    uint8_t response_type = *current_ptr++;
    remaining_bytes--;

    if (response_type == RESP_ACK_SUCCESS) {
      // --- Parsing ACK_SUCCESS ---
      if (remaining_bytes >= sizeof(uint16_t)) {
        uint16_t num_other_members_net;
        memcpy(&num_other_members_net, current_ptr, sizeof(uint16_t));
        current_ptr += sizeof(uint16_t);
        remaining_bytes -= sizeof(uint16_t);
        uint16_t num_other_members = ntohs(num_other_members_net);

        global_groups[free_slot].is_active = 1;
        global_groups[free_slot].member_count = 0; // Ξεκινάμε άδειο

        int members_parsed_count = 0;
        memset(members_from_gms, 0, sizeof(members_from_gms));
        // --- Πρώτα βάζουμε τα μέλη από τον GMS ---
        for (int m = 0; m < num_other_members; m++) {
          if (global_groups[free_slot].member_count >= MAX_MEMBERS) {
            fprintf(stderr, "Join Warning: Max members reached while parsing GMS members.\n");
            break;
          }
          // Έλεγχοι μεγέθους
          if (remaining_bytes < sizeof(uint32_t) + sizeof(uint16_t) + sizeof(uint8_t)) {
            break;
          }
          uint32_t member_ip_net;
          uint16_t member_port_net;
          uint8_t member_id_len;
          memcpy(&member_ip_net, current_ptr, sizeof(uint32_t));
          current_ptr += sizeof(uint32_t);
          remaining_bytes -= sizeof(uint32_t);
          memcpy(&member_port_net, current_ptr, sizeof(uint16_t));
          current_ptr += sizeof(uint16_t);
          remaining_bytes -= sizeof(uint16_t);
          member_id_len = *current_ptr++;
          remaining_bytes--;
          if (remaining_bytes < member_id_len) {
            break;
          }
          if (member_id_len > MAX_ID_LEN - 1) {
            current_ptr += member_id_len;
            remaining_bytes -= member_id_len;
            continue;
          }

          // Parsing
          struct in_addr member_ip_addr;
          member_ip_addr.s_addr = member_ip_net;
          uint16_t member_port = ntohs(member_port_net);
          char member_id_str[MAX_ID_LEN];
          memcpy(member_id_str, current_ptr, member_id_len);
          member_id_str[member_id_len] = '\0';
          current_ptr += member_id_len;
          remaining_bytes -= member_id_len;

          // Αποθήκευση του μέλους από GMS
          int current_member_index = global_groups[free_slot].member_count;
          member_t *p_member = &global_groups[free_slot].members[current_member_index];
          strncpy(p_member->member_id, member_id_str, MAX_ID_LEN - 1);
          p_member->member_id[MAX_ID_LEN - 1] = '\0';
          p_member->port = member_port;
          if (inet_ntop(AF_INET, &member_ip_addr, p_member->address, MAX_ADDR_LEN) == NULL) {
            strcpy(p_member->address, "0.0.0.0");
          }
          // --- Αρχικοποίηση expected_recv_seq & ooo_buffer ---
          global_groups[free_slot].members[current_member_index].expected_recv_seq = 0;
          global_groups[free_slot].members[current_member_index].ooo_count = 0;
          for (int k = 0; k < OUT_OF_ORDER_BUFFER_SIZE; ++k)
            global_groups[free_slot].members[current_member_index].ooo_buffer[k].active = 0;
          global_groups[free_slot].vector_clock[current_member_index] = 0;
          global_groups[free_slot].member_count++;

          strncpy(members_from_gms[members_parsed_count].member_id, member_id_str, MAX_ID_LEN - 1);
          members_from_gms[members_parsed_count].member_id[MAX_ID_LEN - 1] = '\0';
          members_from_gms[members_parsed_count].port = member_port;

          members_parsed_count++;
          // ------------------------------------------------------
        } // --- Τέλος for για μέλη από GMS ---

        // --- Τώρα προσθέτουμε τον εαυτό μας στο τέλος (αν χωράει) ---
        if (global_groups[free_slot].member_count < MAX_MEMBERS) {
          int self_index = global_groups[free_slot].member_count;
          member_t *self = &global_groups[free_slot].members[self_index];
          strncpy(self->member_id, myid, MAX_ID_LEN - 1);
          self->member_id[MAX_ID_LEN - 1] = '\0';
          strncpy(self->address, my_ip_addr_str, MAX_ADDR_LEN - 1);
          self->address[MAX_ADDR_LEN - 1] = '\0';
          self->port = ntohs(my_udp_port_net); // Μετατροπή της πόρτας μας σε host order για αποθήκευση
          // --- Αρχικοποίηση expected_recv_seq & ooo_buffer ---
          self->expected_recv_seq = 0;
          self->ooo_count = 0;
          for (int k = 0; k < OUT_OF_ORDER_BUFFER_SIZE; ++k)
            self->ooo_buffer[k].active = 0;
          global_groups[free_slot].vector_clock[self_index] = 0;
          global_groups[free_slot].member_count++;
          // ------------------------------------------------------
          printf("  -> Added self to members[%d]: ID='%s', IP=%s, Port=%d\n", self_index, self->member_id,
                 self->address, self->port);
        } else {
          fprintf(stderr, "Join Warning: Max members reached, could not add self to list!\n");
          // Θεωρούμε το join επιτυχές ούτως ή άλλως, απλά ο client δεν είναι στη λίστα
          // του! Αυτό πιθανόν να δημιουργήσει προβλήματα αργότερα.
        }

        // Έλεγχος αν διαβάστηκαν σωστά τα μέλη από GMS
        if (members_parsed_count == num_other_members) {
          strncpy(global_groups[free_slot].my_id_in_group, myid, MAX_ID_LEN - 1);
          global_groups[free_slot].my_id_in_group[MAX_ID_LEN - 1] = '\0';
          // --- Αρχικοποίηση sequence number ---
          global_groups[free_slot].next_send_seq = 0;
          // ---------------------------------------
          // --- Αρχικοποίηση Numbering-Sequencer counters ---
          global_groups[free_slot].next_global_seq = 0;
          global_groups[free_slot].expected_global_seq = 0;
          global_groups[free_slot].unstable_count = 0; // Init new buffer count
          // ----------------------------------------

          // --- Μηδενισμός του υπόλοιπου VC ---
          // --- Μηδενισμός του υπόλοιπου VC (αν MAX_MEMBERS > member_count) ---
          for (int k = global_groups[free_slot].member_count; k < MAX_MEMBERS; k++) {
            global_groups[free_slot].vector_clock[k] = 0;
          }
          // ----------------------------------------------------------------------
          global_groups[free_slot].state_transfer_complete = 0; // Mark state as not yet received
          join_result = free_slot;
          // Αποθήκευση του handle στον πίνακα
          int stored = 0;
          for (int i = 0; i < MAX_GROUPS; i++) {
            if (active_handles[i] == -1) {
              active_handles[i] = free_slot;
              stored = 1;
              break;
            }
          }
          if (stored) {
            active_handle_count++;
          }
          assigned_group_index = free_slot;
          printf("Join: Successfully joined group '%s'. Handle: %d\n", grpname, assigned_group_index);

          // --- Εκτύπωση λίστας μελών ---
          printf("--------------------------------------------------\n");
          printf("Group '%s' (Handle: %d) Member List:\n", global_groups[assigned_group_index].group_name,
                 assigned_group_index);
          for (int i = 0; i < global_groups[assigned_group_index].member_count; i++) {
            member_t *m = &global_groups[assigned_group_index].members[i];
            printf("  [%d] ID: %s, Address: %s:%d\n", i, m->member_id, m->address, m->port);
          }
          printf("--------------------------------------------------\n");
          // --- Τέλος εκτύπωσης ---

          // --- Αποστολή STATE_REQUEST στον Sequencer & Αναμονή ---
          // --- State Transfer Logic (Implementing User's Request) ---
          if (num_other_members > 0) {     // Perform state transfer if others exist
            char *target_member_id = NULL; // ID of member to ask for state
            struct sockaddr_in target_addr;
            int found_target_addr = 0;
            group_t *current_group_ptr = &global_groups[free_slot];

            // Determine who the *new* sequencer is (based on the final list including
            // self)
            char *new_sequencer_id = get_sequencer_id(current_group_ptr);

            if (new_sequencer_id && strcmp(new_sequencer_id, myid) == 0) {
              // I AM the new sequencer. Find the *previous* sequencer from the GMS
              // list.
              printf("Join: I am the new sequencer.\n");
              // Use the helper function on the list of *other* members received from
              // GMS
              target_member_id = find_sequencer_in_list(members_from_gms, num_other_members);
              if (target_member_id) {
                printf("Join: Requesting state from previous sequencer %s\n", target_member_id);
              } else {
                fprintf(stderr, "Join Error: Cannot find previous sequencer among "
                                "other members.\n");
                join_result = -1; // Mark join as failed
              }
            } else if (new_sequencer_id) {
              // I am NOT the new sequencer. Ask the current (and previous) sequencer.
              target_member_id = strdup(new_sequencer_id); // Need a copy
              printf("Join: Requesting state from current sequencer %s\n", target_member_id);
            } else {
              fprintf(stderr, "Join Error: Cannot determine sequencer for state request.\n");
              join_result = -1; // Mark join as failed
            }

            if (new_sequencer_id)
              free(new_sequencer_id); // Free the copy from get_sequencer_id

            // Proceed only if we have a valid target and join hasn't failed
            if (target_member_id && join_result != -1) {
              // Find the address of the target member in the *final* members list
              for (int i = 0; i < current_group_ptr->member_count; ++i) {
                if (strcmp(current_group_ptr->members[i].member_id, target_member_id) == 0) {
                  memset(&target_addr, 0, sizeof(target_addr));
                  target_addr.sin_family = AF_INET;
                  target_addr.sin_port = htons(current_group_ptr->members[i].port);
                  if (inet_pton(AF_INET, current_group_ptr->members[i].address, &target_addr.sin_addr) > 0) {
                    found_target_addr = 1;
                  }
                  break;
                }
              }

              if (found_target_addr) {
                // Build STATE_REQUEST message
                uint8_t req_type = TYPE_STATE_REQUEST;
                uint8_t req_grp_len = strlen(grpname);
                uint8_t req_my_id_len = strlen(myid);
                size_t req_len = 1 + 1 + req_grp_len + 1 + req_my_id_len;
                unsigned char state_req_buf[req_len];
                unsigned char *req_ptr = state_req_buf;
                memcpy(req_ptr, &req_type, 1);
                req_ptr++;
                memcpy(req_ptr, &req_grp_len, 1);
                req_ptr++;
                memcpy(req_ptr, grpname, req_grp_len);
                req_ptr += req_grp_len;
                memcpy(req_ptr, &req_my_id_len, 1);
                req_ptr++;
                memcpy(req_ptr, myid, req_my_id_len);

                // Send request
                sendto(global_udp_socket, state_req_buf, req_len, 0, (struct sockaddr *)&target_addr,
                       sizeof(target_addr));
                atomic_fetch_add(&proto_state_request_sent_count, 1);
                printf("Join: Sent STATE_REQUEST to %s\n", target_member_id);

                // Wait for state response using timed wait (Corrected wait logic)
                pthread_mutex_lock(&current_group_ptr->state_transfer_mutex);

                while (!current_group_ptr->state_transfer_complete) {
                  printf("Join: Waiting for STATE_RESPONSE...\n");
                  pthread_mutex_unlock(&group_data_mutex); // Unlock global while waiting
                  pthread_cond_wait(&current_group_ptr->state_transfer_cond, &current_group_ptr->state_transfer_mutex);
                  pthread_mutex_lock(&group_data_mutex); // Unlock global while waiting
                }
                pthread_mutex_unlock(&current_group_ptr->state_transfer_mutex);

                printf("Join: Received STATE_RESPONSE. Synchronization complete.\n");
                // State was updated by udp_receiver_func

              } else {
                fprintf(stderr,
                        "Join Error: Could not find address for state transfer "
                        "target %s.\n",
                        target_member_id);
                join_result = -1;
              }
              free(target_member_id); // Free the copy from find_sequencer_in_list or
                                      // strdup
            }
            // else: join_result already set to -1 if target_member_id was NULL or
            // new_sequencer_id was NULL

          } else { // num_other_members == 0
            printf("Join: No other members, no state transfer needed.\n");
            global_groups[free_slot].state_transfer_complete = 1; // Mark as complete
          }
          // --------------------------------------------------------

        } else {
          fprintf(stderr, "Join Error: Failed to parse all members from ACK_SUCCESS.\n");
          global_groups[free_slot].is_active = 0;
          global_groups[free_slot].member_count = 0;
          memset(global_groups[free_slot].group_name, 0, MAX_ID_LEN);
          join_result = -1;
        }
      } else {
        fprintf(stderr, "Join Error: Received ACK_SUCCESS but with insufficient data for members.\n");
        join_result = -1;
      }
    } else if (response_type == RESP_NACK_DUPLICATE_ID) {
      // --- Parsing NACK --- (ίδιο με πριν)
      fprintf(stderr, "Join: Received NACK (type %u) from GMS.\n", response_type);
      char error_msg[256] = "Unknown Error";
      if (remaining_bytes >= sizeof(uint8_t)) {
        uint8_t error_msg_len = *current_ptr++;
        remaining_bytes--;
        if (error_msg_len > 0 && error_msg_len < sizeof(error_msg)) {
          if (remaining_bytes >= error_msg_len) {
            memcpy(error_msg, current_ptr, error_msg_len);
            error_msg[error_msg_len] = '\0';
          } else {
            fprintf(stderr, "Join Error: Incomplete NACK error message data.\n");
          }
        }
      }
      fprintf(stderr, "GMS Error Message: %s\n", error_msg);
      join_result = -1;
    } else {
      fprintf(stderr, "Join Error: Dequeued unknown message type (%u).\n", response_type);
      join_result = -1;
    }
  } else if (response_len == 0) {
    fprintf(stderr, "Join Error: Woke up but GMS response queue was empty or GMS connection closed!\n");
    join_result = -1;
  } else { // response_len < 0
    fprintf(stderr, "Join Error: Failed to dequeue GMS response (buffer too small?).\n");
    join_result = -1;
  }

  // Cleanup slot on failure
  if (join_result == -1 && free_slot != -1) {
    if (global_groups[free_slot].is_active != 1) {
      global_groups[free_slot].is_active = 0;
      memset(global_groups[free_slot].group_name, 0, MAX_ID_LEN);
      global_groups[free_slot].member_count = 0;
    }
  }

  if (join_result) {
  }
  pthread_mutex_unlock(&group_data_mutex);

  return join_result;
}

int grp_leave(int g) {
  if (!library_initialized) {
    fprintf(stderr, "Leave Error: Library not initialized.\n");
    return -1;
  }
  if (g < 0 || g >= MAX_GROUPS) {
    fprintf(stderr, "Leave Error: Invalid group handle %d.\n", g);
    return -1;
  }

  // Έλεγχος αν αυτό το handle είναι όντως ενεργό
  int handle_is_active = 0;
  for (int i = 0; i < MAX_GROUPS; ++i) {
    if (active_handles[i] == g) {
      handle_is_active = 1;
      break;
    }
  }
  if (!handle_is_active) {
    printf("  Handle %d is not currently active.\n", g);
    return -1;
  }

  unsigned char response_buffer[GMS_BUFFER_SIZE];
  int response_len;
  int leave_result = -1;
  char group_name_copy[MAX_ID_LEN + 1];
  char member_id_copy[MAX_ID_LEN + 1]; // Χρειαζόμαστε το ID μας!

  pthread_mutex_lock(&group_data_mutex);

  if (!global_groups[g].is_active) {
    fprintf(stderr, "Leave Error: Group handle %d is not active.\n", g);
    pthread_mutex_unlock(&group_data_mutex);
    return -1;
  }

  if (strlen(global_groups[g].my_id_in_group) > 0) {
    strncpy(member_id_copy, global_groups[g].my_id_in_group, MAX_ID_LEN);
    member_id_copy[MAX_ID_LEN] = '\0'; // Ensure null termination
  } else {
    fprintf(stderr,
            "Leave Error: Cannot determine own member ID for group handle %d (my_id_in_group "
            "is empty).\n",
            g);
    pthread_mutex_unlock(&group_data_mutex);
    return -1; // Δεν ξέρουμε ποιο ID να στείλουμε
  }
  strncpy(group_name_copy, global_groups[g].group_name, MAX_ID_LEN);
  group_name_copy[MAX_ID_LEN] = '\0';

  printf("grp_leave called for group '%s' (handle %d) with id '%s'.\n", group_name_copy, g, member_id_copy);

  // Σύνταξη μηνύματος LEAVE (binary)
  uint8_t id_len = strlen(member_id_copy);
  uint8_t group_len = strlen(group_name_copy);
  size_t msg_len = sizeof(uint8_t) * 3 + id_len + group_len;
  unsigned char request_buffer[msg_len];
  unsigned char *ptr = request_buffer;
  uint8_t msg_type = MSG_TYPE_LEAVE;

  memcpy(ptr, &msg_type, sizeof(uint8_t));
  ptr += sizeof(uint8_t);
  memcpy(ptr, &id_len, sizeof(uint8_t));
  ptr += sizeof(uint8_t);
  memcpy(ptr, &group_len, sizeof(uint8_t));
  ptr += sizeof(uint8_t);
  memcpy(ptr, member_id_copy, id_len);
  ptr += id_len;
  memcpy(ptr, group_name_copy, group_len);
  ptr += group_len;

  // Αποστολή μηνύματος LEAVE
  if (send(gms_tcp_socket, request_buffer, msg_len, 0) < 0) {
    perror("Error sending LEAVE request to GMS");
    pthread_mutex_unlock(&group_data_mutex);
    return -1;
  }

  while ((response_len = dequeue_gms_response(response_buffer, sizeof(response_buffer))) == 0) {
    printf("downing the sem\n");
    pthread_mutex_unlock(&group_data_mutex);
    sem_wait(&leave_mtx);
    pthread_mutex_lock(&group_data_mutex);
    printf("sem was upped(not by leave)\n");
  }

  // Parsing της απάντησης
  if (response_len > 0) {
    uint8_t response_type = response_buffer[0];
    if (response_type == RESP_ACK_SUCCESS) {
      // --- Έλεγχος μεγέθους για LEAVE ACK ---
      if (response_len == 1) {
        printf("Leave: Successfully left group '%s'.\n", group_name_copy);
        // Σημείωσε το slot ως ανενεργό
        global_groups[g].is_active = 0;
        global_groups[g].member_count = 0;
        memset(global_groups[g].group_name, 0, MAX_ID_LEN);
        // Δεν χρειάζεται να καθαρίσουμε τον πίνακα members εδώ, θα γίνει στην επόμενη join
        leave_result = g; // Επιτυχία
        // Αφαίρεση του handle από τον πίνακα
        for (int i = 0; i < MAX_GROUPS; i++) {
          if (active_handles[i] == g) {
            active_handles[i] = -1; // Σήμανση ως μη έγκυρο
            active_handle_count--;
            break;
          }
        }
      } else {
        fprintf(stderr,
                "Leave Error: Received ACK_SUCCESS but with unexpected length %d (expected "
                "1).\n",
                response_len);
        leave_result = -1;
      }
    } else if (response_type == RESP_NACK_NOT_IN_GROUP) {
      char error_msg[256] = {0};
      uint8_t error_len = 0;
      memcpy(&error_len, response_buffer + sizeof(uint8_t), sizeof(uint8_t));
      memcpy(error_msg, response_buffer + 2 * sizeof(uint8_t), error_len);
      error_msg[error_len] = '\0';
      fprintf(stderr, "GMS Error Message: %s\n", error_msg);
      leave_result = -1;
    }
  } else {
    fprintf(stderr, "Leave Error: Failed to dequeue response or GMS connection closed.\n");
    leave_result = -1;
  }

  pthread_mutex_unlock(&group_data_mutex);
  return leave_result; // 0 για επιτυχία, -1 για λάθος
}

/*
 * Στέλνει ένα μήνυμα στην ομάδα g χρησιμοποιώντας το μοντέλο Numbering-Only Sequencer.
 * 1. Κάνει reliable multicast το μήνυμα ως TYPE_DATA_UNSTABLE σε όλα τα μέλη.
 * 2. Στέλνει ένα μικρό μήνυμα TYPE_SEQ_REQUEST στον sequencer.
 */
int grp_send(int g, void *msg, int len) {
  if (!library_initialized) {
    fprintf(stderr, "Send Error: Library not initialized.\n");
    return -1;
  }
  if (g < 0 || g >= MAX_GROUPS) {
    fprintf(stderr, "Send Error: Invalid group handle %d.\n", g);
    return -1;
  }
  if (msg == NULL || len <= 0) {
    fprintf(stderr, "Send Error: Invalid message or length.\n");
    return -1;
  }

  int members_sent_to = 0;
  int errors_occurred = 0;
  uint32_t current_local_seq_no;
  char group_name_copy[MAX_GROUP_LEN + 1];
  char my_id_copy[MAX_ID_LEN + 1];
  int member_count_copy;
  member_t members_copy[MAX_MEMBERS];
  int current_vector_clock[MAX_MEMBERS];
  int my_member_index = -1;
  char *sequencer_id_copy = NULL;
  struct sockaddr_in sequencer_addr;
  int found_seq_addr = 0;
  unsigned char *data_buffer = NULL; // Buffer for DATA_UNSTABLE
  size_t data_total_len = 0;
  int pending_slot = -1;

  // --- Προετοιμασία (πάρε state υπό κλείδωμα) ---
  pthread_mutex_lock(&group_data_mutex);
  if (!global_groups[g].is_active) {
    pthread_mutex_unlock(&group_data_mutex);
    return -1;
  }
  group_t *target_group = &global_groups[g];

  // Αντιγραφή βασικών στοιχείων
  strncpy(group_name_copy, target_group->group_name, MAX_GROUP_LEN);
  group_name_copy[MAX_GROUP_LEN] = '\0';
  strncpy(my_id_copy, target_group->my_id_in_group, MAX_ID_LEN);
  my_id_copy[MAX_ID_LEN] = '\0';
  member_count_copy = target_group->member_count;
  memcpy(members_copy, target_group->members, sizeof(member_t) * member_count_copy);

  // Βρες τον δικό μας index
  for (int i = 0; i < member_count_copy; i++) {
    if (strcmp(members_copy[i].member_id, my_id_copy) == 0) {
      my_member_index = i;
      break;
    }
  }
  if (my_member_index == -1) {
    pthread_mutex_unlock(&group_data_mutex);
    return -1;
  } // Should not happen

  // Βρες τον sequencer και τη διεύθυνσή του
  sequencer_id_copy = get_sequencer_id(target_group); // Παίρνει αντίγραφο
  if (!sequencer_id_copy) {
    pthread_mutex_unlock(&group_data_mutex);
    return -1;
  } // No members?
  for (int i = 0; i < member_count_copy; ++i) {
    if (strcmp(members_copy[i].member_id, sequencer_id_copy) == 0) {
      memset(&sequencer_addr, 0, sizeof(sequencer_addr));
      sequencer_addr.sin_family = AF_INET;
      sequencer_addr.sin_port = htons(members_copy[i].port);
      if (inet_pton(AF_INET, members_copy[i].address, &sequencer_addr.sin_addr) > 0) {
        found_seq_addr = 1;
      }
      break;
    }
  }
  if (!found_seq_addr) {
    fprintf(stderr, "Send Error: Could not find sequencer address for ID '%s'\n", sequencer_id_copy);
    pthread_mutex_unlock(&group_data_mutex);
    free(sequencer_id_copy);
    return -1;
  }

  // Αύξησε το τοπικό VC και πάρε τον τοπικό seq no
  target_group->vector_clock[my_member_index]++;
  memcpy(current_vector_clock, target_group->vector_clock, sizeof(target_group->vector_clock));
  current_local_seq_no = target_group->next_send_seq++;

  pthread_mutex_unlock(&group_data_mutex); // Ξεκλείδωσε το global mutex
  // ---------------------------------------------------------------------

  atomic_fetch_add(&app_messages_sent_count, 1);

  if (strlen(my_id_copy) == 0) {
    free(sequencer_id_copy);
    return -1;
  }

  // --- 1. Κατασκευή και Αποστολή DATA_UNSTABLE (Reliable Multicast) ---
  uint8_t data_msg_type = TYPE_DATA_UNSTABLE;
  uint8_t group_name_len = strlen(group_name_copy);
  uint8_t sender_id_len = strlen(my_id_copy);
  uint32_t seq_no_net = htonl(current_local_seq_no);
  int vector_clock_bytes = sizeof(int) * MAX_MEMBERS;

  // Header: Type(1) + GroupLen(1) + Group + SenderLen(1) + Sender + LocalSeq(4) + VC(N*4) +
  // Payload
  size_t data_header_len = 1 + 1 + group_name_len + 1 + sender_id_len + sizeof(uint32_t) + vector_clock_bytes;
  data_total_len = data_header_len + len;

  if (data_total_len <= UDP_BUFFER_SIZE) {
    data_buffer = malloc(data_total_len);
    if (!data_buffer) {
      perror("malloc failed for data buffer");
      free(sequencer_id_copy);
      return -1;
    }
    unsigned char *ptr = data_buffer;
    // Serialize Header
    memcpy(ptr, &data_msg_type, sizeof(uint8_t));
    ptr += sizeof(uint8_t);
    memcpy(ptr, &group_name_len, sizeof(uint8_t));
    ptr += sizeof(uint8_t);
    memcpy(ptr, group_name_copy, group_name_len);
    ptr += group_name_len;
    memcpy(ptr, &sender_id_len, sizeof(uint8_t));
    ptr += sizeof(uint8_t);
    memcpy(ptr, my_id_copy, sender_id_len);
    ptr += sender_id_len;
    memcpy(ptr, &seq_no_net, sizeof(uint32_t));
    ptr += sizeof(uint32_t);
    for (int i = 0; i < MAX_MEMBERS; i++) {
      int vc_entry_net = htonl(current_vector_clock[i]);
      memcpy(ptr, &vc_entry_net, sizeof(int));
      ptr += sizeof(int);
    }
    // Copy Payload
    memcpy(ptr, msg, len);
  } else {
    fprintf(stderr, "Send Error: DATA_UNSTABLE message too large.\n");
    free(sequencer_id_copy);
    return -1;
  }

  // Προσθήκη στα pending ACKs
  pthread_mutex_lock(&target_group->pending_ack_mutex);
  pending_slot = -1;
  for (int i = 0; i < PENDING_ACKS_SIZE; ++i) {
    if (!target_group->pending_acks[i].active) {
      pending_slot = i;
      break;
    }
  }
  if (pending_slot == -1) {
    fprintf(stderr, "Send Error: Pending ACK buffer full! Try resending the message.\n");
    free(data_buffer);
    data_buffer = NULL;
    errors_occurred++;
    if(sequencer_id_copy) free(sequencer_id_copy);
    pthread_mutex_unlock(&target_group->pending_ack_mutex);
    return -1;
  }
  if (pending_slot != -1) {
    PendingAckInfo *p_ack = &target_group->pending_acks[pending_slot];
    clear_pending_ack_entry(p_ack);
    p_ack->seq_no = current_local_seq_no;
    p_ack->active = 1;
    p_ack->sent_time = time(NULL);
    p_ack->message_data = data_buffer;
    p_ack->message_len = data_total_len;           // Store the data buffer
    p_ack->original_msg_type = TYPE_DATA_UNSTABLE; // Mark type
    p_ack->target_member_count = member_count_copy;
    p_ack->acks_received_count = 0;
    p_ack->target_member_cap = member_count_copy > 0 ? member_count_copy : 1;
    p_ack->acked_member_cap = INITIAL_MEMBER_LIST_CAPACITY;
    p_ack->target_member_ids = malloc(p_ack->target_member_cap * sizeof(char *));
    p_ack->acked_member_ids = malloc(p_ack->acked_member_cap * sizeof(char *));
    if (!p_ack->target_member_ids || !p_ack->acked_member_ids) {
      clear_pending_ack_entry(p_ack);
      data_buffer = NULL;
      errors_occurred++;
    } // Frees buffer too
    else {
      for (int k = 0; k < p_ack->target_member_cap; ++k)
        p_ack->target_member_ids[k] = NULL;
      for (int k = 0; k < p_ack->acked_member_cap; ++k)
        p_ack->acked_member_ids[k] = NULL;
      int current_target_count = 0;
      int strdup_failed = 0;
      for (int i = 0; i < member_count_copy; ++i) { // Use copied member list
        if (current_target_count < p_ack->target_member_cap) {
          p_ack->target_member_ids[current_target_count] = strdup(members_copy[i].member_id);
          if (!p_ack->target_member_ids[current_target_count]) {
            strdup_failed = 1;
            break;
          }
          current_target_count++;
        } else {
          strdup_failed = 1;
          break;
        }
      }
      if (strdup_failed) {
        clear_pending_ack_entry(p_ack);
        data_buffer = NULL;
        errors_occurred++;
      } else {
        p_ack->target_member_count = current_target_count;
        printf("Send: Added DATA_UNSTABLE LocalSeq=%u to pending ACKs for %d members\n", current_local_seq_no,
               p_ack->target_member_count);
      }
    }
  }
  pthread_mutex_unlock(&target_group->pending_ack_mutex);

  // *** NEW: Track the need for SEQ_ASSIGNMENT ***
  pthread_mutex_lock(&target_group->pending_seq_assignment_mutex);
  int assign_slot = -1;
  for (int i = 0; i < PENDING_SEQ_ASSIGNMENTS_SIZE; ++i) {
    if (!target_group->pending_seq_assignments[i].active) {
      assign_slot = i;
      break;
    }
  }
  if (assign_slot != -1) {
    PendingSeqAssignmentInfo *p_assign = &target_group->pending_seq_assignments[assign_slot];
    p_assign->active = 1;
    p_assign->original_local_seq_no = current_local_seq_no;
    strncpy(p_assign->original_sender_id, my_id_copy, MAX_ID_LEN - 1); // It's *my* message
    p_assign->original_sender_id[MAX_ID_LEN - 1] = '\0';
    strncpy(p_assign->group_name, group_name_copy, MAX_GROUP_LEN - 1); // Need group name for potential resend
    p_assign->group_name[MAX_GROUP_LEN - 1] = '\0';
    p_assign->request_sent_time = time(NULL); // Will be updated on resends
    p_assign->retry_count = 0;
    printf("Send: Tracking SEQ_REQUEST for LocalSeq=%u (Slot %d).\n", current_local_seq_no, assign_slot);
  } else {
    fprintf(stderr,
            "Send Error: Pending Sequence Assignment buffer full! Cannot track request for LocalSeq=%u. Try resending "
            "the message.\n",
            current_local_seq_no);
    // Critical error: Data sent, but cannot ensure sequencing.
    // Should we clean up the pending ack entry too? Probably not, data might still arrive elsewhere.
    errors_occurred++; // Indicate an error occurred
    if(sequencer_id_copy) free(sequencer_id_copy);
    pthread_mutex_unlock(&target_group->pending_seq_assignment_mutex);
    return -1;
  }
  pthread_mutex_unlock(&target_group->pending_seq_assignment_mutex);
  // *** END NEW Tracking ***

  if (!data_buffer) {
    free(sequencer_id_copy);
    return -1;
  } // If adding to pending failed

  // Αποστολή DATA_UNSTABLE σε όλα τα μέλη
  printf("Send: Multicasting DATA_UNSTABLE LocalSeq=%u...\n", current_local_seq_no);

  srand(time(NULL) ^ getpid() ^ clock());

  for (int i = 0; i < member_count_copy; i++) {
    member_t *dest_member = &members_copy[i];
    if (dest_member->address[0] == '\0' || dest_member->port <= 0)
      continue;
    struct sockaddr_in dest_addr;
    memset(&dest_addr, 0, sizeof(dest_addr));
    dest_addr.sin_family = AF_INET;
    dest_addr.sin_port = htons(dest_member->port);
    if (inet_pton(AF_INET, dest_member->address, &dest_addr.sin_addr) <= 0)
      continue;

    // simultate packet loss
    double random_val = (double)rand() / (double)RAND_MAX;

    if (random_val >= LOSS_PROBABILITY) {
      ssize_t sent_bytes = sendto(global_udp_socket, data_buffer, data_total_len, MSG_DONTWAIT,
                                  (struct sockaddr *)&dest_addr, sizeof(dest_addr));
      atomic_fetch_add(&proto_data_unstable_sent_count, 1);
      if (sent_bytes < 0) {
        errors_occurred++;
        printf("send_bytes < 0, send failed");
      } else {
        members_sent_to++;
      }
    } else {
      printf("packet lost...\n");
    }
  }

  // --- 2. Κατασκευή και Αποστολή SEQ_REQUEST ---
  uint8_t req_msg_type = TYPE_SEQ_REQUEST;
  uint8_t req_group_name_len = strlen(group_name_copy);
  uint8_t req_sender_id_len = strlen(my_id_copy);        // Original sender ID
  uint32_t req_seq_no_net = htonl(current_local_seq_no); // Original local seq no

  // Header: Type(1) + GroupLen(1) + Group + SenderLen(1) + Sender(Original) +
  // LocalSeq(4)(Original)
  size_t req_header_len = 1 + 1 + req_group_name_len + 1 + req_sender_id_len + sizeof(uint32_t);
  unsigned char *request_buffer = malloc(req_header_len);
  if (!request_buffer) {
    perror("malloc failed for request buffer");
    free(sequencer_id_copy);
    return -1; /* Or just skip request? */
  }
  unsigned char *req_ptr = request_buffer;

  // Serialize Header
  memcpy(req_ptr, &req_msg_type, sizeof(uint8_t));
  req_ptr += sizeof(uint8_t);
  memcpy(req_ptr, &req_group_name_len, sizeof(uint8_t));
  req_ptr += sizeof(uint8_t);
  memcpy(req_ptr, group_name_copy, req_group_name_len);
  req_ptr += req_group_name_len;
  memcpy(req_ptr, &req_sender_id_len, sizeof(uint8_t));
  req_ptr += sizeof(uint8_t);
  memcpy(req_ptr, my_id_copy, req_sender_id_len);
  req_ptr += req_sender_id_len;
  memcpy(req_ptr, &req_seq_no_net, sizeof(uint32_t));

  // Send ONLY to sequencer
  printf("Send: Sending SEQ_REQUEST for LocalSeq=%u to sequencer '%s'\n", current_local_seq_no, sequencer_id_copy);
  ssize_t sent_bytes = sendto(global_udp_socket, request_buffer, req_header_len, MSG_DONTWAIT,
                              (struct sockaddr *)&sequencer_addr, sizeof(sequencer_addr));
  atomic_fetch_add(&proto_seq_request_sent_count, 1);
  if (sent_bytes < 0) {
    perror("Send Warning: sendto failed sending SEQ_REQUEST to sequencer");
    // Continue anyway, rely on potential retransmission of DATA_UNSTABLE? Or fail? Let's
    // continue.
    errors_occurred++;
  }
  free(request_buffer); // Free request buffer immediately

  free(sequencer_id_copy); // Free the sequencer ID copy
  return len;              // Return original payload length on success (even if request failed)
}

/*
 * Λαμβάνει το επόμενο στοιχείο (DATA ή NOTIFICATION) για την ομάδα g.
 * Διαβάζει από την τελική ουρά message_buffer, η οποία περιέχει
 * notifications ή μηνύματα DATA που έχουν περάσει όλους τους ελέγχους
 * (reliability, causal, total order) και είναι έτοιμα για παράδοση.
 * Ενημερώνει το τοπικό Vector Clock κατά την παράδοση μηνύματος DATA.
 * Χειρίζεται blocking/non-blocking.
 *
 * Επιστρέφει:
 * > 0: Αριθμός bytes του μηνύματος DATA που αντιγράφηκε στο msg.
 * 0: Αν block=0 και δεν υπήρχε τίποτα, Ή αν παραδόθηκε NOTIFICATION.
 * -1: Σε περίπτωση σφάλματος.
 */
int grp_recv(int g, void *msg, int *len, int block) {
  if (!library_initialized) {
    fprintf(stderr, "Recv Error: Library not initialized.\n");
    return -1;
  }
  if (g < 0 || g >= MAX_GROUPS) {
    fprintf(stderr, "Recv Error: Invalid group handle %d.\n", g);
    return -1;
  }
  if (msg == NULL || len == NULL) {
    fprintf(stderr, "Recv Error: Invalid buffer or length pointer.\n");
    return -1;
  }

  pthread_mutex_lock(&group_data_mutex);
  if (!global_groups[g].is_active) {
    pthread_mutex_unlock(&group_data_mutex);
    *len = 0;
    return -1;
  }
  pthread_mutex_unlock(&group_data_mutex);

  unsigned char *app_buffer = (unsigned char *)msg;
  int app_buffer_size = UDP_BUFFER_SIZE; // Assumption - app must provide large enough buffer
  int result = -1;
  *len = 0; // Initialize length to 0

  // --- Βρόχος για Blocking / Έλεγχο της τελικής ουράς ---
  pthread_mutex_lock(&global_groups[g].msg_mutex);
  while (1) {
    if (global_groups[g].msg_count > 0) {
      GroupQueueItem *item_slot = &global_groups[g].message_buffer[global_groups[g].msg_head];
      if (item_slot->internal_type == MSG_BUFFER_TYPE_NOTIFY) {
        // --- Handle Notification ---
        int item_len = item_slot->len;
        printf("grp_recv(g=%d): Delivering NOTIFICATION message (len=%d)\n", g, item_len);
        char formatted_string[MAX_ID_LEN + MAX_ADDR_LEN + 50];
        result = -1;
        unsigned char *notify_data = item_slot->data;
        int notify_remaining = item_len;
        uint8_t subtype = 0;
        char notify_id[MAX_ID_LEN + 1] = {0};
        char notify_ip[MAX_ADDR_LEN + 1] = {0};
        int notify_port = 0;
        int parse_ok = 1;
        /* ... (Parsing logic for notification - same as before) ... */
        if (notify_remaining >= 1) {
          subtype = *notify_data++;
          notify_remaining--;
        } else {
          parse_ok = 0;
        }
        if (parse_ok && notify_remaining >= 1) {
          uint8_t id_l = *notify_data++;
          notify_remaining--;
          if (id_l > 0 && id_l < MAX_ID_LEN && notify_remaining >= id_l) {
            memcpy(notify_id, notify_data, id_l);
            notify_id[id_l] = '\0';
            notify_data += id_l;
            notify_remaining -= id_l;
          } else {
            parse_ok = 0;
          }
        } else if (parse_ok) {
          parse_ok = 0;
        }
        if (parse_ok && notify_remaining >= 1) {
          uint8_t ip_l = *notify_data++;
          notify_remaining--;
          if (ip_l > 0 && ip_l < MAX_ADDR_LEN && notify_remaining >= ip_l) {
            memcpy(notify_ip, notify_data, ip_l);
            notify_ip[ip_l] = '\0';
            notify_data += ip_l;
            notify_remaining -= ip_l;
          } else {
            parse_ok = 0;
          }
        } else if (parse_ok) {
          parse_ok = 0;
        }
        if (parse_ok && notify_remaining >= sizeof(uint16_t)) {
          uint16_t port_net;
          memcpy(&port_net, notify_data, sizeof(uint16_t));
          notify_port = ntohs(port_net);
        } else if (parse_ok) {
          parse_ok = 0;
        }
        if (parse_ok) {
          const char *type_str = "UNKNOWN";
          if (subtype == NOTIFY_SUBTYPE_JOINED)
            type_str = "JOIN";
          else if (subtype == NOTIFY_SUBTYPE_LEFT)
            type_str = "LEAVE";
          else if (subtype == NOTIFY_SUBTYPE_FAILED)
            type_str = "FAIL";
          snprintf(formatted_string, sizeof(formatted_string), "[%s] ID=%s Addr=%s:%d", type_str, notify_id, notify_ip,
                   notify_port);
          int formatted_len = strlen(formatted_string);
          if (formatted_len < app_buffer_size) {
            memcpy(app_buffer, formatted_string, formatted_len + 1);
            *len = formatted_len;
            result = 0;
          } // Return 0 for Notification
          else {
            fprintf(stderr, "Recv Error: App buffer too small for notification!\n");
            result = -1;
          }
        } else {
          fprintf(stderr, "grp_recv: Error parsing stored notification!\n");
          result = -1;
        }
      } else if (item_slot->internal_type == MSG_BUFFER_TYPE_DATA) {
        // --- Handle Data Message ---
        printf("grp_recv(g=%d): Delivering DATA message GlobalSeq=%lu (LocalSeq=%u "
               "Sender=%s)\n",
               g, (unsigned long)item_slot->global_sequence_number, item_slot->sequence_number, item_slot->sender_id);
        if (item_slot->len <= app_buffer_size) {
          memcpy(app_buffer, item_slot->data, item_slot->len);
          *len = item_slot->len;
          result = *len; // Return positive length for DATA
          atomic_fetch_add(&app_messages_recvd_count, 1);
          // printf("len is %d and buf is %s", *len, app_buffer);
          //  --- Update Local Vector Clock UPON DELIVERY ---
          pthread_mutex_unlock(&global_groups[g].msg_mutex);
          pthread_mutex_lock(&group_data_mutex);
          if (global_groups[g].is_active) { // Check if group still active
            // Merge the VC that came with the original DATA_UNSTABLE message
            for (int k = 0; k < global_groups[g].member_count; k++) {
              if (item_slot->received_vector_clock[k] > global_groups[g].vector_clock[k]) {
                global_groups[g].vector_clock[k] = item_slot->received_vector_clock[k];
              }
            }
          }
          pthread_mutex_unlock(&group_data_mutex);
          pthread_mutex_lock(&global_groups[g].msg_mutex);
          // -------------------------------------------
        } else {
          fprintf(stderr, "Recv Error: Application buffer too small for DATA message (%d > %d)!\n", item_slot->len,
                  app_buffer_size);
          result = -1;
          // Do not consume the message if app buffer was too small
          pthread_mutex_unlock(&global_groups[g].msg_mutex);
          return result;
        }
      } else {
        // Should not happen
        fprintf(stderr, "Recv Error: Unknown internal message type in delivery queue: %u\n", item_slot->internal_type);
        result = -1;
      }
      global_groups[g].msg_head = (global_groups[g].msg_head + 1) % GROUP_MSG_QUEUE_SIZE;
      global_groups[g].msg_count--;
      break;
    } else {
      if (block == 0) {
        *len = 0;
        result = -1;
        break;
      } else {
        pthread_cond_wait(&global_groups[g].msg_cond, &global_groups[g].msg_mutex);
      }
    }
  }
  pthread_mutex_unlock(&global_groups[g].msg_mutex);
  return result;
}

/* --- Άλλες Συναρτήσεις (grp_leave, grp_send, grp_recv, grp_shutdown) --- */
/* --- Δεν αλλάζουν σε αυτή την έκδοση, εκτός αν χρειάζεται να καλέσουν --- */
/* --- τη νέα binary μορφή για το LEAVE κλπ.                             --- */

/*
 * Τερματίζει τη βιβλιοθήκη, κλείνει συνδέσεις, σταματά threads,
 * και καταστρέφει ουρές και sync primitives.
 */
void grp_shutdown() {
  printf("Shutting down group communication library...\n");

  // Σταμάτημα Threads
  if (keep_retransmission_thread_running) { // <-- Σταμάτα πρώτα αυτό; Ή τελευταίο; Ας είναι
                                            // πρώτο.
    printf("Stopping Retransmission thread...\n");
    keep_retransmission_thread_running = 0;
    // Δεν χρειάζεται να κάνουμε κάτι για να το ξυπνήσουμε, χρησιμοποιεί sleep()
    pthread_join(retransmission_thread_id, NULL);
    printf("Retransmission thread joined.\n");
  }
  if (keep_tcp_handler_running) { /* ... (ίδιο με πριν) ... */
    printf("Stopping TCP Handler thread...\n");
    keep_tcp_handler_running = 0;
    if (gms_tcp_socket != -1) {
      shutdown(gms_tcp_socket, SHUT_RDWR);
      close(gms_tcp_socket);
      gms_tcp_socket = -1;
    }
    pthread_join(tcp_handler_thread_id, NULL);
    printf("TCP Handler thread joined.\n");
  } else {
    if (gms_tcp_socket != -1) {
      close(gms_tcp_socket);
      gms_tcp_socket = -1;
    }
  }

  if (keep_udp_receiver_running) { /* ... (ίδιο με πριν) ... */
    printf("Stopping UDP Receiver thread...\n");
    keep_udp_receiver_running = 0;
    if (global_udp_socket != -1) {
      shutdown(global_udp_socket, SHUT_RD);
    } // Κλείσε για διάβασμα
    pthread_join(udp_receiver_thread_id, NULL);
    printf("UDP Receiver thread joined.\n");
  }

  // Κλείσιμο UDP socket
  if (global_udp_socket != -1) { /* ... (ίδιο με πριν) ... */
    printf("Closing global UDP socket (%d).\n", global_udp_socket);
    close(global_udp_socket);
    global_udp_socket = -1;
  }

  // Καταστροφή Ουράς GMS
  destroy_gms_queue();

  // Καταστροφή Per-Group Structures
  printf("Destroying per-group structures...\n");
  for (int i = 0; i < MAX_GROUPS; i++) {
    pthread_mutex_lock(&global_groups[i].pending_ack_mutex); // Lock needed before clear? Yes.
    for (int k = 0; k < PENDING_ACKS_SIZE; ++k) {
      if (global_groups[i].pending_acks[k].active) {
        clear_pending_ack_entry(&global_groups[i].pending_acks[k]);
      }
    }
    pthread_mutex_unlock(&global_groups[i].pending_ack_mutex);
    // Το mutex καταστράφηκε ήδη, ίσως λάθος σειρά; Ας το κάνουμε πριν:
    pthread_mutex_destroy(&global_groups[i].pending_ack_mutex);
    pthread_mutex_destroy(&global_groups[i].unstable_mutex);
    pthread_mutex_destroy(&global_groups[i].state_transfer_mutex);
    pthread_cond_destroy(&global_groups[i].state_transfer_cond);
    pthread_mutex_destroy(&global_groups[i].msg_mutex);
    pthread_cond_destroy(&global_groups[i].msg_cond);
    pthread_mutex_destroy(&global_groups[i].vc_state_mutex);
    pthread_mutex_destroy(&global_groups[i].vc_pending_ack_mutex); // Destroy new mutex
    pthread_mutex_lock(&global_groups[i].pending_seq_assignment_mutex);
    for (int k = 0; k < PENDING_SEQ_ASSIGNMENTS_SIZE; ++k) {
      global_groups[i].pending_seq_assignments[k].active = 0;
    }
    pthread_mutex_unlock(&global_groups[i].pending_seq_assignment_mutex);
    pthread_mutex_destroy(&global_groups[i].pending_seq_assignment_mutex);
  }
  // Καταστροφή Global Join/Leave Cond Vars (ίδιο με πριν)
  printf("Destroying global semaphores...\n");
  sem_destroy(&join_mtx);
  sem_destroy(&leave_mtx);

  library_initialized = 0;
  printf("Library shutdown complete.\n");
}

// int main() {
//     // Στοιχεία GMS
//     char *gms_ip = "127.0.0.1";
//     int gms_port = 8080; // Βάλε τη σωστή πόρτα του GMS σου

//     // Μεταβλητές για input χρήστη
//     char action_input[10];
//     char my_member_id[MAX_ID_LEN];
//     char group_name_input[MAX_GROUP_LEN];
//     char handle_input[10];
//     char message_buffer[UDP_BUFFER_SIZE]; // Buffer για αποστολή
//     char recv_buffer[UDP_BUFFER_SIZE + 1]; // Buffer για λήψη (+1 για null terminator)
//     int g;
//     int recv_len;
//     int block_flag;

//     size_t len; // Για αφαίρεση newline από fgets

//     // Αρχικοποίηση της βιβλιοθήκης
//     if (grp_init(gms_ip, gms_port) != 0) {
//         fprintf(stderr, "Library initialization failed. Exiting.\n");
//         return 1;
//     }
//     sleep(1);
//     printf("Library initialized successfully.\n");
//     printf("Enter 'join', 'leave', 'send', 'recv', or 'quit'.\n");

//     // Κύριος βρόχος αλληλεπίδρασης
//     while (1) {
//         // --- Εκτύπωση Ενεργών Handles ---
//         printf("\n------------------------------------\n");
//         printf("Active Group Handles: ");
//         if (active_handle_count == 0) {
//             printf("(None)");
//         } else {
//             int first = 1;
//             for (int i = 0; i < MAX_GROUPS; i++) {
//                 if (active_handles[i] != -1) {
//                     if (!first) printf(", ");
//                     printf("%d", active_handles[i]);
//                     first = 0;
//                 }
//             }
//         }
//         printf(" [%d/%d used]\n", active_handle_count, MAX_GROUPS);
//         printf("------------------------------------\n");

//         // --- Λήψη Ενέργειας ---
//         printf("Enter action (join/leave/send/recv/quit): ");
//         if (fgets(action_input, sizeof(action_input), stdin) == NULL) {
//             fprintf(stderr, "Error reading action. Exiting loop.\n");
//             break;
//         }
//         len = strlen(action_input);
//         if (len > 0 && action_input[len - 1] == '\n') { action_input[len - 1] = '\0'; len--; }
//         for (int i = 0; i < len; i++) { action_input[i] = tolower((unsigned
//         char)action_input[i]); }

//         // --- Έλεγχος Ενέργειας ---
//         if (strcmp(action_input, "quit") == 0) {
//             printf("Exit command received.\n");
//             break;
//         }
//         else if (strcmp(action_input, "join") == 0) {
//             // --- JOIN ---
//             if (active_handle_count >= MAX_GROUPS) { /* ... (Max limit error) ... */ continue; }
//             printf("  Enter your member ID: ");
//             if (fgets(my_member_id, sizeof(my_member_id), stdin) == NULL) { break; }
//             len = strlen(my_member_id); if (len > 0 && my_member_id[len - 1] == '\n') {
//             my_member_id[len - 1] = '\0'; } if (strlen(my_member_id) == 0) { printf("  Member ID
//             cannot be empty.\n"); continue; } printf("  Enter group name to join: "); if
//             (fgets(group_name_input, sizeof(group_name_input), stdin) == NULL) { break; } len =
//             strlen(group_name_input); if (len > 0 && group_name_input[len - 1] == '\n') {
//             group_name_input[len - 1] = '\0'; } if (strlen(group_name_input) == 0) { printf("
//             Group name cannot be empty.\n"); continue; }

//             printf("  Attempting to join group '%s' with ID '%s'...\n", group_name_input,
//             my_member_id); int new_handle = grp_join(group_name_input, my_member_id);

//             if (new_handle >= 0) {
//                 printf("  --> Successfully joined group '%s'. Assigned Handle: %d\n",
//                 group_name_input, new_handle);
//             } else { printf("  --> Failed to join group '%s' with ID '%s'.\n", group_name_input,
//             my_member_id); }
//         }
//         else if (strcmp(action_input, "leave") == 0) {
//             // --- LEAVE ---
//             if (active_handle_count <= 0) { printf("You are not currently in any groups to
//             leave.\n"); continue; } printf("  Enter the group handle to leave: "); if
//             (fgets(handle_input, sizeof(handle_input), stdin) == NULL) { break; } errno = 0; char
//             *endptr; g = strtol(handle_input, &endptr, 10); if (errno != 0 || (*endptr != '\n' &&
//             *endptr != '\0') || g < 0 ) { printf("  Invalid handle entered.\n"); continue; }

//             int handle_is_active = 0; for(int i=0; i<MAX_GROUPS; ++i){ if(active_handles[i] ==
//             g){ handle_is_active = 1; break; } } if(!handle_is_active){ printf("  Handle %d is
//             not currently active.\n", g); continue; }

//             printf("  Attempting to leave group with handle %d...\n", g);
//             int leave_result = grp_leave(g);

//             if (leave_result >= 0) { // grp_leave returns handle on success now? Let's assume 0
//             is success based on previous impl.
//                  // Assuming grp_leave returns 0 on success based on previous implementation
//                  if (leave_result >= 0) {
//                      printf("  --> Successfully left group with handle %d.\n", g);
//                  } else {
//                       printf("  --> Failed to leave group with handle %d (Result: %d).\n", g,
//                       leave_result);
//                  }
//             } else { printf("  --> Failed to leave group with handle %d (Result: %d).\n", g,
//             leave_result); }
//         }
//         else if (strcmp(action_input, "send") == 0) {
//             // --- SEND ---
//             if (active_handle_count <= 0) { printf("You must join a group first to send.\n");
//             continue; } printf("  Enter the group handle to send to: "); if (fgets(handle_input,
//             sizeof(handle_input), stdin) == NULL) { break; } errno = 0; char *endptr; g =
//             strtol(handle_input, &endptr, 10);
//              if (errno != 0 || (*endptr != '\n' && *endptr != '\0') || g < 0 ) { printf(" Invalid
//              handle entered.\n"); continue; }

//             int handle_is_active = 0; for(int i=0; i<MAX_GROUPS; ++i){ if(active_handles[i] ==
//             g){ handle_is_active = 1; break; } } if(!handle_is_active){ printf("  Handle %d is
//             not currently active.\n", g); continue; }

//             printf("  Enter message to send: ");
//             if (fgets(message_buffer, sizeof(message_buffer), stdin) == NULL) { break; }
//             len = strlen(message_buffer); if (len > 0 && message_buffer[len - 1] == '\n') {
//             message_buffer[len - 1] = '\0'; } if (strlen(message_buffer) == 0) { printf("  Cannot
//             send empty message.\n"); continue; }

//             printf("  Sending message to group handle %d...\n", g);
//             int send_result = grp_send(g, message_buffer, strlen(message_buffer));

//             if (send_result >= 0) { // Assuming grp_send returns num_bytes or 0 on success
//                 printf("  --> Message sent successfully (%d bytes).\n", send_result);
//             } else {
//                 printf("  --> Failed to send message to group handle %d.\n", g);
//             }
//         }
//         else if (strcmp(action_input, "recv") == 0) {
//             // --- RECV ---
//              if (active_handle_count <= 0) { printf("You must join a group first to receive.\n");
//              continue; }
//             printf("  Enter the group handle to receive from: ");
//             if (fgets(handle_input, sizeof(handle_input), stdin) == NULL) { break; }
//             errno = 0; char *endptr;
//             g = strtol(handle_input, &endptr, 10);
//              if (errno != 0 || (*endptr != '\n' && *endptr != '\0') || g < 0 ) { printf(" Invalid
//              handle entered.\n"); continue; }

//             int handle_is_active = 0; for(int i=0; i<MAX_GROUPS; ++i){ if(active_handles[i] ==
//             g){ handle_is_active = 1; break; } } if(!handle_is_active){ printf("  Handle %d is
//             not currently active.\n", g); continue; }

//             printf("  Block? (yes/no): ");
//             if (fgets(action_input, sizeof(action_input), stdin) == NULL) { break; }
//             len = strlen(action_input); if (len > 0 && action_input[len - 1] == '\n') {
//             action_input[len - 1] = '\0'; len--; } for (int i = 0; i < len; i++) {
//             action_input[i] = tolower((unsigned char)action_input[i]); } block_flag =
//             (strcmp(action_input, "yes") == 0) ? 1 : 0;

//             printf("  Attempting to receive from group handle %d (blocking: %s)...\n", g,
//             block_flag ? "YES" : "NO"); int recv_result = grp_recv(g, recv_buffer, &recv_len,
//             block_flag);

//             if (recv_result > 0) {
//                  recv_buffer[recv_len] = '\0'; // Null-terminate the received data
//                  printf("  --> Received %d bytes: [%s]\n", recv_len, recv_buffer);
//             } else if (recv_result == 0) {
//                  printf("  --> Received notification for group [%d]. Notifiaction: '%s'.\n", g,
//                  recv_buffer);
//             } else {
//                  printf("  --> Failed to receive message from group handle %d.\n", g);
//             }
//         }
//         else {
//             printf("Invalid action. Please enter 'join', 'leave', 'send', 'recv', or 'quit'.\n");
//         }
//         // Ο βρόχος συνεχίζει
//     } // --- Τέλος while(1) ---

//     // Κάλεσμα της shutdown ΜΙΑ φορά πριν τον τερματισμό
//     printf("\nShutting down library...\n");
//     grp_shutdown();
//     printf("Shutdown complete. Exiting program.\n");

//     return 0;
// }