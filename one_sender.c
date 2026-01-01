#include <errno.h>  // For perror
#include <signal.h> // For kill()
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/wait.h> // For waitpid(), wait()
#include <time.h>     // For seeding rand
#include <unistd.h>   // For fork(), sleep(), usleep(), getpid()

// Include your group communication library header
#include "group_comm.h"

// --- ANSI Color Codes ---
#define ANSI_COLOR_RED     "\x1b[31m" // Start Red Text
#define ANSI_COLOR_RESET   "\x1b[0m"  // Reset to Default Color

// --- Configuration Constants ---
#define CLIENT_EXECUTABLE_NAME "c_simulator_proc_nb" // Name for usage message
#define NUM_CLIENTS 1                                // Default number of clients to simulate
#define GROUP_NAME "TestGroupCProcNB"                // Default group name
#define MESSAGES_PER_CLIENT 5                        // Default number of messages each client sends
#define GMS_ADDRESS "192.168.1.10"                      // Default GMS Address
#define GMS_PORT 8080                                // Default GMS Port
#define LOG_FILENAME_MAXLEN (MAX_ID_LEN + 20)        // Space for "log_" + id + ".txt" + null

// Delays
#define POST_JOIN_DELAY_S 3           // Time allowed for all clients to join (approx)
#define POST_SEND_DELAY_S 3        // Time allowed for all clients to send (approx)
#define INTER_MESSAGE_DELAY_US 0 // 100ms between sends per client
#define RECV_LOOP_SLEEP_US 0      // 50ms sleep in non-blocking receive loop
#define RANDOM_FACTOR 0

// --- Function Prototypes ---
void client_logic(int client_id, char *sender_id, char *groupd_id, int sending_duration);

// --- Main Function (Parent Process) ---
int main(int argc, char *argv[]) {
  // --- Basic Argument Parsing (Optional: Override defaults) ---
  int total_clients = 1;
//   if (argc > 1) {
//     total_clients = atoi(argv[1]);
//     if (total_clients <= 0) {
//       fprintf(stderr, ANSI_COLOR_RED "Warning: Invalid number of clients '%s', using default %d." ANSI_COLOR_RESET "\n", argv[1], NUM_CLIENTS);
//       total_clients = NUM_CLIENTS;
//     }
//   }
  char *group_name_to_use = (argc > 2) ? argv[2] : GROUP_NAME;
  int messages_to_send_per_client = MESSAGES_PER_CLIENT;
  if (messages_to_send_per_client < 0)
    messages_to_send_per_client = MESSAGES_PER_CLIENT;

  printf(ANSI_COLOR_RED "Starting C Simulator (Multi-Process, Non-Blocking Recv): %d clients, Group: '%s'" ANSI_COLOR_RESET "\n", // Added newline for clarity
         total_clients, group_name_to_use);

  pid_t pids[total_clients]; // Array to store child PIDs
  int children_created = 0;

  // --- Fork Child Processes ---
  printf(ANSI_COLOR_RED "Forking %d client processes..." ANSI_COLOR_RESET "\n", total_clients);
  for (int i = 0; i < total_clients; ++i) {
    pid_t pid = fork();

    if (pid < 0) {
      // Fork failed
      perror(ANSI_COLOR_RED "ERROR: fork failed" ANSI_COLOR_RESET); // Color the prefix
      // Terminate already created children before exiting
      fprintf(stderr, ANSI_COLOR_RED "Parent: Terminating already created children due to fork failure." ANSI_COLOR_RESET "\n");
      for (int j = 0; j < children_created; ++j) {
        if (pids[j] > 0) {        // Check if PID is valid before killing
          kill(pids[j], SIGTERM); // Send terminate signal
        }
      }
      // Wait briefly for them to terminate
      sleep(1);
      for (int j = 0; j < children_created; ++j) {
        if (pids[j] > 0) {
          waitpid(pids[j], NULL, WNOHANG); // Non-blocking wait attempt
        }
      }
      exit(EXIT_FAILURE);
    } else if (pid == 0) {
      // --- Child Process ---
      srand(time(NULL) ^ getpid()); // Seed random generator
      client_logic(i, argv[1], argv[2], atoi(argv[3]));
      exit(EXIT_SUCCESS); // Exit child successfully
    } else {
      // --- Parent Process ---
      pids[i] = pid; // Store child PID
      children_created++;
      printf(ANSI_COLOR_RED "  Parent: Forked child %d with PID %d" ANSI_COLOR_RESET "\n", i, pid);
      usleep(50000); // Small delay between forks
    }
  }

  // --- Parent Waits for Children (Corrected Loop with Diagnostics) ---
  printf(ANSI_COLOR_RED "Parent: All children forked. Waiting for them to complete..." ANSI_COLOR_RESET "\n");
  int status;
  int completed_children = 0;
  pid_t terminated_pid;

  // Loop until all children have been waited for
  while (completed_children < total_clients) {
    printf(ANSI_COLOR_RED "Parent: Waiting for a child (completed: %d/%d)..." ANSI_COLOR_RESET "\n", completed_children, total_clients);
    fflush(stdout);                 // Ensure message is seen
    terminated_pid = wait(&status); // Wait for ANY child

    if (terminated_pid > 0) {
      printf(ANSI_COLOR_RED "  Parent: wait() returned PID %d." ANSI_COLOR_RESET "\n", terminated_pid);
      completed_children++; // Increment counter *after* successful wait

      // Find which child index this PID corresponds to (for logging purposes)
      int child_index = -1;
      for (int i = 0; i < total_clients; ++i) {
        // Check against the stored PIDs
        if (pids[i] == terminated_pid) {
          child_index = i;
          // Mark as waited for to avoid confusion if wait returns same PID (shouldn't happen)
          // pids[i] = -1; // Let's comment this out, it shouldn't be needed
          break;
        }
      }

      printf(ANSI_COLOR_RED "  Parent: Child process (index %d, PID %d) terminated " ANSI_COLOR_RESET, child_index, terminated_pid); // Part 1
      if (WIFEXITED(status)) {
        printf(ANSI_COLOR_RED "with exit status %d." ANSI_COLOR_RESET "\n", WEXITSTATUS(status)); // Part 2
        if (WEXITSTATUS(status) != 0) {
          fprintf(stderr, ANSI_COLOR_RED "  Parent WARNING: Child %d (PID %d) exited with non-zero status %d!" ANSI_COLOR_RESET "\n", child_index,
                  terminated_pid, WEXITSTATUS(status));
        }
      } else if (WIFSIGNALED(status)) {
        printf(ANSI_COLOR_RED "killed by signal %d." ANSI_COLOR_RESET "\n", WTERMSIG(status)); // Part 2
        fprintf(stderr, ANSI_COLOR_RED "  Parent WARNING: Child %d (PID %d) was killed by signal %d!" ANSI_COLOR_RESET "\n", child_index, terminated_pid,
                WTERMSIG(status));
      } else {
        printf(ANSI_COLOR_RED "with unknown status 0x%X." ANSI_COLOR_RESET "\n", status); // Part 2
      }
      printf(ANSI_COLOR_RED "  Parent: Completed count now %d." ANSI_COLOR_RESET "\n", completed_children);
      fflush(stdout); // Ensure messages are seen

    } else {
      // wait() returns -1 on error
      if (errno == ECHILD) {
        printf(ANSI_COLOR_RED "  Parent: wait() reported ECHILD. No more children expected by wait()." ANSI_COLOR_RESET "\n");
        fflush(stdout);
        break; // Exit the loop
      } else if (errno == EINTR) {
        // Interrupted by a signal, try waiting again
        printf(ANSI_COLOR_RED "  Parent: wait() interrupted by signal, retrying." ANSI_COLOR_RESET "\n");
        fflush(stdout);
        continue; // Continue the loop to wait again
      } else {
        // Other unexpected error
        perror(ANSI_COLOR_RED "  Parent: wait() error" ANSI_COLOR_RESET); // Color prefix
        fprintf(stderr, ANSI_COLOR_RED "  Parent: Unexpected wait() error %d. Breaking wait loop." ANSI_COLOR_RESET "\n", errno);
        fflush(stdout);
        break; // Exit the loop on other errors
      }
    }
  } // End while loop

  // Final check
  if (completed_children != total_clients) {
    fprintf(stderr, ANSI_COLOR_RED "Parent Warning: Expected %d children, but only waited for %d." ANSI_COLOR_RESET "\n", total_clients,
            completed_children);
  } else {
    printf(ANSI_COLOR_RED "Parent: All %d child processes have completed." ANSI_COLOR_RESET "\n", completed_children);
  }

  printf(ANSI_COLOR_RED "C Simulator finished." ANSI_COLOR_RESET "\n");
  printf(ANSI_COLOR_RED "Check log files (e.g., 'log_Client0.txt') for received messages." ANSI_COLOR_RESET "\n");
  return 0;
}

// --- Client Process Logic ---
void client_logic(int client_id, char *sender_id, char *group_id, int sending_duration) {
  char member_id_str[MAX_ID_LEN];
  strncpy(member_id_str, sender_id, MAX_ID_LEN - 1); // Use strncpy for safety, ensure space for null term
  member_id_str[MAX_ID_LEN - 1] = '\0'; // Ensure null termination

  int group_handle = -1;
  int join_success = 0;
  int lib_initialized = 0;

  // *** ADDED: Variables for receiving/polling ***
  unsigned char receive_buffer[UDP_BUFFER_SIZE + 1]; // +1 for null term safety
  int received_len = 0;
  int polled_data_count = 0;
  int polled_notif_count = 0;
  // *** END ADDED ***

  // --- File pointer for logging (if you uncomment logging later) ---
  // FILE *log_fp = NULL;
  // char log_filename[LOG_FILENAME_MAXLEN];
  // unsigned long long delivery_counter = 0; // Use unsigned long long for safety

  printf(ANSI_COLOR_RED "[%s PID: %d] Process started." ANSI_COLOR_RESET "\n", member_id_str, getpid());

  // --- 0. Initialize Library ---
  printf(ANSI_COLOR_RED "[%s] Initializing library..." ANSI_COLOR_RESET "\n", member_id_str);
  if (grp_init(GMS_ADDRESS, GMS_PORT) != 0) {
    fprintf(stderr, ANSI_COLOR_RED "[%s] ERROR: Failed to initialize library." ANSI_COLOR_RESET "\n", member_id_str);
    exit(EXIT_FAILURE);
  }
  lib_initialized = 1;
  printf(ANSI_COLOR_RED "[%s] Library initialized." ANSI_COLOR_RESET "\n", member_id_str);

  // --- 1. Join Group ---
  printf(ANSI_COLOR_RED "[%s] Attempting to join group '%s'..." ANSI_COLOR_RESET "\n", member_id_str, group_id);
  group_handle = grp_join(group_id, member_id_str);

  if (group_handle < 0) {
    fprintf(stderr, ANSI_COLOR_RED "[%s] ERROR: Failed to join group '%s' (grp_join returned %d)." ANSI_COLOR_RESET "\n", member_id_str, group_id, group_handle);
    join_success = 0;
  } else {
    printf(ANSI_COLOR_RED "[%s] Successfully joined group '%s' with handle %d." ANSI_COLOR_RESET "\n", member_id_str, group_id, group_handle);
    join_success = 1;

    // Uncomment this section if you add logging later
    /*
    // --- Open Log File ---
    snprintf(log_filename, LOG_FILENAME_MAXLEN, "log_%s.txt", member_id_str);
    log_fp = fopen(log_filename, "w");
    if (log_fp == NULL) {
      fprintf(stderr, ANSI_COLOR_RED "[%s] ERROR: Could not open log file '%s': " ANSI_COLOR_RESET, member_id_str, log_filename);
      perror(ANSI_COLOR_RED "System error" ANSI_COLOR_RESET); // Color the prefix for perror
    } else {
      printf(ANSI_COLOR_RED "[%s] Logging received DATA to '%s'." ANSI_COLOR_RESET "\n", member_id_str, log_filename);
    }
    */
  }

  // --- Approximate Synchronization Point 1 (Wait after Join) ---
  if (join_success) {
    printf(ANSI_COLOR_RED "[%s] Joined. Waiting %d seconds before sending/polling..." ANSI_COLOR_RESET "\n", member_id_str, POST_JOIN_DELAY_S);
    sleep(POST_JOIN_DELAY_S);
  } else {
    goto cleanup; // Skip to cleanup if join failed
  }

  // --- 2. Combined Send & Receive Polling Loop ---
  printf(ANSI_COLOR_RED "[%s] Starting send/poll loop for %d seconds..." ANSI_COLOR_RESET "\n", member_id_str, sending_duration);
  srand(time(NULL) ^ getpid() ^ clock());
  int messages_sent_count = 0; // Renamed 'i' for clarity

  time_t start, now;
  int seconds_elapsed;

  time(&start);
  while (1) {
    // Check elapsed time first
    time(&now);
    seconds_elapsed = difftime(now, start);
    if (seconds_elapsed >= sending_duration) {
      printf(ANSI_COLOR_RED "[%s] Sending/polling duration ended after %d seconds." ANSI_COLOR_RESET "\n", member_id_str, sending_duration);
      break; // Exit the loop
    }

    // *** ADDED: Receive Polling Attempt (Non-Blocking) ***
    int recv_result = grp_recv(group_handle, receive_buffer, &received_len, 0); // block = 0

    if (recv_result > 0) {
      // Data received during polling
      polled_data_count++;
      // Optional: Log that sender received something, potentially its own message or another
      // receive_buffer[received_len] = '\0'; // Null terminate if needed
      // printf(ANSI_COLOR_RED "[%s] Poll Recv: DATA (len %d): %s" ANSI_COLOR_RESET "\n", member_id_str, received_len, receive_buffer);
    } else if (recv_result == 0) {
      // Notification received OR queue empty (check received_len)
      if (received_len > 0) {
        polled_notif_count++;
        // Optional: Log the notification
        // receive_buffer[received_len] = '\0'; // Null terminate
        // printf(ANSI_COLOR_RED "[%s] Poll Recv: NOTIFICATION: %s" ANSI_COLOR_RESET "\n", member_id_str, receive_buffer);
      }
      // else: Queue was empty, do nothing
    } else {
      // Error (<0) returned by non-blocking grp_recv
      // fprintf(stderr, ANSI_COLOR_RED "[%s] Poll Recv: grp_recv returned error %d" ANSI_COLOR_RESET "\n", member_id_str, recv_result);
      // No action needed, just means queue empty or actual error
    }
    // *** END ADDED RECEIVE POLLING ***

    // --- Sending Logic ---
    char message_buffer[128];
    // Use messages_sent_count for message numbering
    snprintf(message_buffer, sizeof(message_buffer), "Msg_%d_from_%.98s", messages_sent_count + 1, member_id_str);
    int msg_len = strlen(message_buffer);

    double random_val = (double)rand() / (double)RAND_MAX;

    if (random_val >= RANDOM_FACTOR) {
      int send_result = grp_send(group_handle, message_buffer, msg_len);
      if (send_result < 0) {
        fprintf(stderr, ANSI_COLOR_RED "[%s] ERROR sending message %d ('%s', len %d) (grp_send returned %d)." ANSI_COLOR_RESET "\n", member_id_str,
                messages_sent_count + 1, message_buffer, msg_len, send_result);
        // Decide if you want to stop or continue on send error
      } else {
         // Increment counter only on successful or attempted send (depending on desired metric)
         // Let's count attempts here before the sleep
         messages_sent_count++;
      }
      // Keep the inter-message delay
      usleep(INTER_MESSAGE_DELAY_US);
    } else {
      // --- Simulate Crash ---
      fprintf(stderr, ANSI_COLOR_RED "[%s PID: %d] *** SIMULATING RANDOM CRASH before sending message %d ***" ANSI_COLOR_RESET "\n", member_id_str,
              getpid(), messages_sent_count + 1);
      fflush(stderr);
      // Optional: Clear log file before aborting (add back if needed)
      abort();
    }
    // --- End Sending Logic ---

  } // End of time-controlled while loop

  printf(ANSI_COLOR_RED "[%s] Finished sending loop.\nMessages sent during %d seconds: %d" ANSI_COLOR_RESET "\n", member_id_str, sending_duration, // Multi-line printf
         messages_sent_count);
  printf(ANSI_COLOR_RED "[%s] Messages polled during send loop: %d DATA, %d NOTIF" ANSI_COLOR_RESET "\n", member_id_str, polled_data_count, polled_notif_count);


  // --- Approximate Synchronization Point 2 (Wait after Send) ---
  // This is the GRACE PERIOD for receivers
  printf(ANSI_COLOR_RED "[%s] Finished sending. Waiting %d seconds (grace period for receivers)." ANSI_COLOR_RESET "\n", member_id_str, POST_SEND_DELAY_S);
  sleep(POST_SEND_DELAY_S);

  // NOTE: The sender doesn't need a receiving loop *after* sending stops
  // for the throughput test itself. Its job (sending) is done.
  // Receivers would need the final drain loop here.

cleanup: // Label for jumping here if join fails

  // --- Close Log File (if logging was enabled) ---
  /*
  if (log_fp != NULL) {
    printf(ANSI_COLOR_RED "[%s] Closing log file '%s'." ANSI_COLOR_RESET "\n", member_id_str, log_filename);
    fclose(log_fp);
  }
  */

  // --- Shutdown Library ---
  if (lib_initialized) {
    printf(ANSI_COLOR_RED "[%s] Shutting down library..." ANSI_COLOR_RESET "\n", member_id_str);
    grp_shutdown(); // Call shutdown once at the end
    printf(ANSI_COLOR_RED "[%s] Library shut down." ANSI_COLOR_RESET "\n", member_id_str);
  }
  // Assuming print_instrumentation_counters prints its own output if needed
  print_instrumentation_counters();
  printf(ANSI_COLOR_RED "[%s PID: %d] Process finished." ANSI_COLOR_RESET "\n", member_id_str, getpid());
}