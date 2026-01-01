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
#define NUM_CLIENTS 3                                // Default number of clients to simulate
#define GROUP_NAME "TestGroupCProcNB"                // Default group name
#define MESSAGES_PER_CLIENT 5                        // Default number of messages each client sends
#define GMS_ADDRESS "127.0.0.1"                     // Default GMS Address
#define GMS_PORT 8080                                // Default GMS Port
#define LOG_FILENAME_MAXLEN (MAX_ID_LEN + 20)        // Space for "log_" + id + ".txt" + null

// Delays
#define POST_JOIN_DELAY_S 2           // Time allowed for all clients to join (approx)
#define POST_SEND_DELAY_S 20        // Time allowed for all clients to send (approx)
#define INTER_MESSAGE_DELAY_US 0 // 100ms between sends per client
#define RECV_LOOP_SLEEP_US 0   // 50ms sleep in non-blocking receive loop
#define RANDOM_FACTOR 0

// --- Function Prototypes ---
void client_logic(int client_id, char *group_name_to_use, int messages_to_send);

// --- Main Function (Parent Process) ---
int main(int argc, char *argv[]) {
  // --- Basic Argument Parsing (Optional: Override defaults) ---
  int total_clients = NUM_CLIENTS;
  if (argc > 1) {
    total_clients = atoi(argv[1]);
    if (total_clients <= 0) {
      fprintf(stderr, ANSI_COLOR_RED "Warning: Invalid number of clients '%s', using default %d." ANSI_COLOR_RESET "\n", argv[1], NUM_CLIENTS);
      total_clients = NUM_CLIENTS;
    }
  }
  char *group_name_to_use = (argc > 2) ? argv[2] : GROUP_NAME;
  int messages_to_send_per_client = (argc > 3) ? atoi(argv[3]) : MESSAGES_PER_CLIENT;
  if (messages_to_send_per_client < 0)
    messages_to_send_per_client = MESSAGES_PER_CLIENT;

  printf(ANSI_COLOR_RED "Starting C Simulator (Multi-Process, Non-Blocking Recv): %d clients, Group: '%s', Msgs/client: %d" ANSI_COLOR_RESET "\n",
         total_clients, group_name_to_use, messages_to_send_per_client);

  pid_t pids[total_clients]; // Array to store child PIDs
  int children_created = 0;

  // --- Fork Child Processes ---
  printf(ANSI_COLOR_RED "Forking %d client processes..." ANSI_COLOR_RESET "\n", total_clients);
  for (int i = 0; i < total_clients; ++i) {
    pid_t pid = fork();

    if (pid < 0) {
      // Fork failed
      perror(ANSI_COLOR_RED "ERROR: fork failed" ANSI_COLOR_RESET);
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
      client_logic(i, group_name_to_use, messages_to_send_per_client);
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
        perror(ANSI_COLOR_RED "  Parent: wait() error" ANSI_COLOR_RESET);
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
void client_logic(int client_id, char *group_name_to_use, int messages_to_send) {
  char member_id_str[MAX_ID_LEN];
  snprintf(member_id_str, MAX_ID_LEN, "Client%d", client_id);

  int group_handle = -1;
  int join_success = 0;
  int lib_initialized = 0;
  FILE *log_fp = NULL;
  char log_filename[LOG_FILENAME_MAXLEN];
  unsigned long long delivery_counter = 0;

  printf(ANSI_COLOR_RED "[%s PID: %d] Process started." ANSI_COLOR_RESET "\n", member_id_str, getpid());

  // --- 0. Initialize Library ---
  printf(ANSI_COLOR_RED "[%s] Initializing library..." ANSI_COLOR_RESET "\n", member_id_str);
  if (grp_init(GMS_ADDRESS, GMS_PORT) != 0) {
    fprintf(stderr, ANSI_COLOR_RED "[%s] ERROR: Failed to initialize library." ANSI_COLOR_RESET "\n", member_id_str);
    exit(EXIT_FAILURE); // Exit child on init failure
  }
  lib_initialized = 1;
  printf(ANSI_COLOR_RED "[%s] Library initialized." ANSI_COLOR_RESET "\n", member_id_str);

  // --- 1. Join Group ---
  printf(ANSI_COLOR_RED "[%s] Attempting to join group '%s'..." ANSI_COLOR_RESET "\n", member_id_str, group_name_to_use);
  group_handle = grp_join(group_name_to_use, member_id_str);

  if (group_handle < 0) {
    fprintf(stderr, ANSI_COLOR_RED "[%s] ERROR: Failed to join group '%s' (grp_join returned %d)." ANSI_COLOR_RESET "\n", member_id_str, group_name_to_use,
            group_handle);
    join_success = 0;
  } else {
    printf(ANSI_COLOR_RED "[%s] Successfully joined group '%s' with handle %d." ANSI_COLOR_RESET "\n", member_id_str, group_name_to_use, group_handle);
    join_success = 1;

    // --- Open Log File ---
    snprintf(log_filename, LOG_FILENAME_MAXLEN, "log_%s.txt", member_id_str);
    log_fp = fopen(log_filename, "w");
    if (log_fp == NULL) {
      fprintf(stderr, ANSI_COLOR_RED "[%s] ERROR: Could not open log file '%s': " ANSI_COLOR_RESET, member_id_str, log_filename);
      perror(ANSI_COLOR_RED "System error" ANSI_COLOR_RESET); // Color the prefix for perror
    } else {
      printf(ANSI_COLOR_RED "[%s] Logging received DATA to '%s'." ANSI_COLOR_RESET "\n", member_id_str, log_filename);
    }
  }

  // --- Approximate Synchronization Point 1 (Wait after Join) ---
  if (join_success) {
    printf(ANSI_COLOR_RED "[%s] Joined. Waiting %d seconds before sending..." ANSI_COLOR_RESET "\n", member_id_str, POST_JOIN_DELAY_S);
    sleep(POST_JOIN_DELAY_S);
  } else {
    goto cleanup; // Skip to cleanup if join failed
  }

  // // --- 2. Send Messages ---
  // printf(ANSI_COLOR_RED "[%s] Starting to send %d messages..." ANSI_COLOR_RESET "\n", member_id_str, messages_to_send);
  // srand(time(NULL) ^ getpid() ^ clock());
  // for (int i = 0; i < messages_to_send; ++i) {
  //   char message_buffer[128];
  //   snprintf(message_buffer, sizeof(message_buffer), "Msg_%d_from_%.100s", i + 1, member_id_str);
  //   int msg_len = strlen(message_buffer);

  //   double random_val = (double)rand() / (double)RAND_MAX;

  //   if (random_val >= RANDOM_FACTOR) {
  //     int send_result = grp_send(group_handle, message_buffer, msg_len);
  //     if (send_result < 0) {
  //       fprintf(stderr, ANSI_COLOR_RED "[%s] ERROR sending message %d ('%s', len %d) (grp_send returned %d)." ANSI_COLOR_RESET "\n", member_id_str, i + 1,
  //               message_buffer, msg_len, send_result);
  //     }
  //   } else {
  //     // --- Simulate Crash ---
  //     fprintf(stderr, ANSI_COLOR_RED "[%s PID: %d] *** SIMULATING RANDOM CRASH before sending message %d ***" ANSI_COLOR_RESET "\n", member_id_str,
  //             getpid(), i + 1);
  //     fflush(stderr); // Try to ensure the message gets printed before termination
  //   //   // --- Clear Log File ---
  //   //   if (log_fp != NULL) {
  //   //       printf(ANSI_COLOR_RED "[%s PID: %d] Clearing log file '%s' before aborting..." ANSI_COLOR_RESET "\n",
  //   //              member_id_str, getpid(), log_filename);
  //   //       fflush(stdout);
  //   //       fclose(log_fp); // Close the current handle

  //   //       // Re-open in "w" mode to truncate the file
  //   //       FILE *clear_fp = fopen(log_filename, "w");
  //   //       if (clear_fp != NULL) {
  //   //           fclose(clear_fp); // Close immediately after truncating
  //   //           printf(ANSI_COLOR_RED "[%s PID: %d] Log file cleared." ANSI_COLOR_RESET "\n", member_id_str, getpid());
  //   //           fflush(stdout);
  //   //       } else {
  //   //           // Log an error if re-opening failed, but proceed to abort anyway
  //   //            fprintf(stderr, ANSI_COLOR_RED "[%s PID: %d] ERROR: Could not re-open log file '%s' to clear: " ANSI_COLOR_RESET,
  //   //                    member_id_str, getpid(), log_filename);
  //   //            perror(ANSI_COLOR_RED "System error" ANSI_COLOR_RESET); // Print system error message for fopen failure
  //   //            fflush(stderr);
  //   //       }
  //   //       log_fp = NULL; // Set original pointer to NULL as it's no longer valid
  //   //   } else {
  //   //        printf(ANSI_COLOR_RED "[%s PID: %d] Log file pointer was NULL, cannot clear file '%s'." ANSI_COLOR_RESET "\n",
  //   //               member_id_str, getpid(), log_filename);
  //   //        fflush(stdout);
  //   //   }
  //     abort();        // Cause abnormal process termination
  //                     // The code below abort() will not be reached
  //   }
  // }
  // printf(ANSI_COLOR_RED "[%s] Finished sending messages." ANSI_COLOR_RESET "\n", member_id_str);

  // // --- Approximate Synchronization Point 2 (Wait after Send) ---
  // printf(ANSI_COLOR_RED "[%s] Finished sending. Waiting %d seconds before receiving..." ANSI_COLOR_RESET "\n", member_id_str, POST_SEND_DELAY_S);
  // sleep(POST_SEND_DELAY_S);

  // --- 3. Receive Loop (Now using blocking receive as per code) ---
  printf(ANSI_COLOR_RED "[%s] Entering receive loop (BLOCKING, exits on error/empty after delay).." ANSI_COLOR_RESET "\n", member_id_str); // Updated message
  unsigned char receive_buffer[UDP_BUFFER_SIZE];
  int received_len = 0;
  int data_count = 0;
  int notif_count = 0;
  // No need for consecutive_empty_recv with blocking receive
  // int consecutive_empty_recv = 0;
  // int MAX_CONSECUTIVE_EMPTY = 200000000;

  // Wait for a specific duration after sending to receive messages
  time_t recv_start_time = time(NULL);

  while (difftime(time(NULL), recv_start_time) < POST_SEND_DELAY_S) {
    // Call grp_recv with BLOCKING flag (1)
    // Note: Blocking receive doesn't typically need a loop condition like
    // consecutive misses or a timeout within the loop itself, unless the
    // library supports timeouts on blocking calls. The outer time limit handles exit.
    // print_instrumentation_counters(); // Can be noisy in a tight loop
    int recv_result = grp_recv(group_handle, receive_buffer, &received_len, 1); // BLOCKING = 1
    print_instrumentation_counters();
    if (recv_result > 0) {
      // Data received!
      data_count++;
      // consecutive_empty_recv = 0; // Reset counter on success (Not relevant for blocking)

      // *** LOGGING DATA ***
      if (log_fp != NULL) {
        delivery_counter++;
        if (received_len < UDP_BUFFER_SIZE) {
          receive_buffer[received_len] = '\0';
        } else {
          receive_buffer[UDP_BUFFER_SIZE - 1] = '\0'; // Force termination if buffer is full
          fprintf(stderr, ANSI_COLOR_RED "[%s] Warning: Received message possibly truncated (length %d >= buffer %d)" ANSI_COLOR_RESET "\n", member_id_str,
                  received_len, UDP_BUFFER_SIZE);
        }
        fprintf(log_fp, "%llu:%s\n", delivery_counter, (char *)receive_buffer);
        fflush(log_fp); // Write to file immediately
      }

    } else if (recv_result == 0) {
      // Notification received - DO NOT LOG TO DATA FILE
      notif_count++;
      // consecutive_empty_recv = 0; // Reset counter on success (Not relevant for blocking)
      printf(ANSI_COLOR_RED "[%s] Received Notification (len %d)" ANSI_COLOR_RESET "\n", member_id_str, received_len);
    } else {
      // Error or No Data Available (recv_result < 0)
      // In blocking mode, <0 usually indicates a more serious error or perhaps
      // that the connection/group state is invalid.
      fprintf(stderr, ANSI_COLOR_RED "[%s] Blocking grp_recv returned error %d. Exiting receive loop." ANSI_COLOR_RESET "\n", member_id_str, recv_result);
      break; // Exit the loop on error in blocking mode

      // Non-blocking logic removed as receive is blocking
      /*
      consecutive_empty_recv++;
      if (consecutive_empty_recv % 5 == 0 || consecutive_empty_recv == 1) {
        printf(ANSI_COLOR_RED "[%s] Non-blocking grp_recv returned %d (Empty/Error attempt %d)." ANSI_COLOR_RESET "\n", member_id_str, recv_result,
               consecutive_empty_recv);
        // Removed break; based on original non-blocking logic
      }
      if (consecutive_empty_recv >= MAX_CONSECUTIVE_EMPTY) {
        printf(ANSI_COLOR_RED "[%s] Exiting receive loop after %d consecutive empty non-blocking receives." ANSI_COLOR_RESET "\n", member_id_str,
               MAX_CONSECUTIVE_EMPTY);
        break;
      }
      */
    }

    // No need for sleep in blocking receive loop
    // usleep(RECV_LOOP_SLEEP_US);

  } // End of receive while loop (now time-based)

  printf(ANSI_COLOR_RED "[%s] Finished receive loop (Duration: %d s, Logged Data: %d, Notif: %d)." ANSI_COLOR_RESET "\n", member_id_str, POST_SEND_DELAY_S, data_count, notif_count);

  // --- 4. Leave Group (Moved after receive loop) ---
  printf(ANSI_COLOR_RED "[%s] Leaving group '%s' (handle %d)..." ANSI_COLOR_RESET "\n", member_id_str, group_name_to_use, group_handle);
  int leave_result = grp_leave(group_handle);
  if (leave_result != 0) {
    fprintf(stderr, ANSI_COLOR_RED "[%s] Warning: grp_leave returned %d." ANSI_COLOR_RESET "\n", member_id_str, leave_result);
  } else {
    printf(ANSI_COLOR_RED "[%s] Left group successfully." ANSI_COLOR_RESET "\n", member_id_str);
  }

cleanup: // Label for jumping here if join fails

  // --- Close Log File ---
  if (log_fp != NULL) {
    printf(ANSI_COLOR_RED "[%s] Closing log file '%s'." ANSI_COLOR_RESET "\n", member_id_str, log_filename);
    fclose(log_fp);
  }

  // --- 5. Shutdown Library ---
  if (lib_initialized) {
    printf(ANSI_COLOR_RED "[%s] Shutting down library..." ANSI_COLOR_RESET "\n", member_id_str);
    grp_shutdown();
    printf(ANSI_COLOR_RED "[%s] Library shut down." ANSI_COLOR_RESET "\n", member_id_str);
  }
  // Assuming print_instrumentation_counters prints its own output if needed
  usleep(500000);
  print_instrumentation_counters();

  printf(ANSI_COLOR_RED "[%s PID: %d] Process finished." ANSI_COLOR_RESET "\n", member_id_str, getpid());
  // Child process exits implicitly here or via exit() earlier.
}