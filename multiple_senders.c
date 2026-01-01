#include <errno.h>  // For perror
#include <signal.h> // For kill()
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/wait.h> // For waitpid(), wait()
#include <time.h>     // For seeding rand, timespec
#include <unistd.h>   // For fork(), sleep(), usleep(), getpid()

// Include your group communication library header
#include "group_comm.h" // Make sure this path is correct

// --- ANSI Color Codes ---
#define ANSI_COLOR_RED     "\x1b[31m" // Start Red Text
#define ANSI_COLOR_RESET   "\x1b[0m"  // Reset to Default Color

// --- Configuration Constants ---
#define CLIENT_EXECUTABLE_NAME "c_simulator_concurrent"
#define DEFAULT_NUM_CLIENTS 3
#define DEFAULT_GROUP_NAME "TestGroupConcurrent"
#define DEFAULT_DURATION_S 10
#define GMS_ADDRESS "127.0.0.1"
#define GMS_PORT 8080
#define ID_PREFIX "Client"
#define LOG_FILENAME_MAXLEN (MAX_ID_LEN + 20)

// Delays
#define POST_JOIN_DELAY_S 3
#define GRACE_PERIOD_S 1
#define INTER_MESSAGE_DELAY_US 0 // Send as fast as possible for max throughput test
#define RANDOM_FACTOR 0

// --- Function Prototypes ---
void client_logic(int client_index, int total_clients, char *group_name, int duration_s);

// --- Main Function (Parent Process) ---
int main(int argc, char *argv[]) {
  int total_clients = DEFAULT_NUM_CLIENTS;
  char *group_name_to_use = DEFAULT_GROUP_NAME;
  int duration_s = DEFAULT_DURATION_S;

  // --- Argument Parsing ---
  if (argc < 4) {
     fprintf(stderr, ANSI_COLOR_RED "Usage: %s <num_clients> <group_name> <duration_seconds>" ANSI_COLOR_RESET "\n", argv[0]);
     fprintf(stderr, ANSI_COLOR_RED "Using defaults: %d clients, group '%s', duration %ds" ANSI_COLOR_RESET "\n",
             DEFAULT_NUM_CLIENTS, DEFAULT_GROUP_NAME, DEFAULT_DURATION_S);
     // Keep defaults assigned above
  } else {
     total_clients = atoi(argv[1]);
     group_name_to_use = argv[2];
     duration_s = atoi(argv[3]);

     if (total_clients <= 0) {
       fprintf(stderr, ANSI_COLOR_RED "Warning: Invalid number of clients '%s', using default %d." ANSI_COLOR_RESET "\n", argv[1], DEFAULT_NUM_CLIENTS);
       total_clients = DEFAULT_NUM_CLIENTS;
     }
     if (strlen(group_name_to_use) == 0 || strlen(group_name_to_use) >= MAX_GROUP_LEN) {
         fprintf(stderr, ANSI_COLOR_RED "Warning: Invalid group name, using default '%s'." ANSI_COLOR_RESET "\n", DEFAULT_GROUP_NAME);
         group_name_to_use = DEFAULT_GROUP_NAME;
     }
     if (duration_s <= 0) {
       fprintf(stderr, ANSI_COLOR_RED "Warning: Invalid duration '%s', using default %ds." ANSI_COLOR_RESET "\n", argv[3], DEFAULT_DURATION_S);
       duration_s = DEFAULT_DURATION_S;
     }
  }

  printf(ANSI_COLOR_RED "Starting Concurrent Simulator: %d clients, Group: '%s', Duration: %ds" ANSI_COLOR_RESET "\n",
         total_clients, group_name_to_use, duration_s);

  pid_t pids[total_clients];
  int children_created = 0;

  // --- Fork Child Processes ---
  printf(ANSI_COLOR_RED "Forking %d client processes..." ANSI_COLOR_RESET "\n", total_clients);
  for (int i = 0; i < total_clients; ++i) {
    pid_t pid = fork();

    if (pid < 0) {
      perror(ANSI_COLOR_RED "ERROR: fork failed" ANSI_COLOR_RESET);
      fprintf(stderr, ANSI_COLOR_RED "Parent: Terminating already created children due to fork failure." ANSI_COLOR_RESET "\n");
      for (int j = 0; j < children_created; ++j) {
        if (pids[j] > 0) kill(pids[j], SIGTERM);
      }
      sleep(1);
      for (int j = 0; j < children_created; ++j) {
        if (pids[j] > 0) waitpid(pids[j], NULL, WNOHANG);
      }
      exit(EXIT_FAILURE);
    } else if (pid == 0) {
      // --- Child Process ---
      srand(time(NULL) ^ getpid());
      client_logic(i, total_clients, group_name_to_use, duration_s);
      exit(EXIT_SUCCESS);
    } else {
      // --- Parent Process ---
      pids[i] = pid;
      children_created++;
      printf(ANSI_COLOR_RED "  Parent: Forked child %d (Index %d) with PID %d" ANSI_COLOR_RESET "\n", children_created, i, pid);
      usleep(50000);
    }
  }

  // --- Parent Waits for Children ---
  printf(ANSI_COLOR_RED "Parent: All children forked. Waiting for them to complete..." ANSI_COLOR_RESET "\n");
  int status;
  int completed_children = 0;
  pid_t terminated_pid;

  while (completed_children < total_clients) {
    terminated_pid = wait(&status);

    if (terminated_pid > 0) {
      completed_children++;
      int child_index = -1;
      for (int i = 0; i < total_clients; ++i) {
        if (pids[i] == terminated_pid) {
          child_index = i;
          break;
        }
      }

      if (WIFEXITED(status)) {
          if (WEXITSTATUS(status) != 0) {
              fprintf(stderr, ANSI_COLOR_RED "  Parent WARNING: Child (Index %d, PID %d) exited with non-zero status %d!" ANSI_COLOR_RESET "\n", child_index, terminated_pid, WEXITSTATUS(status));
          } else {
               printf(ANSI_COLOR_RED "  Parent: Child (Index %d, PID %d) exited normally." ANSI_COLOR_RESET "\n", child_index, terminated_pid);
          }
      } else if (WIFSIGNALED(status)) {
        fprintf(stderr, ANSI_COLOR_RED "  Parent WARNING: Child (Index %d, PID %d) killed by signal %d%s!" ANSI_COLOR_RESET "\n",
                child_index, terminated_pid, WTERMSIG(status),
                WCOREDUMP(status) ? " (core dumped)" : ""); // Added core dump info
      } else {
        printf(ANSI_COLOR_RED "  Parent: Child (Index %d, PID %d) terminated with unknown status 0x%X." ANSI_COLOR_RESET "\n", child_index, terminated_pid, status);
      }
       printf(ANSI_COLOR_RED "  Parent: Completed count now %d/%d." ANSI_COLOR_RESET "\n", completed_children, total_clients);
       fflush(stdout);
       fflush(stderr);

    } else { // wait() error
      if (errno == ECHILD) {
        printf(ANSI_COLOR_RED "  Parent: wait() reported ECHILD. No more children expected." ANSI_COLOR_RESET "\n");
        fflush(stdout);
        break;
      } else if (errno == EINTR) {
        printf(ANSI_COLOR_RED "  Parent: wait() interrupted, retrying." ANSI_COLOR_RESET "\n");
        fflush(stdout);
        continue;
      } else {
        perror(ANSI_COLOR_RED "  Parent: wait() error" ANSI_COLOR_RESET);
        break;
      }
    }
  }

  if (completed_children != total_clients) {
    fprintf(stderr, ANSI_COLOR_RED "Parent Warning: Expected %d children, but only waited for %d." ANSI_COLOR_RESET "\n", total_clients, completed_children);
  } else {
    printf(ANSI_COLOR_RED "Parent: All %d child processes have completed." ANSI_COLOR_RESET "\n", completed_children);
  }

  printf(ANSI_COLOR_RED "Concurrent Simulator finished." ANSI_COLOR_RESET "\n");
  printf(ANSI_COLOR_RED "Check log files (e.g., 'log_Client0.txt') for received messages and counts." ANSI_COLOR_RESET "\n");
  return 0;
}


// --- Client Process Logic (Modified for Concurrent Send/Recv with Timing Measurement) ---
void client_logic(int client_index, int total_clients, char *group_name, int duration_s) {
  char member_id_str[MAX_ID_LEN];
  snprintf(member_id_str, MAX_ID_LEN, "%s%d", ID_PREFIX, client_index);

  int group_handle = -1;
  int join_success = 0;
  int lib_initialized = 0;

  // Variables for receiving/polling
  unsigned char receive_buffer[UDP_BUFFER_SIZE + 1];
  int received_len = 0;
  long long messages_delivered_count = 0;
  long long notif_received_count = 0;

  // Log file variables
  FILE *log_fp = NULL;
  char log_filename[LOG_FILENAME_MAXLEN];

  // *** ADDED: Variables for timing the send interval ***
  struct timespec time_before_send;
  long long send_intervals_measured = 0;
  int first_send = 1; // Flag to capture time before the very first send
  // *** END ADDED ***

  printf(ANSI_COLOR_RED "[%s PID: %d] Process started (Index %d)." ANSI_COLOR_RESET "\n", member_id_str, getpid(), client_index);

  // --- 0. Initialize Library ---
  printf(ANSI_COLOR_RED "[%s] Initializing library..." ANSI_COLOR_RESET "\n", member_id_str);
  if (grp_init(GMS_ADDRESS, GMS_PORT) != 0) {
    fprintf(stderr, ANSI_COLOR_RED "[%s] ERROR: Failed to initialize library." ANSI_COLOR_RESET "\n", member_id_str);
    exit(EXIT_FAILURE);
  }
  lib_initialized = 1;
  printf(ANSI_COLOR_RED "[%s] Library initialized." ANSI_COLOR_RESET "\n", member_id_str);

  // --- 1. Join Group ---
  printf(ANSI_COLOR_RED "[%s] Attempting to join group '%s'..." ANSI_COLOR_RESET "\n", member_id_str, group_name);
  group_handle = grp_join(group_name, member_id_str);

  if (group_handle < 0) {
    fprintf(stderr, ANSI_COLOR_RED "[%s] ERROR: Failed to join group '%s' (grp_join returned %d)." ANSI_COLOR_RESET "\n", member_id_str, group_name, group_handle);
    join_success = 0;
  } else {
    printf(ANSI_COLOR_RED "[%s] Successfully joined group '%s' with handle %d." ANSI_COLOR_RESET "\n", member_id_str, group_name, group_handle);
    join_success = 1;

    // Open Log File
    snprintf(log_filename, LOG_FILENAME_MAXLEN, "log_%s.txt", member_id_str);
    log_fp = fopen(log_filename, "w");
    if (log_fp == NULL) {
      fprintf(stderr, ANSI_COLOR_RED "[%s] ERROR: Could not open log file '%s': " ANSI_COLOR_RESET, member_id_str, log_filename);
      perror(ANSI_COLOR_RED "System error" ANSI_COLOR_RESET);
    } else {
      printf(ANSI_COLOR_RED "[%s] Logging received DATA to '%s'." ANSI_COLOR_RESET "\n", member_id_str, log_filename);
      fprintf(log_fp, "--- Log Start: Client %s PID %d ---\n", member_id_str, getpid());
      fflush(log_fp);
    }
  }

  // --- Approximate Synchronization Point 1 (Wait after Join) ---
  if (join_success) {
    printf(ANSI_COLOR_RED "[%s] Joined. Waiting %d seconds before activity..." ANSI_COLOR_RESET "\n", member_id_str, POST_JOIN_DELAY_S);
    sleep(POST_JOIN_DELAY_S);
  } else {
    goto cleanup;
  }

  // --- 2. Combined Send & Receive Polling Loop ---
  printf(ANSI_COLOR_RED "[%s] Starting concurrent send/poll loop for %d seconds..." ANSI_COLOR_RESET "\n", member_id_str, duration_s);
  long long messages_sent_count = 0;

  time_t start_wall_clock, now_wall_clock; // For overall duration check
  int seconds_elapsed;

  time(&start_wall_clock); // Start overall timer
  clock_gettime(CLOCK_MONOTONIC, &time_before_send); // Initialize time_before_send for the first interval

  while (1) {
    time(&now_wall_clock);
    seconds_elapsed = difftime(now_wall_clock, start_wall_clock);
    if (seconds_elapsed >= duration_s) {
      printf(ANSI_COLOR_RED "[%s] Send/poll duration ended after %d seconds." ANSI_COLOR_RESET "\n", member_id_str, duration_s);
      break;
    }

    // *** Receive Polling Attempt (Non-Blocking) ***
    int recv_result = grp_recv(group_handle, receive_buffer, &received_len, 0);
    
    if (recv_result > 0) {
      messages_delivered_count++;
      if (log_fp != NULL) {
          // Ensure null termination, considering potential buffer overflow
          if (received_len < UDP_BUFFER_SIZE) { receive_buffer[received_len] = '\0'; }
          else { receive_buffer[UDP_BUFFER_SIZE] = '\0'; } // Null terminate at buffer end
          fprintf(log_fp, "%lld:%s\n", messages_delivered_count, (char *)receive_buffer);
          fflush(log_fp);
      }
    } else if (recv_result == 0 && received_len > 0) {
        notif_received_count++;
        // Ensure null termination for printing
        if (received_len < UDP_BUFFER_SIZE + 1) { receive_buffer[received_len] = '\0'; }
        else { receive_buffer[UDP_BUFFER_SIZE] = '\0'; }
        printf(ANSI_COLOR_RED "[%s] Polled Recv: NOTIFICATION: %s" ANSI_COLOR_RESET "\n", member_id_str, receive_buffer);
    }

    // --- Sending Logic ---
    char message_buffer[256];
    // Corrected line 269:
    snprintf(message_buffer, sizeof(message_buffer), "Msg_%lld_from_%.98s", messages_sent_count + 1, member_id_str);
    int msg_len = strlen(message_buffer);

    double random_val = (double)rand() / (double)RAND_MAX;

    if (random_val >= RANDOM_FACTOR) {

      // *** ADDED: Record time difference from last send/sleep ***
      // We already have time_before_send from the *previous* iteration's end
      // or the initialization before the loop.
      // Now, record the time just *before* the current send.
      struct timespec current_send_time;
      clock_gettime(CLOCK_MONOTONIC, &current_send_time);

      // Calculate interval since the end of the *last* sleep/send cycle
      if (!first_send) { // Don't count the very first "interval" before any send happened
          send_intervals_measured++;
      }
      first_send = 0; // No longer the first send

      // *** END ADDED ***

      // Send the message
      int send_result = grp_send(group_handle, message_buffer, msg_len);
      if (send_result < 0) {
        fprintf(stderr, ANSI_COLOR_RED "[%s] ERROR sending message %lld (grp_send returned %d)." ANSI_COLOR_RESET "\n", member_id_str, messages_sent_count + 1, send_result);
      } else {
         messages_sent_count++;
      }

      // Apply delay
      usleep(INTER_MESSAGE_DELAY_US);

      // *** ADDED: Record time *after* the sleep for the *next* interval calculation ***
      clock_gettime(CLOCK_MONOTONIC, &time_before_send); // This becomes the start for the next cycle
      // *** END ADDED ***

    } else {
      fprintf(stderr, ANSI_COLOR_RED "[%s PID: %d] *** SIMULATING RANDOM CRASH before sending message %lld ***" ANSI_COLOR_RESET "\n", member_id_str, getpid(), messages_sent_count + 1);
      fflush(stderr);
      if (log_fp != NULL) fclose(log_fp);
      abort();
    }

  } // End of time-controlled while loop

  printf(ANSI_COLOR_RED "[%s] Finished send/poll loop." ANSI_COLOR_RESET "\n", member_id_str);


  // --- Approximate Synchronization Point 2 (Grace Period) ---
  printf(ANSI_COLOR_RED "[%s] Activity loop finished. Waiting %d seconds (grace period)." ANSI_COLOR_RESET "\n", member_id_str, GRACE_PERIOD_S);
  sleep(GRACE_PERIOD_S);

  // --- 3. Final Drain Loop ---
  printf(ANSI_COLOR_RED "[%s] Entering final drain loop..." ANSI_COLOR_RESET "\n", member_id_str);
  int drain_data = 0;
  int drain_notif = 0;
  while (1) {
      int drain_recv_result = grp_recv(group_handle, receive_buffer, &received_len, 0);
      if (drain_recv_result > 0) {
          messages_delivered_count++;
          drain_data++;
          if (log_fp != NULL) {
              // Ensure null termination
              if (received_len < UDP_BUFFER_SIZE) { receive_buffer[received_len] = '\0'; }
              else { receive_buffer[UDP_BUFFER_SIZE] = '\0'; }
              fprintf(log_fp, "%lld:%s (DRAIN)\n", messages_delivered_count, (char *)receive_buffer);
              fflush(log_fp);
          }
      } else if (drain_recv_result == 0 && received_len > 0) {
          drain_notif++;
          // Ensure null termination for printing
          if (received_len < UDP_BUFFER_SIZE + 1) { receive_buffer[received_len] = '\0'; }
          else { receive_buffer[UDP_BUFFER_SIZE] = '\0'; }
          printf(ANSI_COLOR_RED "[%s] Drain Recv: NOTIFICATION: %s" ANSI_COLOR_RESET "\n", member_id_str, receive_buffer);
      } else {
          break;
      }
  }
  printf(ANSI_COLOR_RED "[%s] Finished final drain (got %d data, %d notif)." ANSI_COLOR_RESET "\n", member_id_str, drain_data, drain_notif);

  // --- Final Counts ---
  printf(ANSI_COLOR_RED "[%s] FINAL STATS: Messages Sent: %lld, Messages Delivered (Logged): %lld" ANSI_COLOR_RESET "\n",
         member_id_str, messages_sent_count, messages_delivered_count);
  fflush(stdout);

cleanup:

  // Close Log File
  if (log_fp != NULL) {
    fprintf(log_fp, "--- Log End: Client %s PID %d Sent: %lld Delivered: %lld ---\n",
            member_id_str, getpid(), messages_sent_count, messages_delivered_count);
    printf(ANSI_COLOR_RED "[%s] Closing log file '%s'." ANSI_COLOR_RESET "\n", member_id_str, log_filename);
    fclose(log_fp);
  }

  // --- Shutdown Library ---
  if (lib_initialized) {
    printf(ANSI_COLOR_RED "[%s] Shutting down library..." ANSI_COLOR_RESET "\n", member_id_str);
    grp_shutdown();
    printf(ANSI_COLOR_RED "[%s] Library shut down." ANSI_COLOR_RESET "\n", member_id_str);
  }

  printf(ANSI_COLOR_RED "[%s PID: %d] Process finished." ANSI_COLOR_RESET "\n", member_id_str, getpid());

  // Assuming print_instrumentation_counters prints its own output if needed
  usleep(500000);
  print_instrumentation_counters();

}