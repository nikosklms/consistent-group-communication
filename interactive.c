#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <errno.h> // Για τη χρήση της errno (π.χ., για EAGAIN/EWOULDBLOCK)

// Συμπεριλάβετε το header file της βιβλιοθήκης σας για την επικοινωνία ομάδας
#include "group_comm.h" // Βεβαιωθείτε ότι αυτό το αρχείο υπάρχει και ορίζει τις συναρτήσεις/σταθερές

// --- ANSI Color Codes ---
#define ANSI_COLOR_RED     "\x1b[31m" // Start Red Text
#define ANSI_COLOR_RESET   "\x1b[0m"  // Reset to Default Color

// --- Προεπιλεγμένες Ρυθμίσεις ---
#define GMS_ADDRESS "127.0.0.1" // Προεπιλεγμένη Διεύθυνση GMS
#define GMS_PORT 8080           // Προεπιλεγμένη Θύρα GMS
#define MAX_MSG_LEN 1024        // Μέγιστο μήκος μηνύματος (αν και το UDP_BUFFER_SIZE είναι μεγαλύτερο)
#define MAX_CONCURRENT_GROUPS 10 // Μέγιστος αριθμός ταυτόχρονων groups ανά client
#define LOG_FILENAME_MAXLEN (MAX_ID_LEN + 20) // Χώρος για "log_" + id + ".txt" + null terminator

// --- Δομή για Πληροφορίες Συμμετοχής ---
typedef struct {
    int handle;                 // Το handle που επιστρέφεται από το grp_join
    char name[MAX_GROUP_LEN];   // Το όνομα του group
    int active;                 // 0 = ανενεργό slot, 1 = ενεργό
    unsigned long long delivery_counter; // Μετρητής για τα μηνύματα που καταγράφηκαν σε αυτό το group
} group_membership_t;

// --- Global Μεταβλητή για το Logging ---
FILE *log_fp = NULL;                // File pointer για το αρχείο log
char log_filename[LOG_FILENAME_MAXLEN]; // Όνομα αρχείου log

// --- Πρωτότυπα Βοηθητικών Συναρτήσεων ---
void display_joined_groups(group_membership_t groups[], int max_groups, int active_count);
int find_free_group_slot(group_membership_t groups[], int max_groups);
int find_group_by_input(group_membership_t groups[], int max_groups, const char* input, int *found_handle);
void cleanup_resources(group_membership_t groups[], int max_groups); // Συνάρτηση καθαρισμού πόρων


// --- Κύρια Συνάρτηση ---
int main(int argc, char *argv[]) {
    char member_id[MAX_ID_LEN];     // ID του μέλους (από τα ορίσματα)
    group_membership_t joined_groups[MAX_CONCURRENT_GROUPS]; // Πίνακας για τα groups που έχει μπει ο client
    int active_group_count = 0;     // Πόσα groups είναι ενεργά αυτή τη στιγμή

    char user_input[MAX_MSG_LEN];   // Buffer για την είσοδο του χρήστη
    char target_group_input[MAX_GROUP_LEN]; // Buffer για το όνομα/index του group-στόχου
    char message_to_send[MAX_MSG_LEN];      // Buffer για το μήνυμα προς αποστολή
    unsigned char receive_buffer[UDP_BUFFER_SIZE]; // Buffer για τη λήψη (αρκετά μεγάλο)
    int received_len;               // Μήκος των δεδομένων που λήφθηκαν
    int target_handle;              // Το handle του group-στόχου
    int choice_index;               // Το index του group-στόχου στον πίνακα joined_groups

    // --- Αρχικοποίηση Πληροφοριών Συμμετοχής ---
    for (int i = 0; i < MAX_CONCURRENT_GROUPS; ++i) {
        joined_groups[i].active = 0;
        joined_groups[i].handle = -1;
        joined_groups[i].name[0] = '\0';
        joined_groups[i].delivery_counter = 0; // Αρχικοποίηση μετρητή καταγραφής
    }

    // --- Επεξεργασία Ορισμάτων (Απαιτείται το Member ID) ---
    if (argc != 2) {
        fprintf(stderr, ANSI_COLOR_RED "Usage: %s <member_id>" ANSI_COLOR_RESET "\n", argv[0]);
        return 1;
    }
    strncpy(member_id, argv[1], MAX_ID_LEN - 1);
    member_id[MAX_ID_LEN - 1] = '\0'; // Εξασφάλιση null termination

    printf(ANSI_COLOR_RED "Starting client: MemberID='%s'" ANSI_COLOR_RESET "\n", member_id);

    // --- Αρχικοποίηση Βιβλιοθήκης ---
    printf(ANSI_COLOR_RED "Initializing group library..." ANSI_COLOR_RESET "\n");
    if (grp_init(GMS_ADDRESS, GMS_PORT) != 0) {
        fprintf(stderr, ANSI_COLOR_RED "Failed to initialize group library." ANSI_COLOR_RESET "\n");
        return 1;
    }
    printf(ANSI_COLOR_RED "Library initialized." ANSI_COLOR_RESET "\n");

    // --- Προετοιμασία Logging ---
    snprintf(log_filename, LOG_FILENAME_MAXLEN, "log_%s.txt", member_id);
    log_fp = fopen(log_filename, "w"); // Άνοιγμα σε write mode (δημιουργεί νέο αρχείο ή διαγράφει το παλιό)
    if (log_fp == NULL) {
        // Color the prefix for perror
        perror(ANSI_COLOR_RED "Error opening log file" ANSI_COLOR_RESET);
        fprintf(stderr, ANSI_COLOR_RED "Warning: Could not open log file '%s'. Logging disabled." ANSI_COLOR_RESET "\n", log_filename);
        // Το πρόγραμμα συνεχίζει χωρίς logging
    } else {
        printf(ANSI_COLOR_RED "Logging received DATA messages to: %s" ANSI_COLOR_RESET "\n", log_filename);
    }

    // --- Κύριος Βρόχος Εφαρμογής ---
    while (1) {
        printf(ANSI_COLOR_RED "\n--- Client Menu (ID: %s) ---" ANSI_COLOR_RESET "\n", member_id);
        display_joined_groups(joined_groups, MAX_CONCURRENT_GROUPS, active_group_count);
        printf(ANSI_COLOR_RED "Options: [j]oin, [s]end, [r]eceive, [l]eave, [q]uit: " ANSI_COLOR_RESET);
        fflush(stdout); // Εξασφάλιση εμφάνισης του prompt πριν την ανάγνωση εισόδου

        // Διάβασμα εντολής χρήστη
        if (fgets(user_input, sizeof(user_input), stdin) == NULL) {
            // End-of-file (π.χ., Ctrl+D) ή σφάλμα ανάγνωσης
            printf(ANSI_COLOR_RED "\nInput error or EOF detected. Quitting..." ANSI_COLOR_RESET "\n");
            errno = 0; // Καθαρισμός errno αν προκλήθηκε από EOF
            break; // Έξοδος από τον κύριο βρόχο
        }
        user_input[strcspn(user_input, "\n")] = 0; // Αφαίρεση του newline χαρακτήρα

        // --- Έξοδος (Quit) ---
        if (strcmp(user_input, "q") == 0 || strcmp(user_input, "Q") == 0) {
            printf(ANSI_COLOR_RED "Quitting..." ANSI_COLOR_RESET "\n");
            break; // Έξοδος από τον κύριο βρόχο
        }

        // --- Είσοδος σε Group (Join) ---
        else if (strcmp(user_input, "j") == 0 || strcmp(user_input, "J") == 0) {
            if (active_group_count >= MAX_CONCURRENT_GROUPS) {
                fprintf(stderr, ANSI_COLOR_RED "Cannot join more groups (limit %d reached)." ANSI_COLOR_RESET "\n", MAX_CONCURRENT_GROUPS);
                continue; // Επιστροφή στην αρχή του βρόχου
            }
            printf(ANSI_COLOR_RED "Enter group name to join: " ANSI_COLOR_RESET);
            fflush(stdout);
            if (fgets(target_group_input, sizeof(target_group_input), stdin) == NULL) continue; // Σφάλμα ή EOF
            target_group_input[strcspn(target_group_input, "\n")] = 0;

            if (strlen(target_group_input) > 0) {
                 // Έλεγχος αν ο client είναι ήδη μέλος ΑΥΤΟΥ του group
                 int already_joined = 0;
                 for(int i=0; i<MAX_CONCURRENT_GROUPS; ++i) {
                      if(joined_groups[i].active && strcmp(joined_groups[i].name, target_group_input) == 0) {
                           already_joined = 1;
                           fprintf(stderr, ANSI_COLOR_RED "Already joined group '%s' (Handle: %d)." ANSI_COLOR_RESET "\n", target_group_input, joined_groups[i].handle);
                           break;
                      }
                 }
                 if(already_joined) {
                      continue; // Αν είναι ήδη μέλος, πήγαινε στην επόμενη επανάληψη
                 }

                // Προσπάθεια εισόδου
                printf(ANSI_COLOR_RED "Attempting to join group '%s' as '%s'..." ANSI_COLOR_RESET "\n", target_group_input, member_id);
                int new_handle = grp_join(target_group_input, member_id); // Κλήση της βιβλιοθήκης

                if (new_handle < 0) { // Έλεγχος για σφάλμα
                    fprintf(stderr, ANSI_COLOR_RED "Failed to join group '%s' (Error code from grp_join: %d)." ANSI_COLOR_RESET "\n", target_group_input, new_handle);
                } else { // Επιτυχής είσοδος
                    int slot = find_free_group_slot(joined_groups, MAX_CONCURRENT_GROUPS); // Βρες μια ελεύθερη θέση στον πίνακα
                    if (slot != -1) {
                        // Αποθήκευση πληροφοριών στη θέση που βρέθηκε
                        joined_groups[slot].handle = new_handle;
                        strncpy(joined_groups[slot].name, target_group_input, MAX_GROUP_LEN - 1);
                        joined_groups[slot].name[MAX_GROUP_LEN - 1] = '\0';
                        joined_groups[slot].active = 1; // Σήμανση ως ενεργό
                        joined_groups[slot].delivery_counter = 0; // Μηδενισμός μετρητή καταγραφής για το νέο group
                        active_group_count++; // Αύξηση μετρητή ενεργών groups
                        printf(ANSI_COLOR_RED "Successfully joined group '%s' with handle %d (Assigned to slot %d)." ANSI_COLOR_RESET "\n", joined_groups[slot].name, new_handle, slot);
                    } else {
                        // Αυτό θεωρητικά δεν πρέπει να συμβεί αν ο έλεγχος active_group_count πέρασε
                        fprintf(stderr, ANSI_COLOR_RED "Internal error: No free slot found after successful join check? Attempting to leave." ANSI_COLOR_RESET "\n");
                        grp_leave(new_handle); // Προσπάθεια αποχώρησης από το group που μόλις μπήκε
                    }
                }
            } else {
                printf(ANSI_COLOR_RED "Empty group name provided, not joining." ANSI_COLOR_RESET "\n");
            }
        }

        // --- Αποχώρηση από Group (Leave) ---
        else if (strcmp(user_input, "l") == 0 || strcmp(user_input, "L") == 0) {
            if (active_group_count == 0) {
                fprintf(stderr, ANSI_COLOR_RED "Not currently in any group to leave." ANSI_COLOR_RESET "\n");
                continue;
            }
            display_joined_groups(joined_groups, MAX_CONCURRENT_GROUPS, active_group_count);
            printf(ANSI_COLOR_RED "Enter group name or index # to leave: " ANSI_COLOR_RESET);
            fflush(stdout);
            if (fgets(target_group_input, sizeof(target_group_input), stdin) == NULL) continue;
            target_group_input[strcspn(target_group_input, "\n")] = 0;

            // Βρες το group με βάση την είσοδο του χρήστη (όνομα ή index)
            choice_index = find_group_by_input(joined_groups, MAX_CONCURRENT_GROUPS, target_group_input, &target_handle);

            if (choice_index != -1) { // Αν βρέθηκε το group
                printf(ANSI_COLOR_RED "Leaving group '%s' (handle %d)..." ANSI_COLOR_RESET "\n", joined_groups[choice_index].name, target_handle);
                int leave_result = grp_leave(target_handle); // Κλήση της βιβλιοθήκης
                if (leave_result == 0) {
                    printf(ANSI_COLOR_RED "Left group successfully via grp_leave." ANSI_COLOR_RESET "\n");
                } else {
                     fprintf(stderr, ANSI_COLOR_RED "Warning: grp_leave returned error code %d for handle %d" ANSI_COLOR_RESET "\n", leave_result, target_handle);
                     // Συνεχίζουμε να το αφαιρούμε από τη λίστα μας ούτως ή άλλως
                }
                // Απενεργοποίηση της θέσης στον πίνακα μας
                joined_groups[choice_index].active = 0;
                joined_groups[choice_index].handle = -1; // Προαιρετικό: καθαρισμός handle
                joined_groups[choice_index].delivery_counter = 0; // Μηδενισμός μετρητή
                active_group_count--; // Μείωση μετρητή ενεργών groups
            } else {
                fprintf(stderr, ANSI_COLOR_RED "Group '%s' not found in the list of joined groups." ANSI_COLOR_RESET "\n", target_group_input);
            }
        }

        // --- Αποστολή Μηνύματος (Send) ---
        else if (strcmp(user_input, "s") == 0 || strcmp(user_input, "S") == 0) {
            if (active_group_count == 0) {
                fprintf(stderr, ANSI_COLOR_RED "Not currently in any group to send a message." ANSI_COLOR_RESET "\n");
                continue;
            }
            display_joined_groups(joined_groups, MAX_CONCURRENT_GROUPS, active_group_count);
            printf(ANSI_COLOR_RED "Enter target group name or index # to send to: " ANSI_COLOR_RESET);
            fflush(stdout);
            if (fgets(target_group_input, sizeof(target_group_input), stdin) == NULL) continue;
            target_group_input[strcspn(target_group_input, "\n")] = 0;

            // Βρες το group-στόχο
            choice_index = find_group_by_input(joined_groups, MAX_CONCURRENT_GROUPS, target_group_input, &target_handle);

            if (choice_index != -1) { // Αν βρέθηκε
                printf(ANSI_COLOR_RED "Enter message to send to group '%s': " ANSI_COLOR_RESET, joined_groups[choice_index].name);
                fflush(stdout);
                if (fgets(message_to_send, sizeof(message_to_send), stdin) == NULL) continue;
                message_to_send[strcspn(message_to_send, "\n")] = 0;
                int msg_len = strlen(message_to_send);

                if (msg_len > 0) {
                    // Προαιρετικός έλεγχος μήκους αν η βιβλιοθήκη έχει αυστηρό όριο μικρότερο από το buffer
                    // if (msg_len >= YOUR_LIB_MAX_MSG_SIZE) {
                    //     msg_len = YOUR_LIB_MAX_MSG_SIZE - 1;
                    //     message_to_send[msg_len] = '\0';
                    //     printf(ANSI_COLOR_RED "Warning: Message truncated to %d bytes." ANSI_COLOR_RESET "\n", msg_len);
                    // }

                    printf(ANSI_COLOR_RED "Sending message: '%s' (%d bytes) to group handle %d" ANSI_COLOR_RESET "\n", message_to_send, msg_len, target_handle);
                    int send_result = grp_send(target_handle, message_to_send, msg_len); // Κλήση βιβλιοθήκης

                    // Η περιγραφή του API λέει ότι το grp_send επιστρέφει int. Υποθέτουμε < 0 είναι σφάλμα.
                    if (send_result < 0) {
                        fprintf(stderr, ANSI_COLOR_RED "Error sending message to group '%s' (Error code from grp_send: %d)." ANSI_COLOR_RESET "\n", joined_groups[choice_index].name, send_result);
                    } else {
                        // Η επιτυχία εδώ σημαίνει ότι η βιβλιοθήκη αποδέχτηκε το μήνυμα για αποστολή.
                        printf(ANSI_COLOR_RED "Message accepted by library for sending." ANSI_COLOR_RESET "\n");
                    }
                } else {
                    printf(ANSI_COLOR_RED "Empty message, not sending." ANSI_COLOR_RESET "\n");
                }
            } else {
                fprintf(stderr, ANSI_COLOR_RED "Group '%s' not found in the list of joined groups." ANSI_COLOR_RESET "\n", target_group_input);
            }
        }

        // --- Λήψη Μηνύματος (Receive) ---
        else if (strcmp(user_input, "r") == 0 || strcmp(user_input, "R") == 0) {
             if (active_group_count == 0) {
                fprintf(stderr, ANSI_COLOR_RED "Not currently in any group to receive from." ANSI_COLOR_RESET "\n");
                continue;
            }
            display_joined_groups(joined_groups, MAX_CONCURRENT_GROUPS, active_group_count);
            printf(ANSI_COLOR_RED "Enter group name or index # to receive from: " ANSI_COLOR_RESET);
            fflush(stdout);
            if (fgets(target_group_input, sizeof(target_group_input), stdin) == NULL) continue;
            target_group_input[strcspn(target_group_input, "\n")] = 0;

            // Βρες το group από το οποίο θέλει να λάβει ο χρήστης
            choice_index = find_group_by_input(joined_groups, MAX_CONCURRENT_GROUPS, target_group_input, &target_handle);

            if (choice_index != -1) { // Αν βρέθηκε
                // Επιλογή blocking ή non-blocking receive
                char blocking_choice_input[10];
                int use_blocking = 1; // Προεπιλογή: blocking
                printf(ANSI_COLOR_RED "Receive mode for '%s': [1] Blocking / [0] Non-Blocking? (Default=1): " ANSI_COLOR_RESET, joined_groups[choice_index].name);
                fflush(stdout);
                if (fgets(blocking_choice_input, sizeof(blocking_choice_input), stdin) != NULL) {
                     blocking_choice_input[strcspn(blocking_choice_input, "\n")] = 0;
                     if (strcmp(blocking_choice_input, "0") == 0) {
                         use_blocking = 0; // Ο χρήστης επέλεξε non-blocking
                     }
                }
                printf(ANSI_COLOR_RED "Using %s receive for group '%s' (Handle %d)." ANSI_COLOR_RESET "\n",
                       use_blocking ? "BLOCKING" : "NON-BLOCKING",
                       joined_groups[choice_index].name, target_handle);

                received_len = 0; // Μηδενισμός μήκους πριν την κλήση
                int recv_result = -1; // Το API λέει: >0=data, 0=notification, <0=error/no_data

                printf(ANSI_COLOR_RED "Attempting receive (block=%d)..." ANSI_COLOR_RESET "\n", use_blocking);
                recv_result = grp_recv(target_handle, receive_buffer, &received_len, use_blocking); // Κλήση βιβλιοθήκης

                // Επεξεργασία του αποτελέσματος της grp_recv
                if (recv_result < 0) {
                    // Σφάλμα ή (στην non-blocking) δεν υπήρχαν διαθέσιμα δεδομένα
                    // Αν η βιβλιοθήκη ορίζει errno, μπορούμε να το ελέγξουμε εδώ
                    // if (!use_blocking && (errno == EAGAIN || errno == EWOULDBLOCK)) {
                    //    printf(ANSI_COLOR_RED "--> No data available (non-blocking)." ANSI_COLOR_RESET "\n");
                    // } else {
                         fprintf(stderr, ANSI_COLOR_RED "Receive error or no data available (non-blocking) for group '%s'. Result code: %d" ANSI_COLOR_RESET "\n", joined_groups[choice_index].name, recv_result);
                         // Optional: color the prefix for perror if you uncomment it
                         // perror(ANSI_COLOR_RED "System error related to receive (if any)" ANSI_COLOR_RESET);
                    // }
                } else if (recv_result == 0) {
                    // Ειδοποίηση (Notification), π.χ., αλλαγή μελών (view change)
                    // Το περιεχόμενο της ειδοποίησης είναι στο receive_buffer, μήκος στο received_len
                    printf(ANSI_COLOR_RED "--> Received NOTIFICATION " ANSI_COLOR_RESET); // Print prefix in red
                    if (received_len > 0 && received_len < UDP_BUFFER_SIZE) {
                        receive_buffer[received_len] = '\0'; // Null-terminate αν είναι κείμενο
                        printf(ANSI_COLOR_RED ": %s" ANSI_COLOR_RESET "\n", (char*)receive_buffer); // Print content in red
                    } else if (received_len == 0) {
                         printf(ANSI_COLOR_RED "(empty notification)." ANSI_COLOR_RESET "\n");
                    } else { // Μεγάλο ή δυαδικό notification
                        printf(ANSI_COLOR_RED "(binary/long, length %d)." ANSI_COLOR_RESET "\n", received_len);
                        // Πιθανή επεξεργασία δυαδικών δεδομένων ειδοποίησης εδώ
                    }
                    usleep(500000);
                    print_instrumentation_counters();
                    // Δεν καταγράφουμε Notifications στο log αρχείο δεδομένων
                } else {
                    // Λήψη Μηνύματος Δεδομένων (DATA)
                    // Το recv_result > 0. Ιδανικά, recv_result == received_len.
                    printf(ANSI_COLOR_RED "--> Received DATA (Result code: %d, Length: %d) " ANSI_COLOR_RESET, recv_result, received_len); // Print prefix in red

                    // Ασφαλής χειρισμός του buffer για εκτύπωση και logging
                    if (received_len > 0 && received_len < UDP_BUFFER_SIZE) {
                         receive_buffer[received_len] = '\0'; // Null-terminate
                    } else if (received_len >= UDP_BUFFER_SIZE) {
                         receive_buffer[UDP_BUFFER_SIZE - 1] = '\0'; // Null-terminate στο τέλος του buffer
                         fprintf(stderr, ANSI_COLOR_RED "\nWarning: Received message possibly truncated (length %d >= buffer %d)" ANSI_COLOR_RESET "\n", received_len, UDP_BUFFER_SIZE);
                    } else { // received_len <= 0 αλλά recv_result > 0; Ασυμφωνία στο API;
                         receive_buffer[0] = '\0';
                         fprintf(stderr, ANSI_COLOR_RED "\nWarning: Received DATA indication (result %d) but length is %d" ANSI_COLOR_RESET "\n", recv_result, received_len);
                    }

                    printf(ANSI_COLOR_RED ": %s" ANSI_COLOR_RESET "\n", (char*)receive_buffer); // Εκτύπωση του μηνύματος σε κόκκινο

                    // *** ΚΑΤΑΓΡΑΦΗ ΣΤΟ LOG ΑΡΧΕΙΟ (ΜΟΝΟ ΓΙΑ DATA) ***
                    if (log_fp != NULL) {
                        // Αύξηση του μετρητή παράδοσης για το συγκεκριμένο group
                        joined_groups[choice_index].delivery_counter++;
                        // Καταγραφή: ΑριθμόςΠαράδοσης:ΠεριεχόμενοΜηνύματος
                        fprintf(log_fp, "%llu:%s\n",
                                joined_groups[choice_index].delivery_counter,
                                (char*)receive_buffer); // Χρησιμοποιούμε το null-terminated buffer
                        fflush(log_fp); // Άμεση εγγραφή στο αρχείο (χρήσιμο για debugging)
                    }
                    // *** ΤΕΛΟΣ ΚΑΤΑΓΡΑΦΗΣ ***
                    usleep(500000);
                    print_instrumentation_counters();

                } // Τέλος επεξεργασίας αποτελέσματος grp_recv

                // Σημείωση: Η τρέχουσα δομή εκτελεί μία προσπάθεια λήψης ανά εντολή 'r'.
                // Για συνεχή λήψη (ειδικά σε blocking mode), θα χρειαζόταν διαφορετική δομή,
                // πιθανώς με ξεχωριστό thread που καλεί συνεχώς grp_recv.

            } else { // Το group που ζήτησε ο χρήστης δεν βρέθηκε
                 fprintf(stderr, ANSI_COLOR_RED "Group '%s' not found in the list of joined groups." ANSI_COLOR_RESET "\n", target_group_input);
            }
        } // Τέλος εντολής 'r'

        // --- Άκυρη Επιλογή ---
        else {
            printf(ANSI_COLOR_RED "Invalid option: '%s'. Valid options: j, s, r, l, q" ANSI_COLOR_RESET "\n", user_input);
        }
        errno = 0; // Καθαρισμός errno πριν την επόμενη επανάληψη του βρόχου
    } // --- Τέλος Κύριου Βρόχου (while(1)) ---

    // --- Καθαρισμός Πόρων Πριν την Έξοδο ---
    cleanup_resources(joined_groups, MAX_CONCURRENT_GROUPS);

    printf(ANSI_COLOR_RED "Client '%s' finished." ANSI_COLOR_RESET "\n", member_id);
    return 0;
}


// --- Υλοποιήσεις Βοηθητικών Συναρτήσεων ---

/**
 * @brief Εμφανίζει τα groups στα οποία έχει γίνει join ο client.
 */
void display_joined_groups(group_membership_t groups[], int max_groups, int active_count) {
    printf(ANSI_COLOR_RED "Joined Groups (%d/%d):" ANSI_COLOR_RESET "\n", active_count, max_groups);
    int count = 0;
    for (int i = 0; i < max_groups; ++i) {
        if (groups[i].active) {
            // Εμφάνιση index, ονόματος, handle και αριθμού μηνυμάτων που καταγράφηκαν
            printf(ANSI_COLOR_RED "  [%d]: %s (Handle: %d, MsgsLogged: %llu)" ANSI_COLOR_RESET "\n",
                   i, groups[i].name, groups[i].handle, groups[i].delivery_counter);
            count++;
        }
    }
    if (count == 0) {
        printf(ANSI_COLOR_RED "  (None)" ANSI_COLOR_RESET "\n");
    }
}

/**
 * @brief Βρίσκει το index της πρώτης ελεύθερης θέσης στον πίνακα groups.
 * @return Το index της ελεύθερης θέσης ή -1 αν δεν υπάρχει.
 */
int find_free_group_slot(group_membership_t groups[], int max_groups) {
    for (int i = 0; i < max_groups; ++i) {
        if (!groups[i].active) { // Αν η θέση ΔΕΝ είναι ενεργή
            return i; // Επιστροφή του index της
        }
    }
    return -1; // Δεν βρέθηκε ελεύθερη θέση
}

/**
 * @brief Βρίσκει ένα group στον πίνακα groups με βάση το όνομα ή το index που έδωσε ο χρήστης.
 * @param groups Ο πίνακας των group memberships.
 * @param max_groups Το μέγεθος του πίνακα.
 * @param input Η είσοδος του χρήστη (μπορεί να είναι αριθμός-index ή όνομα).
 * @param found_handle Pointer σε int όπου θα αποθηκευτεί το handle του group αν βρεθεί.
 * @return Το index του group στον πίνακα αν βρεθεί, αλλιώς -1.
 */
int find_group_by_input(group_membership_t groups[], int max_groups, const char* input, int *found_handle) {
     if (!input || input[0] == '\0') return -1; // Έλεγχος για άδεια είσοδο
     *found_handle = -1; // Αρχικοποίηση σε "δεν βρέθηκε"

     // 1. Προσπάθεια ερμηνείας ως index (αριθμός)
     char *endptr;
     long idx = strtol(input, &endptr, 10); // Μετατροπή string σε long integer

     // Έλεγχος αν η μετατροπή πέτυχε και διάβασε όλο το string ως αριθμό
     if (endptr != input && *endptr == '\0') {
        // Η είσοδος ήταν αριθμός. Έλεγχος αν είναι έγκυρο *ενεργό* index.
        if (idx >= 0 && idx < max_groups && groups[(int)idx].active) {
            *found_handle = groups[(int)idx].handle; // Βρέθηκε, βάλε το handle
            return (int)idx; // Επιστροφή του index
        } else {
             // Ήταν αριθμός, αλλά εκτός ορίων ή ανενεργό slot
             return -1; // Θεωρείται ότι δεν βρέθηκε
        }
     }

     // 2. Αν δεν ήταν έγκυρο index, προσπάθεια ερμηνείας ως όνομα group
     for (int i = 0; i < max_groups; ++i) {
        if (groups[i].active && strcmp(groups[i].name, input) == 0) { // Αν είναι ενεργό και το όνομα ταιριάζει
             *found_handle = groups[i].handle; // Βρέθηκε, βάλε το handle
             return i; // Επιστροφή του index
        }
     }

     // Δεν βρέθηκε ούτε ως έγκυρο index ούτε ως όνομα
     return -1;
}

/**
 * @brief Καλεί grp_leave για όλα τα ενεργά groups και κλείνει το log file.
 */
void cleanup_resources(group_membership_t groups[], int max_groups) {
    printf(ANSI_COLOR_RED "Cleaning up memberships..." ANSI_COLOR_RESET "\n");
    for(int i=0; i < max_groups; ++i) {
        if (groups[i].active) {
            printf(ANSI_COLOR_RED "Leaving group '%s' (handle %d)..." ANSI_COLOR_RESET "\n", groups[i].name, groups[i].handle);
            grp_leave(groups[i].handle); // Κλήση grp_leave (αγνοούμε το αποτέλεσμα εδώ)
            groups[i].active = 0; // Σήμανση ως ανενεργό για καλή πρακτική
        }
    }

    printf(ANSI_COLOR_RED "Shutting down library..." ANSI_COLOR_RESET "\n");
    // Κλήση συνάρτησης τερματισμού της βιβλιοθήκης, αν υπάρχει
    // grp_shutdown(); // Αποσχολιάστε αν η βιβλιοθήκη σας έχει τέτοια συνάρτηση

    // Κλείσιμο του αρχείου log αν ήταν ανοιχτό
    if (log_fp != NULL) {
        printf(ANSI_COLOR_RED "Closing log file: %s" ANSI_COLOR_RESET "\n", log_filename);
        fclose(log_fp);
        log_fp = NULL;
    }
}