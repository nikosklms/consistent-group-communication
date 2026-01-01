# Makefile for GMS Server, Test Harness, and potentially other clients

# Compiler and flags
CC = gcc
# Use _GNU_SOURCE to potentially enable htobe64/be64toh if not exposed by default
CFLAGS = -g -Wall -pthread -D_GNU_SOURCE -fsanitize=address
LDFLAGS = -pthread -lm -fsanitize=address
LIBS =

# --- Target Executables ---
TARGET_GMS = gms_server
TARGET_MAIN = receivers_only
TARGET_ONE_SENDER = one_sender
TARGET_MULTI_SENDER = multiple_senders
TARGET_INTERACTIVE = interactive

# --- Source Files ---
# Library Source (Compile this once)
LIB_SRC = group_comm.c
LIB_OBJ = $(LIB_SRC:.c=.o)

# GMS Server Source
SRCS_GMS = gms.c
OBJS_GMS = $(SRCS_GMS:.c=.o)

# Other client sources
SRCS_MAIN = receivers.c
OBJS_MAIN = $(SRCS_MAIN:.c=.o)
SRCS_ONE_SENDER = one_sender.c
OBJS_ONE_SENDER = $(SRCS_ONE_SENDER:.c=.o)
SRCS_MULTI_SENDER = multiple_senders.c
OBJS_MULTI_SENDER = $(SRCS_MULTI_SENDER:.c=.o)
SRCS_INTERACTIVE = interactive.c
OBJS_INTERACTIVE = $(SRCS_INTERACTIVE:.c=.o)


# Default target: Build all defined targets
.PHONY: all
all: $(TARGET_GMS) $(TARGET_HARNESS) $(TARGET_MAIN) $(TARGET_ONE_SENDER) $(TARGET_MULTI_SENDER) $(TARGET_INTERACTIVE)

# --- Linking Rules ---

# GMS server executable (doesn't link the grp library)
$(TARGET_GMS): $(OBJS_GMS)
	@echo "Linking $@..."
	$(CC) $(LDFLAGS) $^ -o $@ $(LIBS)
	@echo "$@ built successfully."

# Main program executable (links its object file(s) and the library object file)
$(TARGET_MAIN): $(OBJS_MAIN) $(LIB_OBJ)
	@echo "Linking $@..."
	# $^ now includes receivers.o and group_comm.o
	$(CC) $(LDFLAGS) $^ -o $@ $(LIBS)
	@echo "$@ built successfully."

# One Sender executable (links its object file(s) and the library object file)
$(TARGET_ONE_SENDER): $(OBJS_ONE_SENDER) $(LIB_OBJ)
	@echo "Linking $@..."
	# $^ now includes one_sender.o and group_comm.o
	$(CC) $(LDFLAGS) $^ -o $@ $(LIBS)
	@echo "$@ built successfully."

# Multiple Senders executable (links its object file(s) and the library object file)
$(TARGET_MULTI_SENDER): $(OBJS_MULTI_SENDER) $(LIB_OBJ)
	@echo "Linking $@..."
	# $^ now includes multiple_senders.o and group_comm.o
	$(CC) $(LDFLAGS) $^ -o $@ $(LIBS)
	@echo "$@ built successfully."

# Interactive
$(TARGET_INTERACTIVE): $(OBJS_INTERACTIVE) $(LIB_OBJ)
	@echo "Linking $@..."
	# $^ now includes interactive.o and group_comm.o
	$(CC) $(LDFLAGS) $^ -o $@ $(LIBS)
	@echo "$@ built successfully."


# --- Compilation Rule ---

# Pattern rule to compile any .c to .o
# This handles group_comm.c, gms.c, grp_test_harness.c, main.c etc.
%.o: %.c
	@echo "Compiling $<..."
	$(CC) $(CFLAGS) -c $< -o $@

# --- Clean Target ---
.PHONY: clean
clean:
	@echo "Cleaning up..."
	# Remove all object files defined above
	# *** Corrected: Added objects for other targets ***
	rm -f $(LIB_OBJ) $(OBJS_GMS) $(OBJS_HARNESS) $(OBJS_MAIN) $(OBJS_ONE_SENDER) $(OBJS_MULTI_SENDER) $(OBJS_INTERACTIVE)
	# Remove all target executables defined above
	# *** Corrected: Added other targets ***
	rm -f $(TARGET_GMS) $(TARGET_HARNESS) $(TARGET_MAIN) $(TARGET_ONE_SENDER) $(TARGET_MULTI_SENDER) $(TARGET_INTERACTIVE)
	# Remove log files from test harness
	rm -f output_*.log
	# Remove barrier directory
	rm -rf /tmp/grp_barrier_dir
	@echo "Cleanup complete."