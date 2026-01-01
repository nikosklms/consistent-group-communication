# Group Membership Service (GMS) & Reliable Group Communication

## Overview

This project implements a distributed system consisting of a central Group Membership Service (GMS) and a robust Group Communication Layer. It provides reliable, causally and totally ordered message delivery with View Synchronicity to handle member failures dynamically.

## Architecture

### 1. Group Membership Service (GMS)

The GMS acts as the central coordinator for group management.

- **Model:** TCP-based "Thread-Per-Client" architecture.
- **Functionality:** Handles JOIN and LEAVE requests and maintains the current view of group members.
- **Storage:** Uses a thread-safe Hash Table to map group names to member lists.
- **Failure Detection:** Implements a heartbeat mechanism. A Liveness Thread periodically sends PING messages. If a PONG is not received within the timeout, the member is marked as failed, and the group is notified.

### 2. Group Communication Layer

A library allowing clients to communicate within groups using UDP, while maintaining control signaling with the GMS via TCP.

- **Reliability:** Uses local sequence numbers, ACKs, and a retransmission thread to ensure message delivery.
- **Total Ordering:** Implements a Sequencer-based protocol. One member (Sequencer) assigns global sequence numbers (TYPE_SEQ_ASSIGNMENT) to ensure all members deliver messages in the same order.
- **Causal Ordering:** Utilizes Vector Clocks to guarantee that messages are delivered respecting cause-and-effect relationships.
- **View Synchronicity:** Handles failures via a protocol (Start → State → Confirm). When a failure is detected, the system pauses delivery, agrees on a global sequence number, and flushes unstable messages before installing the new view.

## Project Structure

- `gms.c`: Source code for the GMS server.
- `group_comm.c`: Implementation of the client library and protocol logic.
- `group_comm.h`: Header file containing API definitions, message structures (TCP/UDP), and constants.

## Prerequisites

- Linux/Unix environment
- GCC Compiler
- GNU Make
- Pthreads library

## Configuration

Before compiling, verify the IP address and PORT settings in the source code. Ensure the client library knows the IP address where `gms_server` will be running.

## Compilation

To compile the GMS server and all client test executables, run:

```bash
make
```

## Usage

### 1. Start the GMS Server

The server must be running before clients attempt to connect.

```bash
./gms_server
```

### 2. Run Clients

Different executables are available for testing various aspects of the protocol.

#### Interactive Mode

```bash
./interactive <member_id>
```

#### Receivers Only

```bash
./receivers <num_of_receivers> <group_name>
```

#### One Sender

```bash
./one_sender <member_id> <group_name> <sending_duration>
```

#### Multiple Senders

```bash
./multiple_senders <num_of_senders> <group_name> <sending_duration>
```

## Protocol Details

### TCP Messages (GMS Control)

- `MSG_TYPE_JOIN` / `RESP_ACK_SUCCESS`
- `MSG_TYPE_LEAVE`
- `MSG_TYPE_PING` / `MSG_TYPE_PONG`
- `NOTIFY_MEMBER_JOINED` / `LEFT` / `FAILED`

### UDP Messages (Group Data)

- `TYPE_DATA_UNSTABLE`
- `TYPE_SEQ_REQUEST`
- `TYPE_SEQ_ASSIGNMENT`
- `TYPE_ACK`
- `TYPE_VIEW_CHANGE_*`
