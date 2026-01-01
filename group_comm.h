#ifndef GROUP_COMM_H
#define GROUP_COMM_H

#include <stdint.h> 

#define MAX_ID_LEN 128
#define MAX_GROUP_LEN 128
#define UDP_BUFFER_SIZE 65507

int grp_init(char *gms_addr, int gms_port);
int grp_join(char *grpname, char *myid);
int grp_leave(int g);
int grp_send(int g, void *msg, int len);
int grp_recv(int g, void *msg, int *len, int block);
void grp_shutdown();
void print_instrumentation_counters();

#endif // GROUP_COMM_H