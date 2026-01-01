Source code for GMS and Group communication is in files: gms.c, group_comm.c/h

Compile:

Don't forget to adjust GMS IP and PORT

GMS: make -> ./gms_server
Interactive: make -> ./interactive <member_id>
Receivers (only): make -> ./receivers <num_of_receivers> <group_name>
One Sender (receiving also): make -> ./one_sender <member_id> <group_name> <sending_duration>
Multiple Senders (receiving also): make -> ./multiple_senders <num_of_senders> <group_name> <sending_duration>
