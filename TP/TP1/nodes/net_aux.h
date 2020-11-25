#ifndef _NET_AUX_H_
#define _NET_AUX_H_ 1






/* client fonctions */
void open_connection(int, const char *, int);

/* server functions */
void start_server(int, const char*, int );
int wait_connection(int);
int wait_connection_adr(int, char *);


/* common functions */
int create_socket(void);
void close_connection(int);
void sock_send(int, const char *);
void sock_send_binary(int, const unsigned char*, int);
void sock_receive(int, char *, int);
int message_is(const char *, const char *);

#endif 
