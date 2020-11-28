#define _REENTRANT 
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <pthread.h>

#define DEBUG 1

int  create_socket(void){
    int             sock;
    pid_t           pid = getpid();
    
    sock = socket(AF_INET, SOCK_STREAM, 0);
    
    if (sock < 0) {
	perror("Error in (create_socket)");
	exit(EXIT_FAILURE);
    }
#ifdef DEBUG
    fprintf(stderr, "(%d) Socket created\n", pid);
#endif
    
    return sock;
}


void open_connection(int sock, const char *ip, int port){
    int             ret;
    struct sockaddr_in adr_serveur;
    struct sockaddr_in adr_client;
    pid_t           pid = getpid();
    socklen_t       adr_client_size;

    memset(&adr_serveur, 0, sizeof(adr_serveur));
    adr_serveur.sin_family = AF_INET;
    adr_serveur.sin_port = htons(port);
    inet_aton(ip, &adr_serveur.sin_addr);
    
    
    ret = connect(sock, (struct sockaddr *) & adr_serveur, sizeof(adr_serveur));
    
    if (ret == -1) {
	perror("Error in (request_connection)");
	exit(EXIT_FAILURE);
    }
    
    adr_client_size = sizeof(adr_client);
    ret = getsockname(sock, (struct sockaddr *) &adr_client, &adr_client_size);
    if( ret == -1) {
	perror("Error in (getsockname)");
	exit(EXIT_FAILURE);
    }

#ifdef DEBUG
    fprintf(stderr, "(%d) Requesting a conection to (ip=%s, port=%d)\n\t from (ip=%s, port=%d)\n",
	    pid, inet_ntoa(adr_serveur.sin_addr),  ntohs(adr_serveur.sin_port),
	   inet_ntoa(adr_client.sin_addr),  ntohs(adr_client.sin_port));
#endif
}

void close_connection(int sock){
    int             ret;
    pid_t           pid = getpid();
    
    ret = close(sock);
    if (ret == -1) {
	perror("Error in (close_connection)");
	exit(EXIT_FAILURE);
    }
#ifdef DEBUG
    fprintf(stderr, "(%d) Closing connection\n", pid);
#endif
    
}

void sock_send(int sock, const char *msg){
    unsigned int    num_ecrit = 0;
    pid_t           pid = getpid();
    num_ecrit = write(sock, msg, strlen(msg));
    
    if (num_ecrit != strlen(msg)) {
	perror("Erreur de (write)");
	exit(EXIT_FAILURE);
    }
#ifdef DEBUG
    fprintf(stderr, "(%d) Sending [%s]\n", pid, msg);
#endif

    sleep(1);

}

void sock_send_binary(int sock, const unsigned char *msg, int size){
    int             num_ecrit = 0;
    pid_t           pid = getpid();
    
    num_ecrit = write(sock, msg, size);    
    if (num_ecrit != size) {
	perror("Erreur de (write)");
	exit(EXIT_FAILURE);
    }
#ifdef DEBUG
    fprintf(stderr, "(%d) Sending [%d]\n", pid, size);
#endif
}

void sock_receive(int sock, char *msg, int size){
    int             num_lues = 0;
    pid_t           pid = getpid();
    num_lues = read(sock, msg, size);
    
    if (num_lues == -1) {
	perror("Error in (sock_receive)");
	exit(EXIT_FAILURE);
    }
    msg[num_lues] = '\0';
    
#ifdef DEBUG
    fprintf(stderr, "(%d) Received [%s]\n", pid, msg);
#endif
}

void start_server(int sock, const char *ip, int port){
    int             ret;
    struct sockaddr_in adr_serveur;
    pid_t           pid = getpid();
    
    memset(&adr_serveur, 0, sizeof(adr_serveur));
    adr_serveur.sin_family = AF_INET;
    adr_serveur.sin_port = htons(port);
    inet_aton(ip, &adr_serveur.sin_addr);
    
    
    ret = bind(sock, (struct sockaddr *) & adr_serveur, sizeof(adr_serveur));
    
    if (ret == -1) {
	perror("Error in (start_server)");
	exit(EXIT_FAILURE);
    }
#ifdef DEBUG
    fprintf(stderr, "(%d) Started server on ip: %s and port: %d\n", pid,
	    inet_ntoa(adr_serveur.sin_addr),
	    ntohs(adr_serveur.sin_port));
#endif
}



int wait_connection(int sock) {
    int             ret;
    int             sock_effective;
    socklen_t       adr_client_size;
    struct sockaddr_in adr_client;
    pid_t           pid = getpid();
    
    ret = listen(sock, 2);
    
    if (ret == -1) {
	perror("Error in wait_connection (listen)");
	exit(EXIT_FAILURE);
    }
    adr_client_size = sizeof(adr_client);
    sock_effective = accept(sock, (struct sockaddr *) & adr_client,
			    &adr_client_size);
    
    if (sock_effective == -1) {
	perror("Error in wait_connection (accept)");
	exit(EXIT_FAILURE);
    }
#ifdef DEBUG
    fprintf(stderr, "(%d) Connection requested from (ip : %s, port : %d)\n", pid,
	    inet_ntoa(adr_client.sin_addr),
	    ntohs(adr_client.sin_port));
#endif
    
    return sock_effective;
}

int wait_connection_adr(int sock, char *ip){
    int             ret;
    int             sock_effective;
    socklen_t       adr_client_size;
    struct sockaddr_in adr_client;
    pid_t           pid = getpid();
    
    ret = listen(sock, 2);
    
    if (ret == -1) {
	perror("Error in wait_connection (listen)");
	exit(EXIT_FAILURE);
    }
    adr_client_size = sizeof(adr_client);
    sock_effective = accept(sock, (struct sockaddr *) & adr_client,
			    &adr_client_size);
    
    if (sock_effective == -1) {
	perror("Error in wait_connection (accept)");
	exit(EXIT_FAILURE);
    }
#ifdef DEBUG
    fprintf(stderr, "(%d) Connection requested from ip: %s\n", pid,
	    inet_ntoa(adr_client.sin_addr));
#endif
    
    strncpy(ip, inet_ntoa(adr_client.sin_addr), strlen(inet_ntoa(adr_client.sin_addr)));
    
    return sock_effective;
}


int message_is(const char *msg, const char *expected){
    int             ret;
    
    ret = strncmp(msg, expected, strlen(expected));   
    return (ret == 0);
}
