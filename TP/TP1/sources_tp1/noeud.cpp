/* Cette constante permet d’utiliser les versions "thread safe" des */
/* fonction de la lib C elle est OBLIGATOIRE */
#define _REENTRANT

#define DEBUG

#include <algorithm>
#include <cstring>
#include <dirent.h> 
#include <experimental/filesystem>
#include <iostream>
#include <mutex>
#include <pthread.h>
#include <signal.h>
#include <string>
#include <vector>
#include <unistd.h>

#include "net_aux.h"

#define BUFFERMAX 100
#define BIND_ADDR "127.0.0.1"
#define CHECK_INTERVAL 5000000

// Event names
#define EVT_GET_NODES "getNodes"
#define EVT_GET_NODES_END "endNodeList"
#define EVT_GET_JOKESTITLES "getJokesTitles"
#define EVT_GET_JOKESTITLES_END "getJokesTitlesEnd"

int srv_sock;
int port = 8000; // Port par defaut
std::vector<std::string> nodeList;
std::vector<std::string> jokes;
std::string jokeDirectory = "jokes/";

std::mutex nodeListMutex, jokeListMutex;

/* A server instance answering client commands */
void* serverFunc(void *s) {
	int socket = *((int*)(&s));
	char buf[BUFFERMAX];

	// Reading command
	sock_receive(socket, buf, BUFFERMAX);

	std::cout << "[SERVER] Recieving message: " << buf << std::endl;

	// Answering command
	if (message_is(buf, EVT_GET_NODES)) {
		nodeListMutex.lock();
			for (std::string str : nodeList) {
				sock_send(socket, str.c_str());
			}
			sock_send(socket, EVT_GET_NODES_END);
		nodeListMutex.unlock();
	} else if (message_is(buf, EVT_GET_JOKESTITLES)) {
		jokeListMutex.lock();
			for (std::string str : jokes) {
				sock_send(socket, str.c_str());
			}
		sock_send(socket, EVT_GET_JOKESTITLES_END);
		jokeListMutex.unlock();
	}

	// Closing connection
	close_connection(socket);

	pthread_exit(NULL);
}

/* Le thread d'ecoute */
void* listenServer(void *n) {
	/* création de la socket de communication */
	srv_sock = create_socket();

	/* initialisation de la structure representant l'adresse */
	start_server(srv_sock, BIND_ADDR, port);

	while(1){
		/* Attendre les requêtes de connexion */
		int sock_effective = wait_connection(srv_sock);

		pthread_t thrd;
		if (pthread_create(&thrd, NULL, serverFunc, (void *)sock_effective) != 0) {
			std::cerr << "[SERVER] Error while creating a new thread" << std::endl;
			pthread_exit(NULL);
		}
	}
}

/* Mise a jour de la liste des nodes */
void* updateNodeList(void *i) {
	char* ip_serveur = *((char**)(&i));
	char buf[BUFFERMAX];  /* buffer pour les données reçues*/

	/* Création de la socket de communication */
	int clt_sock = create_socket();

	/* demande d'une connexion au serveur */
	open_connection(clt_sock, ip_serveur, port);

	/* envoi du message au le serveur */
	sock_send(clt_sock, EVT_GET_NODES);

	/* Lecture du message du client */
	while(true) {
		sock_receive(clt_sock, buf, BUFFERMAX);

		if (message_is(buf, EVT_GET_NODES_END)) { break; }

		std::string newNode(buf);
		if (std::find(nodeList.begin(), nodeList.end(), newNode) == nodeList.end()) {
			std::cout << "[CLIENT] New node: " << newNode << std::endl;
			nodeList.push_back(newNode);
		} else {
			std::cout << "[CLIENT] Node already stored: " << newNode << std::endl;
		}
	}

	/* fermeture de la socket */
	close_connection(clt_sock);
}

/* Mise a jour de la liste des blagues */
void* updateJokeList(void *i) {
	char* ip_serveur = *((char**)(&i));
	std::string ip_serveur_str(ip_serveur);
	char buf[BUFFERMAX];  /* buffer pour les données reçues*/

	/* Création de la socket de communication */
	int clt_sock = create_socket();

	/* demande d'une connexion au serveur */
	open_connection(clt_sock, ip_serveur, port);

	/* envoi du message au le serveur */
	sock_send(clt_sock, EVT_GET_JOKESTITLES);

	/* Lecture du message du client */
	while (true) {
		sock_receive(clt_sock, buf, BUFFERMAX);

		if (message_is(buf, EVT_GET_JOKESTITLES_END)) { break; }

		std::string newJoke(buf);
		std::string newJokeWithIP = ip_serveur_str + "/" + newJoke;
		if (std::find(jokes.begin(), jokes.end(), newJoke) == jokes.end() && std::find(jokes.begin(), jokes.end(), newJokeWithIP) == jokes.end()) {
			std::cout << "[CLIENT] New joke: " << newJokeWithIP << std::endl;
			jokes.push_back(newJokeWithIP);
		} else {
			std::cout << "[CLIENT] Joke already stored: " << newJokeWithIP << std::endl;
		}
	}

	/* fermeture de la socket */
	close_connection(clt_sock);
}

/* Client principal */
void* clientFunc(void *n) {
	while(1) {
		std::cout << "[CLIENT] Updating data ..." << std::endl;
		nodeListMutex.lock();
			for (std::string ip : nodeList) {
				std::cout << "[CLIENT] Seeking node list from " << ip << std::endl;

				pthread_t traitement;
				if(pthread_create(&traitement, NULL, updateNodeList, (void *)ip.c_str()) != 0) {
					std::cerr << "[CLIENT] Error while creating a new thread: client node update" << std::endl;
					pthread_exit(NULL);
				}
			}
		nodeListMutex.unlock();
		usleep(CHECK_INTERVAL);
		jokeListMutex.lock();
			for (std::string ip : nodeList) {
				std::cout << "[CLIENT] Seeking joke list from " << ip << std::endl;

				pthread_t traitement;
				if(pthread_create(&traitement, NULL, updateJokeList, (void *)ip.c_str()) != 0) {
					std::cerr << "[CLIENT] Error while creating a new thread: client jokes update" << std::endl;
					pthread_exit(NULL);
				}
			}
		jokeListMutex.unlock();
		usleep(CHECK_INTERVAL);
	}
}

// Procédure principale
int main(int argc, char* argv[]) {
	// Traitement des arguments
	if (argc == 0) { return -1; }

	for (int i = 0; i < argc; i++) {
		std::string strarg(argv[i]);

		if(strarg == "-p") {
			if (i < (argc - 1)) {
				port = atoi(argv[++i]);
			} else {
				std::cerr << "[MAIN] Error: no specified port" << std::endl;
				exit(EXIT_FAILURE);
			}

		} else if(strarg == "-d") {
			if (i < (argc - 1)) {
				std::string dir(argv[++i]);
				jokeDirectory = dir;
			} else {
				std::cerr << "[MAIN] Erreur: no specified joke directory" << std::endl;
				exit(EXIT_FAILURE);
			}
		} else if(strarg == "-l") {
			++i;
			while (i < argc) {
				std::string noeud(argv[i]);
				nodeList.push_back(noeud);
				i++;
			}
		}
	}

	// jokes
	std::cout << "Adding jokes to list" << std::endl;

	DIR           *dir = opendir(jokeDirectory.c_str());
	struct dirent *pdir;
	while ((pdir = readdir(dir))) {
		if(std::strlen(pdir->d_name) < 5) { continue; }
		jokes.push_back(pdir->d_name);
		std::cout << "  Added " << pdir->d_name << " to joke list." << std::endl;
	}

	// threads
	pthread_t client,
			  listen;

	if (pthread_create(&client, NULL, clientFunc, NULL) != 0) {
		std::cerr << "[MAIN] Error while creating a new thread: client" << std::endl;
		pthread_exit(NULL);
	}

	if (pthread_create(&listen, NULL, listenServer, NULL) != 0) {
		std::cerr << "[MAIN] Error while creating a new thread: server" << std::endl;
		pthread_exit(NULL);
	}

	while(true) { } // Don't stop, we need to let thread run

	return EXIT_SUCCESS;
}