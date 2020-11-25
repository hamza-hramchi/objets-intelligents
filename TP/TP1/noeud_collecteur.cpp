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
#define EVT_DATA_AVAILABLE "dataAvailable"
#define EVT_DATA_AVAILABLE_ACK "dataAvailableAck"
#define EVT_GET_NODES "getNodes"
#define EVT_GET_NODES_END "endNodeList"
#define EVT_GET_DATA "getDataFile"
#define EVT_GET_DATA_END "getDataFileEnd"

typedef struct{
	int sock;
	std::string ip_addr;
} server_params;

int srv_sock;
int port = 8000; // Port par defaut
std::vector<std::string> nodeList;
std::vector<std::string> dataList;
std::vector<std::string> nodeswithdata;
std::string dataDirectory = "database/";

std::mutex nodeListMutex, dataListMutex, nodeswithdataMutex;

/* A server instance answering client commands */
void* serverFunc(void *s) {
	
	server_params params = *((server_params*)&s);
	int socket = params.sock;
	std::string ip_client = params.ip_addr;
	
	std::cout << "[SERVER] Recieving message from ip : " << ip_client.c_str() << std::endl;
	
	//int socket = *((int*)(&s));
	char buf[BUFFERMAX];

	// Reading command
	sock_receive(socket, buf, BUFFERMAX);

	std::cout << "[SERVER] Receiving message: " << buf << std::endl;
	
	// Answering command
	if (message_is(buf, EVT_DATA_AVAILABLE)) {
	
		nodeListMutex.lock();
		nodeswithdataMutex.lock();
		//Parcourir la liste des capteurs connus pour savoir si ip_client y est 
		if (std::find(nodeList.begin(), nodeList.end(), ip_client) == nodeList.end()) {
			std::cout << "[CLIENT] New sensor node: " << ip_client << std::endl;
			nodeList.push_back(ip_client);
		} else {//En fonctionnement normal, enlever l'affichage ci-dessous
			std::cout << "[CLIENT] Node already stored: " << ip_client << std::endl;
		}
		if (std::find(nodeswithdata.begin(), nodeswithdata.end(), ip_client) == nodeswithdata.end()) {
			//Affichage à enlever en fonctionnement normal
			std::cout << "[CLIENT] Sensor node having data : " << ip_client << std::endl;
			nodeswithdata.push_back(ip_client);
		}
		sock_send(socket, EVT_DATA_AVAILABLE_ACK);
		nodeswithdataMutex.unlock();
		nodeListMutex.unlock();
	} 
	//else if (message_is(buf, EVT_GET_JOKESTITLES)) {
	//	jokeListMutex.lock();
	//		for (std::string str : jokes) {
	//			sock_send(socket, str.c_str());
	//		}
	//	sock_send(socket, EVT_GET_JOKESTITLES_END);
	//	jokeListMutex.unlock();
	//}
	

	// Answering command
	//if (message_is(buf, EVT_GET_NODES)) {
	//	nodeListMutex.lock();
	//		for (std::string str : nodeList) {
	//			sock_send(socket, str.c_str());
	//		}
	//		sock_send(socket, EVT_GET_NODES_END);
	//	nodeListMutex.unlock();
	//} else if (message_is(buf, EVT_GET_JOKESTITLES)) {
	//	jokeListMutex.lock();
	//		for (std::string str : jokes) {
	//			sock_send(socket, str.c_str());
	//		}
	//	sock_send(socket, EVT_GET_JOKESTITLES_END);
	//	jokeListMutex.unlock();
	//}

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
			
		server_params params;
		char buf[BUFFERMAX];
		
		/* Attendre les requêtes de connexion */
		int sock_effective = wait_connection_adr(srv_sock, buf);
		
		memset(&params,0,sizeof(params));
		params.sock = sock_effective;
		std::string newNode(buf);
		params.ip_addr = newNode;

		pthread_t thrd;
		if (pthread_create(&thrd, NULL, serverFunc, (void *)params) != 0) {
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
void* updateDataList(void *i) {
	char* ip_serveur = *((char**)(&i));
	std::string ip_serveur_str(ip_serveur);
	char buf[BUFFERMAX];  /* buffer pour les données reçues*/

	/* Création de la socket de communication */
	int clt_sock = create_socket();

	/* demande d'une connexion au serveur */
	open_connection(clt_sock, ip_serveur, port);

	/* envoi du message au serveur */
	sock_send(clt_sock, EVT_GET_DATA);

	/* Lecture du message du client */
	while (true) {
		sock_receive(clt_sock, buf, BUFFERMAX);

		if (message_is(buf, EVT_GET_DATA_END)) { break; }

		std::string newdata(buf);
		std::string newdataWithIP = ip_serveur_str + "/" + newdata;
		if (std::find(dataList.begin(), dataList.end(), newdata) == dataList.end() && std::find(dataList.begin(), dataList.end(), newdataWithIP) == dataList.end()) {
			std::cout << "[CLIENT] New data: " << newdataWithIP << std::endl;
			dataList.push_back(newdataWithIP);
		} else {
			std::cout << "[CLIENT] Data already stored: " << newdataWithIP << std::endl;
		}
	}

	/* fermeture de la socket */
	close_connection(clt_sock);
}

/* Client principal */
void* clientFunc(void *n) {
	while(1) {
		
		std::cout << "[CLIENT] Updating data ..." << std::endl;
		//nodeListMutex.lock();
		//	for (std::string ip : nodeList) {
		//		std::cout << "[CLIENT] Seeking node list from " << ip << std::endl;

		//		pthread_t traitement;
		//		if(pthread_create(&traitement, NULL, updateNodeList, (void *)ip.c_str()) != 0) {
		//			std::cerr << "[CLIENT] Error while creating a new thread: client node update" << std::endl;
		//			pthread_exit(NULL);
		//		}
		//	}
		//nodeListMutex.unlock();
		//usleep(CHECK_INTERVAL);
		nodeswithdataMutex.lock();
		dataListMutex.lock();
			for (std::string ip : nodeswithdata) {
				std::cout << "[CLIENT] Seeking data list from " << ip << std::endl;

				pthread_t traitement;
				if(pthread_create(&traitement, NULL, updateDataList, (void *)ip.c_str()) != 0) {
					std::cerr << "[CLIENT] Error while creating a new thread: client jokes update" << std::endl;
					pthread_exit(NULL);
				}
			}
		nodeswithdataMutex.unlock();
		dataListMutex.unlock();
		//usleep(CHECK_INTERVAL);
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
				dataDirectory = dir;
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

	// data
	std::cout << "Adding data to list" << std::endl;

	DIR           *dir = opendir(dataDirectory.c_str());
	struct dirent *pdir;
	while ((pdir = readdir(dir))) {
		if(std::strlen(pdir->d_name) < 5) { continue; }
		dataList.push_back(pdir->d_name);
		std::cout << "  Added " << pdir->d_name << " to data list." << std::endl;
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
