/* Cette constante permet d’utiliser les versions "thread safe" des */
/* fonction de la lib C elle est OBLIGATOIRE */
#define _REENTRANT

#define DEBUG

#include <algorithm>
#include <cstring>
#include <dirent.h> 
#include <experimental/filesystem>
#include <iostream>
#include <fstream>
#include <mutex>
#include <pthread.h>
#include <signal.h>
#include <string>
#include <vector>
#include <unistd.h>

#include "net_aux.h"

#define BUFFERMAX 100
#define CHECK_INTERVAL 3000000

// Event names
#define DATA_AVAILABLE "dataAvailable"
#define EVT_DATA_AVAILABLE_ACK "dataAvailableAck"
#define EVT_GET_NODES "getNodes"
#define EVT_GET_NODES_END "endNodeList"
#define EVT_GET_DATA "getDataFile"
#define EVT_GET_DATA_END "getDataFileEnd"
#define SEEK_ECO_CONFIG "seekEcoConfig"
#define SEEK_MIN_CONFIG "seekMinConfig"
#define SEEK_FULL_CONFIG "seekFullConfig"
#define SEEK_ECO_CONFIG_ACK "seekEcoConfigAck"
#define SEEK_MIN_CONFIG_ACK "seekMinConfigAck"
#define SEEK_FULL_CONFIG_ACK "seekFullConfigAck"
#define SEND_ECO_CONFIG "sendEcoConfig"
#define SEND_MIN_CONFIG "sendMinConfig"
#define SEND_FULL_CONFIG "sendFullConfig"



typedef struct {
	int sock;
	char ip_addr[15];
} server_params;

int srv_sock;
std::string ip_addr_df("127.0.0.1"); //adresse par defaut
int port = 8000; // Port par defaut
int sensor_port = 8002;
std::vector<std::string> nodeList;
std::vector<std::string> dataList;
std::vector<std::string> dataConfig;
std::vector<std::string> nodeswithdata;
std::vector<std::string> nodesSeekingEco;
std::vector<std::string> nodesSeekingMin;
std::vector<std::string> nodesSeekingFull;

std::string dataDirectory = "database";

std::mutex nodeListMutex, dataListMutex, nodeswithdataMutex, nodesSeekingEcoMutex, nodesSeekingMinMutex, nodesSeekingFullMutex;

/* A server instance answering client commands */
void* serverFunc(void *s) {
	
	server_params* params = (server_params*) s;
	
	int socket = params->sock;
	std::string ip_client(params->ip_addr);
	
	std::cout << "[Sink SERVER] Receiving message from ip : " << ip_client.c_str() << std::endl;
	std::cout << "[Sink SERVER] For transmission on socket : " << socket << std::endl;
	
	//int socket = *((int*)(&s));
	char buf[BUFFERMAX];

	// Reading command
	sock_receive(socket, buf, BUFFERMAX);

	std::cout << "[Sink SERVER] Receiving message: " << buf << std::endl;
	
	std::string message(buf);
	std::string command = message.substr(0,message.find("."));
	std::string ip_sensor = message.substr(message.find(".")+1,message.size());
	
	std::cout << "[Sink SERVER Collector] Receiving message: " << command << std::endl;
	std::cout << "[Sink SERVER Collector] Received sensor address : " << ip_sensor.c_str() << std::endl;
	// Answering command Notify Data Available
	if (message_is(buf, DATA_AVAILABLE)) {
	
		nodeListMutex.lock();
		nodeswithdataMutex.lock();
		//Parcourir la liste des capteurs connus pour savoir si ip_client y est 
		if (std::find(nodeList.begin(), nodeList.end(), ip_sensor) == nodeList.end()) {
			std::cout << "[Sink CLIENT] New sensor node: " << ip_sensor.c_str() << std::endl;
			nodeList.push_back(ip_sensor);
		} else {//En fonctionnement normal, enlever l'affichage ci-dessous
			std::cout << "[Sink CLIENT] Node already stored: " << ip_sensor.c_str() << std::endl;
		}
		if (std::find(nodeswithdata.begin(), nodeswithdata.end(), ip_sensor) == nodeswithdata.end()) {
			//Affichage à enlever en fonctionnement normal
			std::cout << "[Sink CLIENT] Sensor node having data : " << ip_sensor.c_str() << std::endl;
			nodeswithdata.push_back(ip_sensor);
		}
		sock_send(socket, EVT_DATA_AVAILABLE_ACK);
		nodeswithdataMutex.unlock();
		nodeListMutex.unlock();
		
		
	}
	// Answering command Seek Eco_config
	std::cout << "buf : " << buf<< "  --  evt : "<< SEEK_ECO_CONFIG<<std::endl;
	if (message_is(buf, SEEK_ECO_CONFIG)) {
	
		
		nodesSeekingEcoMutex.lock();
		//Parcourir la liste des capteurs connus pour savoir si ip_client y est 
		if (std::find(nodesSeekingEco.begin(), nodesSeekingEco.end(), ip_sensor) == nodesSeekingEco.end()) {
			std::cout << "[Sink CLIENT] New sensor seeking ECO node: " << ip_sensor.c_str() << std::endl;
			nodesSeekingEco.push_back(ip_sensor);
		} else {//En fonctionnement normal, enlever l'affichage ci-dessous
			std::cout << "[Sink CLIENT] Node seeking ECO already stored: " << ip_sensor.c_str() << std::endl;
		}

		sock_send(socket, SEEK_ECO_CONFIG_ACK);
		nodesSeekingEcoMutex.unlock();
		
	} 
	
		// Answering command Seek Min_config
		         
		if (message_is(buf, SEEK_MIN_CONFIG)) {
	
		
		nodesSeekingMinMutex.lock();
		//Parcourir la liste des capteurs connus pour savoir si ip_client y est 
		if (std::find(nodesSeekingMin.begin(), nodesSeekingMin.end(), ip_sensor) == nodesSeekingMin.end()) {
			std::cout << "[Sink CLIENT] New sensor node seeking MIN: " << ip_sensor.c_str() << std::endl;
			nodesSeekingMin.push_back(ip_sensor);
		} else {//En fonctionnement normal, enlever l'affichage ci-dessous
			std::cout << "[Sink CLIENT] Node seeking MIN already stored: " << ip_sensor.c_str() << std::endl;
		}

		sock_send(socket, SEEK_MIN_CONFIG_ACK);
		nodesSeekingMinMutex.unlock();
		
	} 
	
	
			// Answering command Seek Full_config
	if (message_is(buf, SEEK_FULL_CONFIG)) {
	
		
		nodesSeekingFullMutex.lock();
		//Parcourir la liste des capteurs connus pour savoir si ip_client y est 
		if (std::find(nodesSeekingFull.begin(), nodesSeekingFull.end(), ip_sensor) == nodesSeekingFull.end()) {
			std::cout << "[Sink CLIENT] New sensor node: " << ip_sensor.c_str() << std::endl;
			nodesSeekingFull.push_back(ip_sensor);
		} else {//En fonctionnement normal, enlever l'affichage ci-dessous
			std::cout << "[Sink CLIENT] Node already stored: " << ip_sensor.c_str() << std::endl;
		}

		sock_send(socket, SEEK_FULL_CONFIG_ACK);
		nodesSeekingFullMutex.unlock();
		
	} 
	
	// Closing connection
	close_connection(socket);
	
	free(params);

	pthread_exit(NULL);
}

/* Le thread d'ecoute */
void* listenServer(void *n) {
	/* création de la socket de communication */
	srv_sock = create_socket();

	/* initialisation de la structure representant l'adresse */
	start_server(srv_sock, ip_addr_df.c_str(), port);

	while(1){
			
		server_params* params = (server_params*)malloc(sizeof(server_params));
		
		char buf[BUFFERMAX];
		
		/* Attendre les requêtes de connexion */
		int sock_effective = wait_connection_adr(srv_sock, buf);
		
		//memset(params,0,sizeof(server_params));
		params->sock = sock_effective;
		
		std::cout << "[Sink_Listener] Connection accepted at socket : " << params->sock << std::endl;
		
		strncpy(params->ip_addr, buf, strlen(buf));
		
		std::cout << "[Sink_Listener] Connection accepted from : " << params->ip_addr << std::endl;

		pthread_t thrd;
		if (pthread_create(&thrd, NULL, serverFunc, (void *) params) != 0) {
			std::cerr << "[Sink SERVER] Error while creating a new thread" << std::endl;
			pthread_exit(NULL);
		}
		usleep(500000);
	}
}


void* Sent_Eco(void *i) {
	char* ip_serveur = *((char**)(&i));
	char buf[BUFFERMAX];  /* buffer pour les données reçues*/

	/* Création de la socket de communication */
	int clt_sock = create_socket();

	/* demande d'une connexion au serveur */
	open_connection(clt_sock, ip_serveur, sensor_port);
	
	sock_send(clt_sock, SEND_ECO_CONFIG);

	/* envoi du message au le serveur */
	sock_send(clt_sock, dataConfig.at(0).c_str());
		
	sock_send(clt_sock, SEEK_ECO_CONFIG_ACK);
	

	/* fermeture de la socket */
	close_connection(clt_sock);
}

void* Sent_Min(void *i) {
	char* ip_serveur = *((char**)(&i));
	char buf[BUFFERMAX];  /* buffer pour les données reçues*/

	/* Création de la socket de communication */
	int clt_sock = create_socket();

	/* demande d'une connexion au serveur */
	open_connection(clt_sock, ip_serveur, sensor_port);
	
	sock_send(clt_sock, SEND_MIN_CONFIG);

	/* envoi du message au le serveur */
	sock_send(clt_sock, dataConfig.at(1).c_str());

	sock_send(clt_sock, SEEK_ECO_CONFIG_ACK);

	/* fermeture de la socket */
	close_connection(clt_sock);
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
			std::cout << "[Sink CLIENT] New node: " << newNode << std::endl;
			nodeList.push_back(newNode);
		} else {
			std::cout << "[Sink CLIENT] Node already stored: " << newNode << std::endl;
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
	open_connection(clt_sock, ip_serveur, sensor_port);

	/* envoi du message au serveur */
	sock_send(clt_sock, EVT_GET_DATA);

	nodeswithdataMutex.lock();
	dataListMutex.lock();
	/* Lecture du message du client */
	while (true) {
		sock_receive(clt_sock, buf, BUFFERMAX);

		if (message_is(buf, EVT_GET_DATA_END)) { 
			std::vector<std::string>::iterator it = std::find(nodeswithdata.begin(), nodeswithdata.end(), ip_serveur_str);
			if (it != nodeswithdata.end())
				nodeswithdata.erase(it);
			break; 
		}else {
		
		std::string newdata(buf);
		//std::string newdataWithIP = ip_serveur_str + "_" + newdata;
		std::string newdataWithIP = newdata;
		if (std::find(dataList.begin(), dataList.end(), newdataWithIP) == dataList.end()) {
			std::cout << "[Sink CLIENT] New data: " << newdataWithIP << std::endl;
			FILE* datafile;
			std::string filename(dataDirectory + "/" + newdataWithIP);
			datafile = fopen(filename.c_str(), "w");
			fclose(datafile);
			dataList.push_back(newdataWithIP);
		} else {
			std::cout << "[Sink CLIENT] Data already stored: " << newdataWithIP << std::endl;
		}
		}
	}
	
	dataListMutex.unlock();
	nodeswithdataMutex.unlock();
	std::cout << "[Sink CLIENT] List of current data in database " << std::endl;	
	for (std::string dataname : dataList) {
		std::cout << "[Sink CLIENT] " << dataname << std::endl;	
	}
	
	
	/* fermeture de la socket */
	close_connection(clt_sock);
}

/* Client principal */
void* clientFunc(void *n) {
	while(1) {
		
		std::cout << "[Sink CLIENT] Waiting data ..." << std::endl;

		nodeswithdataMutex.lock();
		dataListMutex.lock();
			for (std::string ip : nodeswithdata) {
				std::cout << "[Sink CLIENT] Seeking data list from " << ip << std::endl;

				pthread_t traitement;
				if(pthread_create(&traitement, NULL, updateDataList, (void *)ip.c_str()) != 0) {
					std::cerr << "[Sink CLIENT] Error while creating a new thread: client sensor data update" << std::endl;
					pthread_exit(NULL);
				}
			}
		nodeswithdataMutex.unlock();
		dataListMutex.unlock();
		
		//Response ECO_CONFIG
		nodesSeekingEcoMutex.lock();
			for (std::string ip : nodesSeekingEco) {
			   std::cout << "[Sink CLIENT] Seeking Eco_Config file from " << ip << std::endl;

				pthread_t traitement;
				if(pthread_create(&traitement, NULL, Sent_Eco, (void *)ip.c_str()) != 0) {
					std::cerr << "[Sink CLIENT] Error while creating a new thread: client sensor data update" << std::endl;
					pthread_exit(NULL);
				}
			}
		nodesSeekingEcoMutex.unlock();
		
		
		//Response MIN_CONFIG
		nodesSeekingMinMutex.lock();
			for (std::string ip : nodesSeekingMin) {
			   std::cout << "[Sink CLIENT] Seeking Min_Config file from " << ip << std::endl;

				pthread_t traitement;
				if(pthread_create(&traitement, NULL, Sent_Min, (void *)ip.c_str()) != 0) {
					std::cerr << "[Sink CLIENT] Error while creating a new thread: client sensor data update" << std::endl;
					pthread_exit(NULL);
				}
			}
		nodesSeekingMinMutex.unlock();
		
		
		
		usleep(CHECK_INTERVAL);
	}
}

// Procédure principale
int main(int argc, char* argv[]) {
	// Traitement des arguments
	if (argc == 0) { return -1; }

	for (int i = 0; i < argc; i++) {
		std::string strarg(argv[i]);

		if(strarg == "-a") {
			if (i < (argc - 1)) {
				ip_addr_df = argv[++i];
			} else {
				std::cerr << "[MAIN] Error: no specified ip address" << std::endl;
				exit(EXIT_FAILURE);
			}
		} else if(strarg == "-p") {
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
				std::cerr << "[MAIN] Erreur: no specified data directory" << std::endl;
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
	std::cout << "Adding data Config to list" << std::endl;

	DIR           *dir = opendir(dataDirectory.c_str());
	struct dirent *pdir;
	while ((pdir = readdir(dir))) {
		if(std::strlen(pdir->d_name) < 5) { continue; }
		dataConfig.push_back(pdir->d_name);
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
