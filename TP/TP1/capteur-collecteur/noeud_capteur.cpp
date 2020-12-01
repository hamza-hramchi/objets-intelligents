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

#include "lib/net_aux.h"

#define BUFFERMAX 100
//#define BIND_ADDR "127.0.1.1"
#define CHECK_INTERVAL 2000000

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


typedef struct{
	int sock;
	char ip_addr[15];
} server_params;

int srv_sock;
std::string ip_addr_df("127.0.1.1"); //adresse par defaut
int port = 8002; // Port par defaut
int sink_port = 8000;
std::vector<std::string> nodeList;
std::vector<std::string> dataList;
std::vector<std::string> dataLost;//pour les données non stocker

std::vector<std::string> receivedConfig;
std::vector<std::string> nodeswithdata;
std::string dataDirectory = "DataCapteur1/";
int numeroData = 1;
int low_battery_status = 0 ;
int max_data_size = 10;
int stop = 0;
int file_lost = 0;
int file_sent = 0;
int file_generated = 0;
int battery_level = 1000;
bool test_battery_600 = false;
bool test_battery_300 = false;
bool test_battery_50 = false;
double average_lost = 0 ;
double average_lost_binding = 0;


std::mutex nodeListMutex, dataListMutex, nodeswithdataMutex, dataLostMutex, receivedConfigMutex;

/* A server instance answering client commands */
void* serverFunc(void *s) {
	
	//server_params params = *((server_params*)(&s));
	server_params* params = (server_params*) s;
	int socket = params->sock;
	std::string ip_client(params->ip_addr);
	
	std::cout << "[SERVER] Recieving message from ip : " << ip_client.c_str() << std::endl;
	
	//int socket = *((int*)(&s));
	char buf[BUFFERMAX];

	// Reading command
	sock_receive(socket, buf, BUFFERMAX);

	std::cout << "[SERVER] Receiving message: " << buf << std::endl;

	
	if (message_is(buf, EVT_GET_DATA)) {
		dataListMutex.lock();
		for (std::string str : dataList) {
			if(battery_level >= 20){
				battery_level -= 20;
				file_sent ++;
				sock_send(socket, str.c_str());
				std::cout <<"transmission data -20  : "<< battery_level <<std::endl;
				std::cout <<"Number of files transmited  : "<< file_sent <<std::endl;
			}
			else{
				low_battery_status = 1;
			}
		}
		
		
		//EFFACER LES DONNEES EXISTANTES
		if(low_battery_status == 0){
			sock_send(socket, EVT_GET_DATA_END); // notify the end of sent for client
			for (std::string dataname : dataList) {
			std::string filename;
			filename = dataDirectory + "/" + dataname;
			remove(filename.c_str());
			}	
			dataList.clear();
			stop = 0;
			dataListMutex.unlock();
		}
	}
	
	if(message_is(buf, SEND_ECO_CONFIG)){
		
		if(battery_level >= 30){
	
			battery_level -= 30;
			std::cout <<"[CLIENT_CAPTEUR]file reception -30 : "<< battery_level <<std::endl;
			sock_receive(socket, buf, BUFFERMAX);
		 	receivedConfigMutex.lock();
		 	std::string newConfig(buf);
			FILE* datafile;
			std::string filename(dataDirectory + "/" + newConfig);
			datafile = fopen(filename.c_str(), "w");
			fclose(datafile);
			receivedConfig.push_back(newConfig);
			receivedConfigMutex.unlock();
	 	}
		else{
			low_battery_status = 1;
		}
		 
		}
		
		

	
	if(message_is(buf, SEND_MIN_CONFIG)){
			
			/* Lecture du message du client */
		if(battery_level >= 30){
			battery_level -= 30;
			std::cout <<"[CLIENT_CAPTEUR]file reception -30 : "<< battery_level <<std::endl;
			sock_receive(socket, buf, BUFFERMAX);
		 	receivedConfigMutex.lock();
		 	std::string newConfig(buf);
			FILE* datafile;
			std::string filename(dataDirectory + "/" + newConfig);
			datafile = fopen(filename.c_str(), "w");
			fclose(datafile);
			receivedConfig.push_back(newConfig);
			receivedConfigMutex.unlock();
		}
		 else{
			low_battery_status = 1;
		}
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
	start_server(srv_sock, ip_addr_df.c_str(), port);

	while(1){
			
		//server_params* params;
		server_params* params = (server_params*)malloc(sizeof(server_params));
		char buf[BUFFERMAX];
		
		/* Attendre les requêtes de connexion */
		
		
		int sock_effective = wait_connection_adr(srv_sock, buf);
		
		//memset(params,0,sizeof(server_params));
		params->sock = sock_effective;
		strncpy(params->ip_addr, buf, strlen(buf));

		pthread_t thrd;
		if (pthread_create(&thrd, NULL, serverFunc, (void *) params) != 0) {
			std::cerr << "[SERVER] Error while creating a new thread" << std::endl;
			pthread_exit(NULL);
		}
	}
}

/* Mise a jour de la liste des nodes */
void* notifyDataAvailable(void *i) {
	char* ip_serveur = *((char**)(&i));
	char buf[BUFFERMAX];  /* buffer pour les données reçues*/

	/* Création de la socket de communication */
	int clt_sock = create_socket();

	/* demande d'une connexion au serveur */
	open_connection(clt_sock, ip_serveur, sink_port);
	
	std::string delimiter(".");
	std::string message = DATA_AVAILABLE + delimiter + ip_addr_df;
	/* envoi du message au le serveur */
	sock_send(clt_sock, message.c_str());
	battery_level -= 10;
	std::cout <<"notification -10 : "<< battery_level <<std::endl;
	
	

	/* Lecture du message du client */
	while(true) {
		sock_receive(clt_sock, buf, BUFFERMAX);

		if (message_is(buf, EVT_DATA_AVAILABLE_ACK)) { break; }
	}

	/* fermeture de la socket */
	close_connection(clt_sock);
}


void* seek_eco_config(void *i) {

	char* ip_serveur = *((char**)(&i));
	char buf[BUFFERMAX];  /* buffer pour les données reçues*/

	/* Création de la socket de communication */
	int clt_sock = create_socket();

	/* demande d'une connexion au serveur */
	open_connection(clt_sock, ip_serveur, sink_port);
	
	std::string delimiter(".");
	std::string message = SEEK_ECO_CONFIG + delimiter + ip_addr_df;
	/* envoi du message au le serveur */
	sock_send(clt_sock, message.c_str());
	
	
	//Lecture du message du Server(reponse): 
	while(true) {
		sock_receive(clt_sock, buf, BUFFERMAX);

		if (message_is(buf, SEEK_ECO_CONFIG_ACK)) { break; }
	}
	
	

	/* fermeture de la socket */
	close_connection(clt_sock);
}


void* seek_min_config(void *i) {
	char* ip_serveur = *((char**)(&i));
	char buf[BUFFERMAX];  /* buffer pour les données reçues*/


	
	/* Création de la socket de communication */
	int clt_sock = create_socket();

	/* demande d'une connexion au serveur */
	open_connection(clt_sock, ip_serveur, sink_port);
	
	std::string delimiter(".");
	std::string message = SEEK_MIN_CONFIG + delimiter + ip_addr_df;
	/* envoi du message au le serveur */
	sock_send(clt_sock, message.c_str());
	
	//Lecture du message du Server(reponse): 
	while(true) {
		sock_receive(clt_sock, buf, BUFFERMAX);

		if (message_is(buf, SEEK_MIN_CONFIG_ACK)) { break; }
	}
	
	
	/* fermeture de la socket */
	close_connection(clt_sock);
}

/* Client principal */
void* clientFunc(void *n) {
		
	while(1) {
		
		//std::cout << "[CLIENT] Sensing data ..." << std::endl;

		//créer la prochaine data
		FILE* datafile;
		std::string num = std::to_string(numeroData);
		std::string filename;
		if(stop == 0){
			
			if(battery_level >= 50){
				dataListMutex.lock();
				battery_level -= 50;
				std::cout <<"file creation -50 : "<< battery_level <<std::endl;
				filename = dataDirectory + "/" + ip_addr_df + "_data_" + num;
			    	datafile = fopen(filename.c_str(), "w");
				fclose(datafile);
				std::cout << "[CLIENT] Adding data to dataList" << filename.c_str() << std::endl;
				dataList.push_back(ip_addr_df + "_data_" + num);
				file_generated++;
				std::cout << "[CLIENT] number of data generated " << file_generated << std::endl;
				numeroData++;
				dataListMutex.unlock();
			}
			else{
				low_battery_status = 1;
				
			}
		
		}
		else if(stop == 1 && low_battery_status == 0){
		
		filename = dataDirectory + "/" + ip_addr_df + "_data_Lost" + num ;
		//nodeswithdataMutex.lock();
		dataLostMutex.lock();
		std::cout << "[CLIENT] Adding data perdu " << filename.c_str() << "to list of dataLost"<< std::endl;
		dataLost.push_back(ip_addr_df + "_data_Lost" + num);
		file_lost++;
		file_generated++;
		std::cout << "[CLIENT] number of data losted " << file_lost << std::endl;
				numeroData++;
		numeroData++;
		
		dataLostMutex.unlock();
		}
		

		
		std::cout << "[CLIENT_Sensor] List of current data in dataList(capteur1) " << std::endl;	
		for (std::string dataname : dataList) {
		std::cout << "[CLIENT_Sensor] " << dataname << std::endl;	
		}
		std::cout << "[CLIENT_Sensor] List of current data in dataListPerdu(capteur1) " << std::endl;	
		for (std::string dataname : dataLost) {
		std::cout << "[CLIENT_Sensor] " << dataname << std::endl;	
		}
		
		if(dataList.size() ==  max_data_size - 5)
		{ 
			pthread_t traitement;
			if(pthread_create(&traitement, NULL, notifyDataAvailable, (void *) nodeList.at(0).c_str()) != 0) {
			std::cerr << "[CLIENT] Error while creating a new thread: client sensor data update" << std::endl;
			pthread_exit(NULL);
		}
		if(dataList.size() >= max_data_size && stop == 0){
			std::cerr << "##################################################################################################" << std::endl;
			stop = 1;
		}
		
		}
		
		//Check battery 600 and seeking eco_config
		if(battery_level <= 600 && test_battery_600 == false)
		{ test_battery_600 = true;
		
			pthread_t traitement;
			if(pthread_create(&traitement, NULL, seek_eco_config, (void *) nodeList.at(0).c_str()) != 0) {
			std::cerr << "[CLIENT] Error while creating a new thread: client sensor data update" << std::endl;
			pthread_exit(NULL);
		}
		
		}
		//Check battery 300 and seeking min_config
		if(battery_level <= 300 && test_battery_300 == false)
		{  test_battery_300 = true;
			pthread_t traitement;
			if(pthread_create(&traitement, NULL, seek_min_config, (void *) nodeList.at(0).c_str()) != 0) {
			std::cerr << "[CLIENT] Error while creating a new thread: client sensor data update" << std::endl;
			pthread_exit(NULL);
		}
		

		}
		
		
	
		
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
	//std::cout << "Adding data to list" << std::endl;

	//DIR           *dir = opendir(dataDirectory.c_str());
	//struct dirent *pdir;
	//while ((pdir = readdir(dir))) {
		//if(std::strlen(pdir->d_name) < 5) { continue; }
		//dataList.push_back(pdir->d_name);
		//std::cout << "  Added " << pdir->d_name << " to data list." << std::endl;
	//}


	// threads
	
	pthread_t client,listen;

	if (pthread_create(&client, NULL, clientFunc, NULL) != 0) {
		std::cerr << "[MAIN] Error while creating a new thread: client" << std::endl;
		pthread_exit(NULL);
	}

	if (pthread_create(&listen, NULL, listenServer, NULL) != 0) {
		std::cerr << "[MAIN] Error while creating a new thread: server" << std::endl;
		pthread_exit(NULL);
	}
	while(true) 
		{ if(battery_level < 50 && test_battery_50 == false){
				//Delete files from database of captor and add them to number of data lost
				test_battery_50 = true;
				dataListMutex.lock();
				for (std::string dataname : dataList) {
					std::string filename;
					filename = dataDirectory + dataname;
					std::cout << "[CLIENT] Delete file" << filename << std::endl;
					remove(filename.c_str());
					file_lost++;
				}
				dataListMutex.unlock();	
				dataList.clear();
						
		}
		
		if(low_battery_status == 1) {

			//calculate average of lost of captor-collector binding
			average_lost_binding = ((double)(file_generated-file_sent)/file_generated) * 100;
			//calculate average of internal lost of captor
			average_lost =((double)file_lost/file_generated)*100;
			std::cout << "[Capteur] Power off, Battery level incefficent for the recommended operation , Good by"  << std::endl;
			std::cout << "[Capteur] ############################## Statistics ##############################"  << std::endl;
			std::cout << "[Capteur] ## Number of data generated: "<< file_generated  << std::endl;
			std::cout << "[Capteur] ## Number of data sent: "<< file_sent  << std::endl;
			std::cout << "[Capteur] ## Number of data lost: "<< file_lost << std::endl;
	
			std::cout << "[Capteur] ## the average of data lost of captor-collector binding is : "<<  average_lost_binding << "%" << std::endl;
			std::cout << "[Capteur] ## the internal average of data lost of captor : "<< average_lost << "%" << std::endl;
			break; } // Don't stop, we need to let thread run
		}
		
		

	return EXIT_SUCCESS;
}
