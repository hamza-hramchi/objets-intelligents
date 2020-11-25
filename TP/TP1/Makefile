main:
	g++ -W -Wall -pthread -lstdc++fs --std=c++17 net_aux.cpp noeud_collecteur.cpp -o collecteur
	g++ -W -Wall -pthread -lstdc++fs --std=c++17 net_aux.cpp noeud_capteur.cpp -o capteur
#./collecteur -d database (pour lancer le collecteur après avoir créé le répertoire database)
#./capteur -d capteur1 -l 127.0.0.1 (pour lancer le capteur 1 après avoir créé le dossier capteur1)
#possibilité de changer adresse ip (-a IP), port (-p PORT), dossier (-d DOSSIER) et cible (-l IP)


