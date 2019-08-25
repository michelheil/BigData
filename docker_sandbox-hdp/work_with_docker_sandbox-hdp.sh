# Oder wenn man das Volume "data" uebergeben moechte, dann das modifizierte sh-Skript ausfuehren
# Vorher noch das Volume erzeugen
docker volume create data

cd ~/hdp
sudo bash docker-deploy-hdp30_withVolumeData.sh




# HDP-Sandbox pausieren
# Wenn Sie die HDP-Sandbox pausieren/herunterfahren möchten, führen Sie die folgenden Befehle aus:

sudo docker stop sandbox-hdp
sudo docker stop sandbox-proxy

# HDP-Sandbox neu starten
# Wenn Sie die HDP-Sandbox neu starten möchten, führen Sie die folgenden Befehle aus:

sudo docker start sandbox-hdp # http://sandbox-hdp.hortonworks.com:1080
sudo docker start sandbox-proxy

# HDP-Sandbox entfernen
# Ein Container ist eine Instanz des Sandbox-Images. Daher müssen Sie Abhängigkeiten des Containers auflösen, bevor Sie ihn entfernen. Führen Sie die folgenden Befehle aus:

docker stop sandbox-hdp
docker stop sandbox-proxy
docker rm sandbox-hdp
docker rm sandbox-proxy

# Wenn Sie das Image der HDP-Sandbox entfernen möchten, führen Sie nach Pausieren und Entfernen der Container folgenden Befehl aus:
sudo docker rmi hortonworks/sandbox-hdp:{release}


# Sandbox Tutorial                   https://hortonworks.com/tutorial/hadoop-tutorial-getting-started-with-hdp/
# Sandbox Docs HDP 3.0.1             https://de.hortonworks.com/tutorial/hortonworks-sandbox-guide/section/1/
# Sandbox Port Forwards HDP 3.0.1    https://de.hortonworks.com/tutorial/hortonworks-sandbox-guide/section/3/


# You can access the sandbox via shell by using Putty (Windows) and the following command:
# klappt leider nicht    ssh root@sandbox.hortonworks.com -p 2222
# also was anderes versuchen: 
ssh root@172.18.0.2 -p 22 # die IP erhalte ich in dem man in in der Web Shell in a Box einfach nur "ifconfig" eingibt. Dann ist es die erste angezeigte "inet" Adresse
# Jetzt bin ich mit meiner Konsole eingeloggt auf der sandbox-hdp als root user

# close ssh connection and return to terminal
cd ~
press Ctrl + d


https://docs.hortonworks.com/HDPDocuments/HDP3/HDP-3.1.0/administration/content/starting_hdp_services.html
Alle Scripte zum starten koennen hier gefunden werden: /usr/hdp/current/


# Shell in a Box
http://sandbox-hdp.hortonworks.com:4200/ # web-basierteKonsole auf der HDP-Sandbox
# Anmeldedaten: root // Tl leet
cd /$HADOOP_INSTALL_HOME/ 








#################################
## Sandbox Virtual Box
#################################
# https://hortonworks.com/tutorial/sandbox-deployment-and-install-guide/section/1/#introduction

# file HDP_3.0.1_virtualbox_181205.ova already downloaded in ~/Downloads


########
# Tutorial Quotes
# The Hortonworks Sandbox is a single node implementation of HDP.


### How to make Geolocation.zip available in HDP Sandbox
# Download Geolocation.zip into ~/Downloads
# sudo -i
# cd /home/michael/Downloads/
# mv Geolocation.zip /var/lib/docker/volumes/data/_data/Geolocation.zip
# unzip Geolocation.zip

# Warten bis HDFS gestartet ist:
docker exec --privileged sandbox-hdp hdfs dfs -mkdir -p /tmp/data/
docker exec --privileged sandbox-hdp hdfs dfs -put /usr/share/volume/data/geolocation.csv /tmp/data/
docker exec --privileged sandbox-hdp hdfs dfs -put /usr/share/volume/data/trucks.csv /tmp/data/
docker exec --privileged sandbox-hdp hdfs dfs -put /usr/share/volume/data/avgmileage.csv /tmp/data/
docker exec --privileged sandbox-hdp hdfs dfs -put /usr/share/volume/data/drivermileage.csv /tmp/data/
docker exec --privileged sandbox-hdp hdfs dfs -put /usr/share/volume/data/truckmileage.csv /tmp/data/
docker exec --privileged sandbox-hdp hdfs dfs -chmod -R 777 /tmp/data/



# Tutorial weitermachen!

Im Zeppelin Notebook in local:Downloads mal schauen, ob ich die Tabelle riskfactor auch direkt als csv Datei speichern kann
https://hortonworks.com/tutorial/hadoop-tutorial-getting-started-with-hdp/section/5/

