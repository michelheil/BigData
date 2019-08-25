#!/usr/bin/env bash
##############################################
### Variables
##############################################
RED='\033[0;31m'
YELLOW='\033[0;33m'
GREEN='\033[0;32m'
NC='\033[0m' # No Color

APP="hdp"
URL="https://downloads-hortonworks.akamaized.net/sandbox-hdp-3.0.1/HDP_3.0.1_docker-deploy-scripts_18120587fc7fb.zip"
INSTFILE=${URL##*/}

# in order to export JAVA_HOME
source /etc/environment

##############################################
### Link
##############################################
# https://de.hortonworks.com/tutorial/sandbox-deployment-and-install-guide/section/3/

##############################################
### Script start
##############################################
if [ "$EUID" -ne 0 ]
  then echo -e "${RED}Please run as root using sudo!${NC}"
  exit
fi

echo -e "${GREEN}*********************************************************************"
echo "* Setup and start $APP"
echo -e "*********************************************************************${NC}"
# Add host if not exist
if ! grep -q "sandbox-hdp.hortonworks.com" /etc/hosts; then 
  sudo sed -i '/127.0.0.1*/c\127.0.0.1  localhost sandbox-hdp.hortonworks.com sandbox-hdf.hortonworks.com/' /etc/hosts
  echo -e "${GREEN}Added 127.0.0.1  localhost sandbox-hdp.hortonworks.com sandbox-hdf.hortonworks.com to /etc/hosts $APP ${NC}"
fi

# Check if folder exists
if [ ! -d "$HOME/$APP" ]; 
    then
        echo -e "${YELLOW}Directory $HOME/$APP does not exist! Will create folder, download and unpack $APP ${NC}"
        mkdir $HOME/$APP && cd $HOME/$APP
        wget "${URL}"
        unzip $INSTFILE
        echo -e "${GREEN}Successfully downloaded and unpacked $APP ${NC}"
    else
        echo -e "${GREEN}Directory $HOME/$APP already exists ${NC}"
fi

cd $HOME/$APP

bash docker-deploy-hdp30.sh
if [ $? -eq 0 ]; then
  echo -e "${GREEN}hdp docker successfully started! Please navigate to Sandbox Welcome Page  http://sandbox-hdp.hortonworks.com:1080/${NC}"
else
  echo -e "${RED}error while starting hdp docker. PLease read error message ... ${NC}"
fi

