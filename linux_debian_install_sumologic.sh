set -e
# /opt/SumoCollector/uninstall -q -console

sdir=$(pwd)
token=$SUMO_TOKEN
clobber=False  # True/False
install_dir="/tmp/sumo"
hostname=$(hostname | tr '[:upper:]' '[:lower:]')
mkdir -p $install_dir
cd $install_dir
wget "https://collectors.sumologic.com/rest/download/linux/64" -O SumoCollector.sh && chmod +x SumoCollector.sh
curl -L "https://raw.githubusercontent.com/jeremybusk/sumologic/master/linux_ubuntu_default_sources.json" -o "$install_dir/sources.json"
./SumoCollector.sh -console -q "-Vclobber=$clobber" "-Vsumo.token_and_url=$token" "-Vcollector.name=${hostname}_events" "-Vsources=$install_dir/"
cd $sdir
