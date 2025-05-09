# Deploy On Ansible

## Start to Deploy DingoDB
```shell
cd dingo-deploy
ansible-playbook playbook.yml

# Offline user
cd dingo-deploy
/usr/local/miniconda3/bin/ansible-playbook playbook.yml
```

## Ansible usage

**Command-line usage**
```shell
cd dingo-deploy
ansible store -m ping # Test all IPs in the [store] group, which can be used to ensure that the Ansible connection is available on each machine before installation

ansible store -m shell -a "ps -ef |grep dingodb_server" #  View the dingodb_server process

ansible store -m shell -a "/path/scripts/start-coordinator.sh  start/stop/clean/restart/deploy/cleanstart" --become --become-user "myuser"  # Start the coordinator node with cleanstart, which includes stop, clean, deploy, and start
ansible store -m shell -a "/path/scripts/start-store.sh  start/stop/clean/restart/deploy/cleanstart" --become --become-user "myuser"  # Start store node
```

**Management tool DingoControl**
```shell
# Prerequisite: group_vars/all/_shared.yml and hosts have been configured
# Assign permissions to DingoControl
chmod +x DingoControl

# DiongControl command details
Options:
--help        display this help and exit
deploy        deploy the application
show          show all process for user
stop          stop  server
clean         clean server
start         start server
cleanstart    stop clean deploy  and start server
install       install playbook

# install
# Installation options: playbook installs the default configuration, dingodb installs Dingo (executor) and dingo-store, monitor installs all monitoring tools, and the rest are installed in individual steps
# Example: ./DingoControl install playbook
playbook    playbook.yml
system      install system
jdk         install jdk
dingo       install dingo
dingo-store install dingo-store
dingodb     install dingo and dingo-store
prometheus  install prometheus
grafana     install grafana
node        install node_exporter
process     install process_exporter
monitor     install prometheus grafana node_exporter process_exporter

# deploy
# The deployment module includes the deployment of the coordinator and the deployment of the store, generating the directory structure before starting
# Example: ./DingoControl deploy all
all               stop/clean/deploy store/coordinator
store             stop/clean/deploy store
coordinator       stop/clean/deploy coordinator

# show
# Example: ./DingoControl show process
process          show all process # Display all processes of the deployment user
file             show all file not user # Check if there are directories with non-deployment user permissions under the deployment directory

# stop
# Stop processes, you can stop individual processes or stop all of them
# ./DingoControl stop store
all               stop all process
store             stop store
coordinator       stop coordinator
executor          stop executor
node-exporter     stop node-exporter
process-exporter  stop process-exporter
prometheus        stop prometheus
grafana           stop grafana

# clean
# Clean up files, automatically stop processes. Only 'all' cleans up all files and deletes directories. 'coordinator' and 'store' clean up deployment files and data. The monitoring module cleans up the systemctl service files
# Example: ./DingoControl clean store
all               stop all server and clean all file, if want del completely
dingo-store       stop/clean store/coordinator and clean dingo-store, deprecated
store             stop/clean store
coordinator       stop/clean coordinator
node-exporter     stop/clean node-exporter
process-exporter  stop/clean process-exporter
prometheus        stop/clean prometheus
grafana           stop/clean grafana

# start
# Start the process, and when starting the store, automatically execute mysql_init.sh to initialize the tables
# Example: ./DingoControl start store
all               start all process
store             start store
coordinator       start coordinator
executor          start executor
node-exporter     start node-exporter
process-exporter  start process-exporter
prometheus        start prometheus
grafana           start grafana

# cleanstart
# Clear the data and redeploy and start the coordinator, store, and executor
# Example: ./DingoControl cleanstart all
all   stop-clean-deploy store/coordinator and restart executor
```
