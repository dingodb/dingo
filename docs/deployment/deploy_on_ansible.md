## Environment preparation
Operating system：centos8.x

#### System check for libc
```shell
ls -l /lib64/libc.so.6

 """
  /lib64/libc.so.6 -> libc-2.28.so
 """
```
Determine the main operating IP, and all Ansible deployment operations will be performed on the main operating IP. In this example, the main operating IP is 172.20.3.200.

### Set up passwordless sudo login for non-sudo users

If the password is the same for each machine and the root username and password are used in inventory/hosts, then this step is not necessary.
```shell
vim /etc/sudoers

"""
dingo    ALL=(ALL)       NOPASSWD:ALL
"""
```

### Passwordless login using IP address

If the password is the same for each machine and the root username and password are used in inventory/hosts, then the following steps are not necessary.

1. Generate SSH public and private keys
 ```shell
ssh-keygen -f ~/.ssh/id_rsa -N '' -t rsa -q -b 2048
```
2. Copy the public key to all host nodes, including the local node
```shell
ssh-copy-id root@172.20.3.200
ssh-copy-id root@172.20.3.201
ssh-copy-id root@172.20.3.202

```
3. Test if passwordless login is successful
```shell
ssh root@172.20.3.200
```

### Ansible install 
#### Online install

```shell
 #Ensure that Python environment is installed on each machine using Python 3
 #Install Ansible on the main IP, if it is not already installed.
 
pip3 install pip -U
pip3 install ansible==2.9.27 # Install with fixed version number, non-mandatory

pip3 install pyopenssl -U
```

#### Offline machine install
```shell
# Preparation of files:
wget -r -nH http://172.20.3.202:9000  # Download all required package files with one click

ansible_installer_centos8.sh Place the files separately and proceed with Ansible installation
Place other files in the deployment package dingo-deploy/artifacts directory

#  Installing Ansible requires root user
bash ansible_installer_centos8.sh # This needs to be executed on all three machines to ensure that the path to Python 3 is consistent on each machine

#Note to modify inventory/hosts before deployment
ansible_python_interpreter=/usr/local/miniconda3/bin/python

# Replace all Ansible and Python commands below with full path, for example:
/usr/local/miniconda3/bin/ansible-playbook playbook.yml
```

## Configuration instructions
#### Pull git deploy code
```shell
git clone https://github.com/dingodb/dingo-deploy.git

# Offline installation requires manual download of the zip package
```

#### Deployment configuration
**Configure installation options**

配置文件地址： dingo-deploy/group_vars/all/_shared.yml
```shell
# Configuration file address
installer_root_path

# Modify user group
dingo_user
dingo_group
jdk_user
jdk_group  # Currently, the JDK group and Dingo group are configured the same, and initializing MySQL and Java are mandatory installation options

# Modify installation options
install_system: true    # Install system environment, must be enabled for the first installation, set file limits
install_java_sdk: true  # Install JDK environment, enable it during the first installation, required for executing executor and mysql_init
install_dingo: true     # install executor
install_dingo_store: true   # install dingo-store,Including coordinator、store、mysql_init
--The following are the selected configurations for installing monitoring:
install_prometheus: true    # Install Prometheus database
install_node_exporter: true # Install node monitoring
install_process_exporter: true  # Install process monitoring
install_grafana: true  # Install Grafana monitoring interface

# Offline installation users also need to configure install_system
install_system: true
install_system_basicTools: false
install_system_fileLimits: true   #The file descriptor limit must be enabled, and it only needs to be enabled during the first installation

# define the root path of install
installer_root_path: /home/dingo
installer_cache_path: /tmp
delete_cache_after_install: false

# define the global log and data directory
dingo_log_dir: "{{ installer_root_path }}/log"
dingo_data_dir: "{{ installer_root_path }}/data"
dingo_run_dir: "{{ installer_root_path }}/run"

dingo_user: root
dingo_group: root
jdk_user: root
jdk_group: root

#-----------------------------------------------------
# 1. Update System Configuration about OS
#-----------------------------------------------------
install_system: true
install_system_basicTools: true
install_system_fileLimits: true

#-----------------------------------------------------
# 2. Install Java SDK
#-----------------------------------------------------
install_java_sdk: true
jdk_install_path: /opt
jdk_home: "{{ jdk_install_path }}/jdk"

#-----------------------------------------------------
# 3. Install Dingo to Dingo home
#-----------------------------------------------------
install_dingo: true
install_dingo_basic_command: true
install_dingo_update_configuration: true
install_dingo_start_roles: true

#-----------------------------------------------------
# 4.Install Dingo-store to Dingo home
#-----------------------------------------------------
install_dingo_store: true
install_dingo_store_basic_command: true
install_dingo_store_update_configuration: true
install_dingo_store_start_roles: true

#-----------------------------------------------------
# 5. Install Prometheus to Dingo directory
#-----------------------------------------------------
install_prometheus: true
blackbox_exporter_port: 19115
blackbox_exporter_server: "{{ prometheus_server }}"

# Node Exporter
install_node_exporter: true
node_exporter_port: 19100
node_exporter_servers: "{{ groups['node_exporter'] }}"

# Process Exporter
install_process_exporter: true
process_exporter_port: 19256
process_exporter_servers: "{{ groups['process_exporter'] }}"

# Prometheus
prometheus_port: 19090
prometheus_server: "{{ groups['prometheus'][0] }}"
prometheus_url: "http://{{ prometheus_server }}:{{ prometheus_port }}/prometheus"

# Pushgateway
pushgateway_port: 19091
pushgateway_server: "{{ prometheus_server }}"

# Grafana
install_grafana: true
grafana_port: 3000
grafana_server: "{{ groups['grafana'][0] }}"
default_dashboard_uid: "RNezu0fWk"

dingo_tmp_coordinator_list: "{{ groups['coordinator'] }}"
dingo_tmp_executor_list: "{{ groups['executor'] | default(\"\") }}"

# define dingo coordinator http monitor port: 172.20.3.18:8080,172.20.3.19:8080,172.20.3.20:8080
dingo_coordinator_http_monitor_port: 9201
dingo_executor_http_monitor_port: 9201

# ['172.20.31.10:9201','172.20.31.11:9201','172.20.31.12:9201']
dingo_coordinator_http_tmp_list: "{% for item in dingo_tmp_coordinator_list %} '{{item}}:{{ dingo_coordinator_http_monitor_port }}' {% endfor %}"
dingo_coordinator_http_monitor_list: "[ {{ dingo_coordinator_http_tmp_list.split() | join(\",\") | default(\"\") }} ]"

dingo_executor_http_tmp_list: "{% for item in dingo_tmp_executor_list %} '{{item}}:{{ dingo_executor_http_monitor_port }}' {% endfor %}"
dingo_executor_http_monitor_list: "[ {{ dingo_executor_http_tmp_list.split() | join(\",\") | default(\"\") }} ]"
```

#### Download deployment package
```shell
# Download the deployment package configured above to the dingo-deploy/artifacts directory
wget -r -nH http://172.20.3.202:9000   Download all files with one click

# Download the main files, or download package files separately with wget http://172.20.3.202:9000/pack_name
dingo-store:dingo-store.tar.gz is a POC version, and you need to switch to the old version of Ansible deployment

Monitoring metrics：  
wget http://172.20.3.202:9000/blackbox_exporter-0.16.0.linux-amd64.tar.gz  
wget http://172.20.3.202:9000/grafana-8.3.3.linux-amd64.tar.gz  
wget http://172.20.3.202:9000/node_exporter-0.18.1.linux-amd64.tar.gz  
wget http://172.20.3.202:9000/process-exporter-0.7.10.linux-amd64.tar.gz  
wget http://172.20.3.202:9000/prometheus-2.14.0.linux-amd64.tar.gz  
wget http://172.20.3.202:9000/pushgateway-1.0.0.linux-amd64.tar.gz

jdk:  
wget http://172.20.3.202:9000/jdk-8u171-linux-x64.tar.gz

exector:
wget http://172.20.3.202:9000/dingo.zip

# If you want to get the latest version of dingo-store.tar.gz
Dingo-0.6.0: wget http://172.20.3.14/dingo-store.tar.gz # Get the latest tar package
```

#### Config Hosts
Configuration file address： inventory/hosts
```text
1. Configure the IP address of the [coordinator] node, with one coordinator service started on each node.
2. Configure the IP address of the [store] node, with one node started by default on each node. If multiple nodes are started, set the store_num variable. If you want to deploy the store on different disks, set the disk variable and separate them with spaces.
3. Delete or comment out the [executor], [driver], and [web] configurations for Dingo-0.5.0.
4. Configure the IP address of the monitoring tools:
   [prometheus]: Monitor the database, configure a single node.
   [grafana]: Monitor the interface, configure a single node.
   [node_exporter]: Monitor the node, system performance, deploy it for each node. 
   [process_exporter]: Monitor the process, process performance, deploy it for each node.
```
```shell
[all:vars]
#ansible_connection=ssh
#ansible_ssh_user=root
#ansible_ssh_pass=datacanvas@123
ansible_python_interpreter=/usr/bin/python3

[coordinator]
172.20.3.201
172.20.3.200
172.20.3.202

[store]
172.20.3.201 store_num=2 disk="/home/sd1 /home/sd2"
172.20.3.200
172.20.3.202

[executor]
172.20.3.201

#[web]
#172.20.3.201

[executor_nodes:children]
executor
web

[prometheus]
172.20.3.201

[grafana]
172.20.3.201

[all_nodes:children]
coordinator
store

[node_exporter]
172.20.3.201
172.20.3.200
172.20.3.202

[process_exporter]
172.20.3.201
172.20.3.200
172.20.3.202
```

## Ansible deployment and usage
### Deploy
```shell
cd dingo-deploy
ansible-playbook playbook.yml

# Offline user
cd dingo-deploy
/usr/local/miniconda3/bin/ansible-playbook playbook.yml
```

### ansible usage
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



## View the cluster status
```shell
# Enter the directory configured in installer_root_path
# View the coordinator status # Modify the following IP address to the IP address of your host machine cd dingo-store/build/bin
./dingodb_client_coordinator --method=GetCoordinatorMap

# Output result
WARNING: Logging before InitGoogleLogging() is written to STDERR
E20230517 11:29:53.836163 1828522 coordinator_client.cc:320] [main] coordinator url is empty, try to use file://./coor_list
I20230517 11:29:53.850924 1828526 naming_service_thread.cpp:203] brpc::policy::FileNamingService("./coor_list"): added 3
I20230517 11:29:53.851150 1828522 coordinator_interaction.cc:64] [InitByNameService] Init channel by service_name file://./coor_list service_type=0
I20230517 11:29:53.851306 1828522 coordinator_interaction.cc:64] [InitByNameService] Init channel by service_name file://./coor_list service_type=1
I20230517 11:29:53.882277 1828529 coordinator_interaction.h:208] [SendRequestByService] name_service_channel_ connect with meta server finished. response errcode: 0, leader_addr: 172.20.3.201:22001
I20230517 11:29:53.882387 1828529 coordinator_client_function_coor.cc:496] [SendGetCoordinatorMap] SendRequest status=OK
I20230517 11:29:53.882423 1828529 coordinator_client_function_coor.cc:497] [SendGetCoordinatorMap] leader_location {
host: "172.20.3.202"
port: 22001
}
auto_increment_leader_location {
host: "172.20.3.202"
port: 22001
}


#  View store status ./dingodb_client_coordinator --method=GetStoreMap

# Output result
WARNING: Logging before InitGoogleLogging() is written to STDERR
E20230517 11:30:12.944561 1828534 coordinator_client.cc:320] [main] coordinator url is empty, try to use file://./coor_list
I20230517 11:30:12.958177 1828539 naming_service_thread.cpp:203] brpc::policy::FileNamingService("./coor_list"): added 3
I20230517 11:30:12.958379 1828534 coordinator_interaction.cc:64] [InitByNameService] Init channel by service_name file://./coor_list service_type=0
I20230517 11:30:12.958513 1828534 coordinator_interaction.cc:64] [InitByNameService] Init channel by service_name file://./coor_list service_type=1
I20230517 11:30:12.968495 1828539 coordinator_interaction.h:208] [SendRequestByService] name_service_channel_ connect with meta server finished. response errcode: 0, leader_addr: 172.20.3.202:22001
I20230517 11:30:12.968590 1828539 coordinator_client_function_coor.cc:457] [SendGetStoreMap] SendRequest status=OK
I20230517 11:30:12.968619 1828539 coordinator_client_function_coor.cc:459] [SendGetStoreMap] epoch: 1003
storemap {
epoch: 1003
stores {
id: 1201
state: STORE_NORMAL
server_location {
host: "172.20.3.202"
port: 20001
}
raft_location {
host: "172.20.3.202"
port: 20101
}
keyring: "TO_BE_CONTINUED"
create_timestamp: 1683768684801
last_seen_timestamp: 1684294210333
}
stores {
id: 1101
state: STORE_NORMAL
server_location {
host: "172.20.3.200"
port: 20001
}
raft_location {
host: "172.20.3.200"
port: 20101
}
keyring: "TO_BE_CONTINUED"
create_timestamp: 1683768687632
last_seen_timestamp: 1684294210202
}
stores {
id: 1001
state: STORE_NORMAL
server_location {
host: "172.20.3.201"
port: 20001
}
raft_location {
host: "172.20.3.201"
port: 20101
}
keyring: "TO_BE_CONTINUED"
create_timestamp: 1683768692877
last_seen_timestamp: 1684294207689
}
}
I20230517 11:30:12.970433 1828539 coordinator_client_function_coor.cc:467] [SendGetStoreMap] store_id=1201 state=1 in_state=0 create_timestamp=1683768684801 last_seen_timestamp=1684294210333
I20230517 11:30:12.970482 1828539 coordinator_client_function_coor.cc:467] [SendGetStoreMap] store_id=1101 state=1 in_state=0 create_timestamp=1683768687632 last_seen_timestamp=1684294210202
I20230517 11:30:12.970510 1828539 coordinator_client_function_coor.cc:467] [SendGetStoreMap] store_id=1001 state=1 in_state=0 create_timestamp=1683768692877 last_seen_timestamp=1684294207689
I20230517 11:30:12.970541 1828539 coordinator_client_function_coor.cc:474] [SendGetStoreMap] DINGODB_HAVE_STORE_AVAILABLE, store_count=3

# Create table
../dingodb_client_coordinator --method=CreateTable  --name=test1

# Output result
WARNING: Logging before InitGoogleLogging() is written to STDERR
E20230517 11:34:04.153780 1828620 coordinator_client.cc:320] [main] coordinator url is empty, try to use file://./coor_list
I20230517 11:34:04.167536 1828630 naming_service_thread.cpp:203] brpc::policy::FileNamingService("./coor_list"): added 3
I20230517 11:34:04.167704 1828620 coordinator_interaction.cc:64] [InitByNameService] Init channel by service_name file://./coor_list service_type=0
I20230517 11:34:04.167815 1828620 coordinator_interaction.cc:64] [InitByNameService] Init channel by service_name file://./coor_list service_type=1
W20230517 11:34:04.174784 1828630 coordinator_interaction.h:199] [SendRequestByService] name_service_channel_ connect with meta server success by service name, connected to: 172.20.3.201:22001 found new leader: 172.20.3.202:22001
I20230517 11:34:04.202661 1828628 coordinator_client_function_meta.cc:314] [SendCreateTable] SendRequest status=OK
I20230517 11:34:04.202737 1828628 coordinator_client_function_meta.cc:315] [SendCreateTable] table_id {
entity_type: ENTITY_TYPE_TABLE
parent_entity_id: 2
entity_id: 66006
}

# View all table
./dingodb_client_coordinator --method=GetTables

# View single table
./dingodb_client_coordinator --method=GetTable  --id=66006
./dingodb_client_coordinator --method=GetTableRange  --id=66006
./dingodb_client_coordinator --method=GetTableByName  --name=test1

# Delete table
./dingodb_client_coordinator --method=DropTable  --name=test1
```
