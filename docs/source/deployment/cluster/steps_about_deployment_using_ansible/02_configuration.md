# Update Configuration about Ansible playbook

## Pull git deploy code
```shell
git clone https://github.com/dingodb/dingo-deploy.git

# Offline installation requires manual download of the zip package
```

## Deployment configuration

**Configure installation options**

Configuration file address: dingo-deploy/group_vars/all/_shared.yml
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

## Download Package

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

jdk: wget http://172.20.3.202:9000/jdk-8u171-linux-x64.tar.gz

exector:
wget http://172.20.3.202:9000/dingo.zip

# If you want to get the latest version of dingo-store.tar.gz
Dingo-0.6.0: wget http://172.20.3.14/dingo-store.tar.gz # Get the latest tar package
```

## Config Hosts
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
