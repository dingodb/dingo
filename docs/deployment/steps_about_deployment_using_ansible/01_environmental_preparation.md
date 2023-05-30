# Environment Preparation

> Requirement
> Operating system：centos8.x

## Libc Requirement
```shell
ls -l /lib64/libc.so.6

 """
  /lib64/libc.so.6 -> libc-2.28.so
 """
```
Determine the main operating IP, and all Ansible deployment operations will be performed on the main operating IP. In this example, the main operating IP is 172.20.3.200.

## Set up passwordless sudo login for non-sudo users

If the password is the same for each machine and the root username and password are used in inventory/hosts, then this step is not necessary.
```shell
vim /etc/sudoers

"""
dingo    ALL=(ALL)       NOPASSWD:ALL
"""
```

## Passwordless login using IP address

If the password is the same for each machine and the root username and password are used in inventory/hosts, then the following steps are not necessary.

1. Generate SSH public and private keys
 ```shell
ssh-keygen -f ~/.ssh/id_rsa -N '' -t rsa -q -b 2048
```
2. Copy the public key to all host nodes, including the local node
```shell
ssh-copy-id root@ip
ssh-copy-id root@ip
ssh-copy-id root@ip

```
3. Test if passwordless login is successful
```shell
ssh root@ip
```

## Directory Structure Planning

| Directory Types                                                                | Path                                       |
|--------------------------------------------------------------------------------|:-------------------------------------------|
| Program installation path                                                      | ${installer_root_path}                     |
| Data path                                                                      | ${installer_root_path}/data                |
| Log path                                                                       | ${installer_root_path}/log                 |
| dingo-store script path                                                        | ${installer_root_path}/dingo-store/scripts |
| dingo-store deployment path <br/>(including separate data and log directories) | ${installer_root_path}/dingo-store/dist    |

## User group planning

| Deployment Method | User name | Gorup name |
|-------------------|-----------|------------|
| ansible deploy    | dingodb   | dingodb    |

## Component list and port usage

| Component        | Version | Port                                                               | Remarks                                                             |
|------------------|---------|--------------------------------------------------------------------|---------------------------------------------------------------------|
| DingoDB          | 0.6.0   | raft:22101/20101  <br/>server:22001/20001  <br/>executor:3307/8765 | Real-time analytics database                                        |
| Prometheus       | 2.14.0  | 19115                                                              | Real-time analytics databaseTime-series database, metric monitoring |
| node_exporter    | 0.18.1  | 19100                                                              | Prometheus component, monitoring node information                   |
| process_exporter | 0.7.10  | 19090                                                              | Prometheus component, monitoring process information                |
| grafana          | 8.3.3   | 3000                                                               | Data visualization platform                                         |

## Ansible install

### Online install
```shell
 #Ensure that Python environment is installed on each machine using Python 3
 #Install Ansible on the main IP, if it is not already installed.
 
pip3 install pip -U
pip3 install ansible==2.9.27 # Install with fixed version number, non-mandatory

pip3 install pyopenssl -U
```

### Offline machine install
```shell
# Preparation of files:
wget -r -nH http://ip:9000  # Download all required package files with one click

ansible_installer_centos8.sh Place the files separately and proceed with Ansible installation
Place other files in the deployment package dingo-deploy/artifacts directory

#  Installing Ansible requires root user
bash ansible_installer_centos8.sh # This needs to be executed on all three machines to ensure that the path to Python 3 is consistent on each machine

#Note to modify inventory/hosts before deployment
ansible_python_interpreter=/usr/local/miniconda3/bin/python

# Replace all Ansible and Python commands below with full path, for example:
/usr/local/miniconda3/bin/ansible-playbook playbook.yml
```
