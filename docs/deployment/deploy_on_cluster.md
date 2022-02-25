# Distributed Cluster Mode

To achieve high concurrency and high throughput, DingoDB uses an elastic distributed deployment mode. In order to simplify the deployment, this project introduces the deployment of DingoDB using [ansible](https://www.ansible.com/).

## Deploy Architecture View

There are many types of roles in the cluster mode of DingoDB, such as coordinator, executor, driver-proxy, and the cluster mode requires at least three machines. A typical physical deployment architecture is as follows:

![Physical Topology about DingoDB](../images/cluster_topology.png)

## Installation Notes

You can follow this guide to install a 3-nodes DingoDB cluster:

[![asciicast](https://asciinema.org/a/4INSgMgv1q7gW5NZrpIGVJWVt.svg)](https://asciinema.org/a/4INSgMgv1q7gW5NZrpIGVJWVt)


For more details about deplement, you can refer to the [dingo-deploy](https://github.com/dingodb/dingo-deploy) project.