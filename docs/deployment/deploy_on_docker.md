# Docker-compose Mode

For the convenience of developers, the deployment of DingoDB can also choose docker-compse mode.


## Deploy Architecture View

There are many types of roles in the cluster mode of DingoDB, such as coordinator, executor, driver-proxy, and the cluster mode requires at least three machines. The architecture view can refer [dingo-deploy-architecture](https://dingodb.readthedocs.io/en/latest/deployment/deploy_on_cluster.html).


## Installation Notes

You can follow this guide to install three `Coordinator` and three `Executor` virtual DingoDB cluster:

[![asciicast](https://asciinema.org/a/J2BENeRd6yJoDVDfgfDFk7fao.svg)](https://asciinema.org/a/J2BENeRd6yJoDVDfgfDFk7fao)