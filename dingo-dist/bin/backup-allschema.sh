#!/bin/bash

export srcUser=root
export srcPwd=123123
export srcHost=172.30.14.203
export srcPort=3307

ROOT="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

databases=$(mysql -h $srcHost -P $srcPort -u root -p$srcPwd -e "SHOW DATABASES;" | tail -n +2)
mkdir export_data
chmod 777 export_data
data_dir=export_data/$(date +'%Y-%m-%d--%H-%M-%S')
mkdir $data_dir
chmod 777 $data_dir

echo $data_dir
echo '----------------'
mysql -h $srcHost -P $srcPort -u root -p$srcPwd -e "SHOW DATABASES;" | tail -n +2 >$ROOT/$data_dir/database.list

for srcSchema in $databases; do
    echo "Processing database: $srcSchema"
    mkdir -p $ROOT/$data_dir/$srcSchema
    cd $ROOT/$data_dir/$srcSchema
    chmod 777 $ROOT/$data_dir/$srcSchema -R *

    mysql -u$srcUser -p$srcPwd -h $srcHost -P$srcPort $srcSchema --ssl-mode=disabled -e "show tables" >tables.sql
    echo "show table list complete" 
    mysqldump -u$srcUser -p$srcPwd -h $srcHost -P $srcPort --ssl-mode=disabled --no-data $srcSchema >create.sql
    echo "export table with data success"
    mysql -u$srcUser -p$srcPwd -h $srcHost -P $srcPort --ssl-mode=disabled -e "show startts" >ts.sql
    cat ts.sql |grep -v ts >pointerts
    export ts=`cat ts.sql|grep -v 'ts'`

    for i in `cat tables.sql|grep -v Tables_in`
    do
            mysql -u$srcUser -p$srcPwd -h $srcHost -P $srcPort --ssl-mode=disabled $srcSchema -e "select * from $i into outfile '$ROOT/$data_dir/$srcSchema/$i.data' startts $ts"
            #echo "-------------$ROOT/$data_dir/$srcSchema/$i.data    -------$ts"
            echo "$i export success"
    done
    echo "dump complete"
    cd $ROOT/$data_dir
    tar -zcvf $srcSchema.tar.gz $srcSchema

    mysql -u$srcUser -p$srcPwd -h $srcHost -P $srcPort --ssl-mode=disabled -e "set global increment_backup=on"
    #exit
done
