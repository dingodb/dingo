#!/bin/bash

export dstUser=root
export dstPwd=123123
export dstHost=172.30.14.203
export dstPort=3307
export dstPath=$srcPath/$1

mysql -u$dstUser -p$dstPwd -h $dstHost -P$dstPort --ssl-mode=disabled -e "create database if not exists $1"
mysql -u$dstUser -p$dstPwd -h $dstHost -P$dstPort $1 --ssl-mode=disabled < $dstPath/create.sql
for i in `cat $dstPath/tables.sql|grep -v Tables_in`
do
        if test -f $dstPath/$i.data; then
          echo "$i import"
          mysql -u$dstUser -p$dstPwd -h $dstHost -P$dstPort $1 --ssl-mode=disabled -e "load data infile '$dstPath/$i.data' into table $i"
        fi
done

echo "import $1 data complete"
