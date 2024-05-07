#!/bin/bash
export dstUser=root
export dstPwd=123123
export dstHost=172.20.3.93
export dstPort=3307
export dstSchema=dingo4
export dstPath=/root/gjn/infile

mysql -u$dstUser -p$dstPwd -h $dstHost -P$dstPort --ssl-mode=disabled -e "create database if not exists $dstSchema"
mysql -u$dstUser -p$dstPwd -h $dstHost -P$dstPort $dstSchema --ssl-mode=disabled <create.sql
for i in `cat tables.sql|grep -v Tables_in`
do
        #if test -f $dstPath/data/$i.data; then
          echo "$i import"
          mysql -u$dstUser -p$dstPwd -h $dstHost -P$dstPort $dstSchema --ssl-mode=disabled -e "load data infile '$dstPath/data/$i.data' into table $i"
        #fi
done

echo "import all data complete"
