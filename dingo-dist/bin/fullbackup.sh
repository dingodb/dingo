ROOT="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

rm -rf data

mkdir data
chmod 777 data

mysql -uroot -p123123 -h $3 -P3307 -e "use $1; show tables" >tables.sql

mysqldump -uroot -p123123 -h $3 -P3307 --no-data dingo >create.sql
mysql -uroot -p123123 -h $3 -P3307 -e "show startts" >ts.sql
cat ts.sql |grep -v ts >pointerts

export ts=`cat ts.sql|grep -v 'ts'`

for i in `cat tables.sql|grep -v Tables_in`
do
        mysql -uroot -p123123 -h $3 -P3307 -e "use $1; select * from $i into outfile '$ROOT/data/$i.data' startts $ts"
done

mysql -uroot -p123123 -h $3 -P3307 -e "create database if not exists $2"
mysql -uroot -p123123 -h $3 -P3307 $2 <create.sql
for i in `cat tables.sql|grep -v Tables_in`
do
        mysql -uroot -p123123 -h $3 -P3307 $2 -e "load data infile '$ROOT/data/$i.data' into table $i"
done

mysql -uroot -p123123 -h $3 -P3307 -e "set global increment_backup=on"
