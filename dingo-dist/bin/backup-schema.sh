export srcUser=root
export srcPwd=123123
export srcHost=172.30.14.203
export srcPort=3307
export srcSchema=dingo

ROOT="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

rm -rf data
rm -rf data.tar.gz

mkdir data
chmod 777 data

mysql -u$srcUser -p$srcPwd -h $srcHost -P$srcPort $srcSchema --ssl-mode=disabled -e "show tables" >tables.sql
echo "show table list complete"

mysqldump -u$srcUser -p$srcPwd -h $srcHost -P $srcPort --ssl-mode=disabled --no-data $srcSchema >create.sql
echo "export table with data success"

mysql -u$srcUser -p$srcPwd -h $srcHost -P $srcPort --ssl-mode=disabled -e "show startts" >ts.sql
cat ts.sql |grep -v ts >pointerts

export ts=`cat ts.sql|grep -v 'ts'`

for i in `cat tables.sql|grep -v Tables_in`
do
        mysql -u$srcUser -p$srcPwd -h $srcHost -P $srcPort --ssl-mode=disabled $srcSchema -e "select * from $i into outfile '$ROOT/data/$i.data' startts $ts"
	echo "$i export success"
done
echo "dump complete"

tar -zcvf data.tar.gz data

mysql -u$srcUser -p$srcPwd -h $srcHost -P $srcPort --ssl-mode=disabled -e "set global increment_backup=on"
