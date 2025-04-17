#!/bin/bash

export srcPath='/root/gjn/tmp/dingo/dingo-dist/bin/export_data/2024-10-14--17-00-19'

export mysql=mysql
export information_schema=information_schema
export meta=meta

for i in `cat $srcPath/database.list`
do
          if [ $i == 'information_schema' ] ; then
                echo "skip schema:$i"
                continue
          fi
          echo "import $i start"
            bash import-schema.sh $i
          echo "import $i end"
done
