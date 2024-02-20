#!/bin/bash

SFS=(300 1000 3000)

for SF in "${SFS[@]}"; do
  echo $SF
    for i in $(seq 1 10); do
      echo "Round $SF-$i"
        mkdir -p "sf-$SF"
        ./dbgen -s "$SF" -S "$i" -C 10
        mv *tbl* "sf-$SF/"
        aws s3 cp "sf-$SF/" "s3://neural-science/aws/data/sf-$SF/" --recursive
        rm -rf "sf-$SF"
    done
done