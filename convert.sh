#!/bin/bash

find "$1" -type f | while read FFN # 'dir' should be changed...
do
    encoding=$(uchardet "$FFN")
    echo "$FFN: $encoding"
    enc=`echo $encoding | sed 's#^x-mac-#mac#'`
    set +x
    recode $enc..UTF-8 "$FFN"
done
