#!/bin/sh
#TODO "修改路径"
#flag=`tail -n 1 /data/ai-news-faiss-index/logs/biz/ai-rec-faiss-index.log`
flag="ready_start_update_index_haha"
tmp="start_update_index"
now=`date -d "now" +"%Y-%m-%d %H:%M:%S"`
echo "$now"
echo "$flag"
# local ip
local_ip=$(/sbin/ifconfig -a|grep inet|grep -v 127.0.0.1|grep -v inet6|awk '{print $2}'|tr -d "addr:")
echo $local_ip
if [[ $flag == *$tmp* ]]; then
        echo "ok"
        /usr/bin/curl http://${local_ip}:8000/exit_index/
        echo "exit"
        /home/app/anaconda3/bin/python  /home/app/ttian/flask_http.py &
        echo "restart"
fi
