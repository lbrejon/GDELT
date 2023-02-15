#!/bin/bash

# Specify workers to use 
computers=("tp-hadoop-11" "tp-hadoop-12" "tp-hadoop-30" "tp-hadoop-31" "tp-hadoop-55" "tp-hadoop-54" "tp-hadoop-43" "tp-hadoop-37" "tp-hadoop-58" "tp-hadoop-59")

# Check that the ssh bridge is working, if not, start it 
# bridge_running=$(ps | grep 'ssh -L' | head -n 1 | awk '{print $4}')

# echo $bridge_running

# if [ "$bridge_running" = "grep" ];then
#     bridge=$(ssh -L localhost:8889:192.168.3.99:8889 -L localhost:8085:192.168.3.99:8085 -L localhost:4041:192.168.3.99:4040 -L localhost:8081:192.168.3.99:8081 ubuntu@137.194.211.146)
#     pid=$(ps | grep 'ssh -L' | head -n 1 | awk '{print $2}')
#     echo 'Bridge is running on the process ${pid}'
#     "${bridge}"
# else 
#    echo 'Bridge already established'
# fi

if [ "$1" = "start" ];then 
    # ====== START CLUSTER =======
    for c in ${computers[@]};do
        echo "Ready to start $c."
        if [ "$c" = "tp-hadoop-31" ];then
            command=("ssh" "-t" "ubuntu@137.194.211.146" "ssh" "-t" "$c" "./spark-3.2.3-bin-hadoop3.2/sbin/start-master.sh") 
            #echo ${command[*]}
            "${command[@]}"
        else 
            command=("ssh" "-t" "ubuntu@137.194.211.146" "ssh" "-t" "$c" "./spark-3.2.3-bin-hadoop3.2/sbin/start-worker.sh spark://192.168.3.99:7078")
            #echo ${command[*]}
            "${command[@]}"
        fi
    done

else 
    # ====== STOP CLUSTER =======
    for c in ${computers[@]};do
        echo "Ready to start $c."
        if [ "$c" = "tp-hadoop-31" ];then 
           command=("ssh" "-t" "ubuntu@137.194.211.146" "ssh" "-t" "$c" "./spark-3.2.3-bin-hadoop3.2/sbin/stop-master.sh") 
            #echo ${command[*]}
            "${command[@]}"
        else 
            command=("ssh" "-t" "ubuntu@137.194.211.146" "ssh" "-t" "$c" "./spark-3.2.3-bin-hadoop3.2/sbin/stop-worker.sh spark://192.168.3.99:7078")
            #echo ${command[*]}
            "${command[@]}"
        fi
    done
fi











