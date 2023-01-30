#!/bin/bash
#!/bin/sh
#include <libgen.h>


TIME=3


# Specify parameters
login="lbrejon-21"
remoteFolderLogin="/tmp/$login/" 
bridge="$login@ssh.enst.fr"
bridge_ubuntu="ubuntu@137.194.211.146"
github_repository="https://github.com/lbrejon/GDELT.git"

# Program to deploy on remote computers
flag_files=true
# files=("server.py" "utils.py" "../config/params.yaml")
# for file in ${files[@]};do
#     if [ ! -f "$file" ];then
#         echo "ERROR: '$file' file does not exist."
#         flag_files = false
#     else
#         echo "'$file' file exists."
#     fi
# done
# echo

if $flag_files; then
    # Specify computers to use 
    computers=("tp-hadoop-43" "tp-hadoop-54")
    # computers=("tp-hadoop-43.enst.fr" "tp-hadoop-54.enst.fr" "tp-hadoop-45.enst.fr" "tp-hadoop-30.enst.fr")

    echo "Ready to execute '${files[0]}' file on remote computers: ${computers}."

    # Run commands
    for c in ${computers[@]}; do
        # command0=("ssh" "-t" "$bridge" "ssh" "-t" "$bridge_ubuntu" "ssh" "-t" "$c" "lsof -ti | xargs kill -9") # listing and killing process
        command1=("ssh" "-t" "$bridge" "ssh" "-t" "$bridge_ubuntu" "ssh" "-t" "$c" "rm -rf $remoteFolderLogin;mkdir $remoteFolderLogin")
        command2=("ssh" "-t" "$bridge" "ssh" "-t" "$bridge_ubuntu" "ssh" "-t" "$c" "cd $remoteFolderLogin;touch aaa") # copy server.py
        # command2=("ssh" "-t" "$bridge" "ssh" "-t" "$bridge_ubuntu" "ssh" "-t" "$c" "cd $remoteFolderLogin;git clone ${github_repository}") # copy server.py
        command3=("ssh" "-t" "$bridge" "ssh" "-t" "$bridge_ubuntu" "ssh" "-t" "$c" "cd ${remoteFolderLogin}/{GDELT};python3 ${files[0]}") # run server.py script
        
        echo
        echo "CLEANING..."
        echo ${command0[*]}
        "${command0[@]}"
        echo ${command1[*]}
        "${command1[@]}"


        echo
        echo "CLONING REPOSITORY..."
        echo ${command2[*]}
        "${command2[@]}"
        # sleep $TIME
        # echo ${command22[*]}
        # "${command22[@]}"
        # sleep $TIME


        # echo
        # echo "UPLOAD DATA..."
        # echo ${command2[*]}
        # "${command2[@]}" &
    done
fi

echo ">>>>>>>>>> END ./deploy_server.sh <<<<<<<<<<"