#!/bin/bash
#!/bin/sh
#include <libgen.h>


# ssh lbrejon-21@ssh.enst.fr
# ssh ubuntu@137.194.211.146

# ssh tp-hadoop-43
# ssh tp-hadoop-54
# ssh tp-hadoop-55
# ssh tp-hadoop-30 

# mkdir /tmp/lbrejon-21;cd /tmp/lbrejon-21;ls
# cd /tmp/lbrejon-21;git clone https://github.com/lbrejon/GDELT.git;ls
# cd /tmp/lbrejon-21/GDELT/;chmod u+x setup.sh;./setup.sh
# cd /tmp/lbrejon-21/GDELT/;source venv/bin/activate;python3 src/functions/upload_data.py -n_rows 100





TIME=3


# Specify parameters
login="lbrejon-21"
remoteFolderLogin="/home/ubuntu/$login/" 
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
    # computers=("tp-hadoop-43.enst.fr" "tp-hadoop-54.enst.fr" "tp-hadoop-55.enst.fr" "tp-hadoop-30.enst.fr")

    echo "Ready to execute '${files[0]}' file on remote computers: ${computers}."

    # Run commands
    for c in ${computers[@]}; do
        # command0=("ssh" "-t" "$bridge" "ssh" "-t" "$bridge_ubuntu" "ssh" "-t" "$c" "lsof -ti | xargs kill -9") # listing and killing process
        command1=("ssh" "-t" "$bridge" "ssh" "-t" "$bridge_ubuntu" "ssh" "-t" "$c" "rm -rf $remoteFolderLogin")
        command2=("ssh" "-t" "$bridge" "ssh" "-t" "$bridge_ubuntu" "ssh" "-t" "$c" "mkdir -p $remoteFolderLogin")

        # command2=("ssh" "-t" "$bridge" "ssh" "-t" "$bridge_ubuntu" "ssh" "-t" "$c" "cd $remoteFolderLogin;touch aaa") # copy server.py
        command2=("ssh" "-t" "$bridge" "ssh" "-t" "$bridge_ubuntu" "ssh" "-t" "$c" "cd $remoteFolderLogin;git clone ${github_repository}") # clone github repository
        # command3=("ssh" "-t" "$bridge" "ssh" "-t" "$bridge_ubuntu" "ssh" "-t" "$c" "cd ${remoteFolderLogin}GDELT/;chmod u+x setup.sh;./setup.sh") # run setup.sh script
        # command4=("ssh" "-t" "$bridge" "ssh" "-t" "$bridge_ubuntu" "ssh" "-t" "$c" "cd ${remoteFolderLogin}GDELT/;source venv/bin/activate;python3 src/functions/upload_data.py") # run upload_data.py script

        # echo
        # echo "CLEANING REPOSITORY..."
        # # echo ${command0[*]}
        # # "${command0[@]}"
        # echo ${command1[*]}
        # "${command1[@]}"


        echo
        echo "CLONING GITHUB REPOSITORY..."
        echo ${command2[*]}
        "${command2[@]}"
        # sleep $TIME


        # echo
        # echo "SETUP VIRTUAL ENVIRONMENT..."
        # echo ${command3[*]}
        # "${command2[@]}"
        # # sleep $TIME


        # echo
        # echo "UPLOADING DATA..."
        # echo ${command4[*]}
        # "${command4[@]}" &
    done
fi

echo ">>>>>>>>>> END ./deploy_server.sh <<<<<<<<<<"