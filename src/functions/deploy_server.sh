#!/bin/bash
#!/bin/sh
#include <libgen.h>


TIME=3


# Specify user parameters
login="lbrejon-21"
remoteFolderLogin="/tmp/$login/" 
bridge="$login@ssh.enst.fr"

# Program to deploy on remote computers
flag_files=true
files=("server.py" "utils.py" "../config/params.yaml")
for file in ${files[@]};do
    if [ ! -f "$file" ];then
        echo "ERROR: '$file' file does not exist."
        flag_files = false
    else
        echo "'$file' file exists."
    fi
done
echo

if $flag_files; then
    # Specify computers to use 
    
    # 1
    # computers=("tp-1d23-16.enst.fr")

    # 2
    # computers=("tp-1d23-16.enst.fr" "tp-1d23-17.enst.fr")

    # 3
    computers=("tp-1d23-16.enst.fr" "tp-1d23-17.enst.fr" "tp-1d23-19.enst.fr")

    # 4
    # computers=("tp-1d23-16.enst.fr" "tp-1d23-17.enst.fr" "tp-1d23-18.enst.fr" "tp-1d23-19.enst.fr")

    # 5
    # computers=("tp-1d23-16.enst.fr" "tp-1d23-17.enst.fr" "tp-1d23-18.enst.fr" "tp-1d23-19.enst.fr" "tp-1d23-22.enst.fr")
    
    # 6
    # computers=("tp-1d23-16.enst.fr" "tp-1d23-17.enst.fr" "tp-1d23-18.enst.fr" "tp-1d23-19.enst.fr" "tp-1d23-22.enst.fr" "tp-1d23-23.enst.fr")
    
    # 10
    # computers=("tp-1d23-16.enst.fr" "tp-1d23-17.enst.fr" "tp-1d23-18.enst.fr" "tp-1d23-19.enst.fr" "tp-1d23-22.enst.fr" "tp-1d23-23.enst.fr" "tp-1d23-14.enst.fr" "tp-1d23-25.enst.fr" "tp-1d23-00.enst.fr" "tp-1d23-02.enst.fr")

    # 12
    # computers=("tp-1d23-16.enst.fr" "tp-1d23-17.enst.fr" "tp-1d23-18.enst.fr" "tp-1d23-19.enst.fr" "tp-1d23-22.enst.fr" "tp-1d23-23.enst.fr" "tp-1d23-14.enst.fr" "tp-1d23-25.enst.fr" "tp-1d23-00.enst.fr" "tp-1d23-02.enst.fr" "tp-1d23-11.enst.fr" "tp-1d23-13.enst.fr")

    echo "Ready to execute '${files[0]}' file on remote computers: ${computers}."

    # Run commands
    for c in ${computers[@]}; do
        command0=("ssh" "-J" "$bridge" "$login@$c" "lsof -ti | xargs kill -9") # listing and killing process
        command1=("ssh" "-J" "$bridge" "$login@$c" "rm -rf $remoteFolderLogin;mkdir $remoteFolderLogin")
        command21=("scp" "-J" "$bridge" "${files[0]}" "$login@$c:$remoteFolderLogin${files[0]}") # copy server.py
        command22=("scp" "-J" "$bridge" "${files[1]}" "$login@$c:$remoteFolderLogin${files[1]}") # copy utils.py
        command23=("scp" "-J" "$bridge" "${files[2]}" "$login@$c:$remoteFolderLogin$(basename ${files[2]})") # copy params.yml
        command3=("ssh" "-J" "$bridge" "$login@$c" "cd $remoteFolderLogin;python3 ${files[0]}") # run server.py script
        
        echo
        echo "CLEANING..."
        echo ${command0[*]}
        "${command0[@]}"
        echo ${command1[*]}
        "${command1[@]}"

        echo
        echo "COPYING FILES..."
        echo ${command21[*]}
        "${command21[@]}"
        sleep $TIME
        echo ${command22[*]}
        "${command22[@]}"
        sleep $TIME
        echo ${command23[*]}
        "${command23[@]}"

        echo
        echo "STARTING SERVER..."
        echo ${command3[*]}
        "${command3[@]}" &
    done
fi

echo ">>>>>>>>>> END ./deploy_server.sh <<<<<<<<<<"