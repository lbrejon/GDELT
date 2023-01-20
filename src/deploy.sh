TIME=3

# Specify user parameters
login="lbrejon-21"
remoteFolderLogin="/tmp/$login/" 
bridge="$login@ssh.enst.fr"
bridge_ubuntu="ubuntu@137.194.211.146"

# Program to deploy on remote computers
flag_files=true
# files=('client.py' 'utils.py' '../config/params.yaml' '../data/wet.paths.gz')
files=('test_deploy.py')

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
    computers=("ubuntu@tp-hadoop-60")
    echo "Ready to execute '${files[0]}' file on remote computers: ${computers}."

    # Run commands
    for computer in ${computers[@]}; do

        # command0=("ssh" "-t" "$bridge" "ssh" "-t" "$bridge_ubuntu" "ssh" "-t" "$computer" "lsof -ti | xargs kill -9") # listing and killing process
       

        # command1=("ssh" "-t" "$bridge" "ssh" "-t" "$bridge_ubuntu" "ssh" "-t" "$computer" "echo 'Connected!' && rm -rf $remoteFolderLogin && echo 'Still connected?'")
        command1=("ssh" "-t" "$bridge" "ssh" "-t" "$bridge_ubuntu" "ssh" "-t" "$computer" "rm -rf $remoteFolderLogin")
        command2=("ssh" "-t" "$bridge" "ssh" "-t" "$bridge_ubuntu" "ssh" "-t" "$computer" "mkdir -p $remoteFolderLogin")
        command3=("scp" "-J" "$bridge" "$bridge_ubuntu" "'${files[0]}'" "$computer:'$remoteFolderLogin${files[0]}'")
        #command3=("ssh" "-t" "ubuntu@137.194.211.146" "ssh" "-t" "$c" "cd $remoteFolderLogin;python ${files[0]}$ext")
        # command21=("scp" "-J" "$bridge" "${files[0]}" "$login@$c:$remoteFolderLogin${files[0]}") # copy server.py



        # echo ${command0[*]}
        # "${command0[@]}"
        # echo

        echo ${command1[*]}
        "${command1[@]}"
        echo

        echo ${command2[*]}
        "${command2[@]}"
        echo

        echo ${command3[*]}
        "${command3[@]}"
        echo

    done
fi
