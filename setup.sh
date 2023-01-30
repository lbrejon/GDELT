#!/bin/bash
#!/bin/sh
#include <libgen.h>


# Install pip first
sudo apt-get install python3-pip

# Install virtualenv using pip3
sudo pip3 install virtualenv 

# Create a virtual environment
virtualenv venv 

# Active your virtual environment
source venv/bin/activate

# Install requirements packages
pip install -r requirements.txt




