#!/bin/bash
HOME=/opt/pyscript/canvas-auto-shifter
logger -t canvas-auto-shifter "changing directory to $HOME"
cd $HOME
logger -t canvas-auto-shifter "activating virtual environment at $HOME/venv"
source $HOME/venv/bin/activate
logger -t canvas-auto-shifter "installing requirements at $HOME/requirements.txt"
$HOME/venv/bin/pip install -r requirements.txt
logger -t canvas-auto-shifter "creating logs folder"
mkdir -p  $HOME/logs
logger -t canvas-auto-shifter "running DAP process to synchronize. Args: $@"
$HOME/venv/bin/python $HOME/dap_synchronizer.py $@ > $HOME/logs/dap_synchronizer.log
logger -t canvas-auto-shifter "finished with DAP sync process"
