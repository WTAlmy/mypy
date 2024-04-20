import os
import time
import math
from pprint import pprint

base_path = "/Users/ArunAdmin/starlette"

def list_python_files(base_dir):
    python_files = []
    curr_python_files = []
    
    for item in os.listdir(base_dir):
        item_path = os.path.join(base_dir, item)
        
        if os.path.isfile(item_path) and item.endswith('.py'):
            curr_python_files.append(item_path)
        elif os.path.isdir(item_path):
            python_files.extend(list_python_files(item_path))
            
    if len(curr_python_files) > 0:
        python_files.extend(curr_python_files)

    return python_files

import subprocess
import concurrent.futures

def execute_command(command):
    """Function to execute a single command"""
    #print(f"COMMAND: {command}")
    process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    output, error = process.communicate()
    return (output, error, process.returncode)

def execute_commands(paths_list):

    with concurrent.futures.ProcessPoolExecutor(max_workers=len(paths_list)) as executor:
        results = executor.map(execute_command, paths_list)
        
    return results
#print(res, len(res))

if __name__ == '__main__':
    start = time.time()
    res = list_python_files(base_path)
    sizes = []
    for f in res:
        sizes.append(os.path.getsize(f))
    
    num_procs = 8
    target_size = math.ceil(sum(sizes)/num_procs)
    
    print(sum(sizes), target_size)
    
    commands = []
    for _ in range(num_procs):
        commands.append([["mypy"], 0])
    
    append_idx = 0
    for i in range(len(res)):
        if commands[append_idx][1] >= target_size:
            append_idx += 1
        commands[append_idx][0].append(res[i])
        commands[append_idx][1] += sizes[i]
        
    submit_commands = [cmd[0] for cmd in commands if len(cmd[0]) > 1]
    pprint(submit_commands)
    
    results = execute_commands(submit_commands)
    end = time.time()
    
    for command, result in zip(res, results):
        output, error, return_code = result
        print("Command:", command)
        print("Output:", output.decode("utf-8"))
        print("Error:", error.decode("utf-8"))
        print("Return code:", return_code)
        print("-" * 40)
        
    print(f"ELAPSED {end - start}")