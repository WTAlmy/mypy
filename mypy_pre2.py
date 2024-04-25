import os
import time
import math
from mypy.build import _build_fp, load_graph, get_graph_nodes, parallel_topological_sort, build_parallel_graph
from mypy.main import process_options

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

def process_layer(layer, node_sccs, graph):
    #for k in graph:
    #    print(graph[k].path)
    
    paths = layer
    
    sizes = []
    for f in paths:
        sizes.append(os.path.getsize(f))
    
    num_procs = min(16, len(paths))
    target_size = math.ceil(sum(sizes)/num_procs)
        
    commands = []
    for _ in range(num_procs):
        commands.append([["mypy"], 0])
    
    append_idx = 0
    for i in range(len(paths)):
        if commands[append_idx][1] >= target_size:
            append_idx += 1
        commands[append_idx][0].append(paths[i])
        commands[append_idx][1] += sizes[i]
        
    submit_commands = [cmd[0] for cmd in commands if len(cmd[0]) > 1]
    #pprint(submit_commands)
    
    results = execute_commands(submit_commands)
    return results

if __name__ == '__main__':
    start = time.time()
    
    res = list_python_files(base_path)
    
    oh_start = time.time()
    
    sources, options = process_options(res)
    print(len(sources))
    modules = set()
    for src in sources:
        modules.add(src.module)
    
    temp_mgr = _build_fp(sources, options, None, None, None, None, None, [])
    graph = load_graph(sources, temp_mgr)
    #print(graph.keys())
    nodes = get_graph_nodes(graph)
    topograph, source_sccs, node_sccs = build_parallel_graph(nodes)
    layers = parallel_topological_sort(topograph)
    
    oh_end = time.time()
    print(f"OVERHEAD {oh_end - oh_start}")
    
    proc_sets = []
    
    curr_inters = set()
    result_list = []
    for i in range(len(layers)):
        curr_layer = set(layers[i])
        
        curr_layer_set = set()
        for node in curr_layer:
            if node in node_sccs:
                for mod in node_sccs[node]:
                    curr_layer_set.add(mod)
                
        inters = curr_layer_set.intersection(modules)
        for item in inters:
            curr_inters.add(item)
        
        if len(curr_inters) >= 16:
            print(curr_inters)
            proc_start_time = time.time()
            curr_nodes_to_process = [graph[node].path for node in curr_inters]
            proc_sets.append(curr_nodes_to_process)
            result_list.append(process_layer([graph[node].path for node in curr_inters], node_sccs, graph))
            curr_inters.clear()
            proc_end_time = time.time()
            print(f"ELAPSED {proc_end_time - proc_start_time}")
            
    if len(curr_inters) > 0:
        print(curr_inters)
        proc_start_time = time.time()
        curr_nodes_to_process = [graph[node].path for node in curr_inters]
        proc_sets.append(curr_nodes_to_process)
        result_list.append(process_layer(curr_nodes_to_process, node_sccs, graph))
        curr_inters.clear()
        proc_end_time = time.time()
        print(f"ELAPSED {proc_end_time - proc_start_time}")
    
    end = time.time()
    
    
    #for r in result_list:
    #    for item in r:
    #        print(item)
        #output, error, return_code = r[0], r[1], r[2]
        #print("Output:", output.decode("utf-8"))
        #print("Error:", error.decode("utf-8"))
        #print("Return code:", return_code)
        #print("-" * 40)
        
    print(f"ELAPSED {end - start}")
    
    