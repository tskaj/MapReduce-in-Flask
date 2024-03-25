import subprocess
import os
import json
import uuid
from flask import Flask, request

app = Flask(__name__)

# Define constants
DATA_DIR = 'data'
MAPPER_DIR = 'mappers'
REDUCER_DIR = 'reducers'
JOB_STATUS_DIR = 'job_status'
MAP_REDUCE_SCRIPT = 'map_reduce.py'

# Function to spawn a process
def spawn_process(cmd):
    return subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

# Function to initialize the MapReduce cluster
def init_cluster(num_mappers, num_reducers):
    # Create directories for data storage, mappers, reducers, and job status
    os.makedirs(DATA_DIR, exist_ok=True)
    os.makedirs(MAPPER_DIR, exist_ok=True)
    os.makedirs(REDUCER_DIR, exist_ok=True)
    os.makedirs(JOB_STATUS_DIR, exist_ok=True)
    return str(uuid.uuid4())

# Function to run a MapReduce job
def run_mapred(input_data, map_fn, reduce_fn, output_location):
    job_id = str(uuid.uuid4())

    # Write input data to a file
    input_file = os.path.join(DATA_DIR, f'{job_id}_input.txt')
    with open(input_file, 'w') as f:
        f.write(input_data)

    # Write map and reduce functions to files
    map_file = os.path.join(MAPPER_DIR, f'{job_id}_map.py')
    reduce_file = os.path.join(REDUCER_DIR, f'{job_id}_reduce.py')
    with open(map_file, 'w') as f:
        f.write(map_fn)
    with open(reduce_file, 'w') as f:
        f.write(reduce_fn)

    # Execute the MapReduce job using map_reduce.py script
    cmd = f'python {MAP_REDUCE_SCRIPT} {input_file} {map_file} {reduce_file} {output_location}'
    process = spawn_process(cmd)

    # Store job status
    job_status_file = os.path.join(JOB_STATUS_DIR, f'{job_id}_status.json')
    with open(job_status_file, 'w') as f:
        json.dump({'status': 'running'}, f)

    return job_id

# Function to destroy the MapReduce cluster
def destroy_cluster(cluster_id):
    # Terminate all running processes and cleanup directories
    os.system(f'rm -rf {DATA_DIR} {MAPPER_DIR} {REDUCER_DIR} {JOB_STATUS_DIR}')

    return 'Cluster destroyed'

# API endpoint for initializing the cluster
@app.route('/init_cluster', methods=['POST'])
def handle_init_cluster():
    num_mappers = request.form.get('num_mappers')
    num_reducers = request.form.get('num_reducers')
    cluster_id = init_cluster(num_mappers, num_reducers)
    return cluster_id

# API endpoint for running a MapReduce job
@app.route('/run_mapred', methods=['POST'])
def handle_run_mapred():
    input_data = request.form.get('input_data')
    map_fn = request.form.get('map_fn')
    reduce_fn = request.form.get('reduce_fn')
    output_location = request.form.get('output_location')
    job_id = run_mapred(input_data, map_fn, reduce_fn, output_location)
    return job_id

# API endpoint for destroying the cluster
@app.route('/destroy_cluster', methods=['POST'])
def handle_destroy_cluster():
    cluster_id = request.form.get('cluster_id')
    result = destroy_cluster(cluster_id)
    return result

if __name__ == '__main__':
    app.run(debug=True)
