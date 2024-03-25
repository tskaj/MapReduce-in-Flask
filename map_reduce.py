import sys
import os
import subprocess
import json

def spawn_process(cmd):
    """Function to spawn a process."""
    return subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

def read_input(input_file):
    """Function to read input data from a file."""
    with open(input_file, 'r') as f:
        return f.readlines()

def main(input_file, map_file, reduce_file, output_location):
    """Main function to execute the MapReduce job."""
    # Read input data
    input_data = read_input(input_file)

    # Execute map phase
    map_output = []
    for line in input_data:
        # Spawn a mapper process
        cmd = f'python {map_file}'
        mapper_process = spawn_process(cmd)

        # Pass input data to the mapper process
        mapper_input = line.strip()
        mapper_process.stdin.write(f'{mapper_input}\n'.encode())
        mapper_process.stdin.flush()

        # Read output from mapper process
        map_output.extend(mapper_process.stdout.readlines())

        # Wait for the process to finish
        mapper_process.wait()

    # Group map output by key
    grouped_data = {}
    for item in map_output:
        key, value = item.decode().strip().split('\t')
        if key not in grouped_data:
            grouped_data[key] = []
        grouped_data[key].append(value)

    # Execute reduce phase
    with open(output_location, 'w') as f:
        for key, values in grouped_data.items():
            # Spawn a reducer process
            cmd = f'python {reduce_file}'
            reducer_process = spawn_process(cmd)

            # Pass key and values to the reducer process
            reducer_process.stdin.write(f'{key}\t{",".join(values)}\n'.encode())
            reducer_process.stdin.flush()

            # Read output from reducer process
            output = reducer_process.stdout.read().decode().strip()

            # Write output to the output file
            f.write(f'{output}\n')

            # Wait for the process to finish
            reducer_process.wait()

if __name__ == "__main__":
    if len(sys.argv) != 5:
        print("Usage: python map_reduce.py <input_file> <map_file> <reduce_file> <output_location>")
        sys.exit(1)

    input_file, map_file, reduce_file, output_location = sys.argv[1:]
    main(input_file, map_file, reduce_file, output_location)
