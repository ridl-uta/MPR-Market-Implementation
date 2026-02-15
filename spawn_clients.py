# spawn_clients.py
import subprocess
import time
import argparse
import sys

parser = argparse.ArgumentParser(description="Spawn multiple client processes.")
parser.add_argument("--count", type=int, default=3, help="Number of clients to spawn")
parser.add_argument("--host", default="127.0.0.1", help="HPC manager host")
parser.add_argument("--port", type=int, default=8000, help="HPC manager port")
parser.add_argument("--http_port", type=int, default=5000, help="HPC manager Flask port")
parser.add_argument("--perf_data_path", default="all_model_data.xlsx", help="Path to performance data file")
parser.add_argument("--job_types", default="xsbench,comd,minife", help="Comma-separated job types")
parser.add_argument("--script_path", default="Client/main.py", help="Path to client script")
args = parser.parse_args()

num_clients = args.count
job_types = [j.strip() for j in args.job_types.split(",") if j.strip()]
perf_data_path = args.perf_data_path
script_path = args.script_path
hpc_manager_host = args.host
hpc_manager_port = args.port
hpc_manager_flask_port = args.http_port

client_processes = []

for i in range(num_clients):
    job = job_types[i % len(job_types)]
    process = subprocess.Popen([sys.executable, script_path, "--job", job, "--host", hpc_manager_host, "--port",str(hpc_manager_port),
                                "--perf_data_path", perf_data_path,"--http_port", str(hpc_manager_flask_port)],)
    client_processes.append(process)
    print(f"Started client {i+1} with job: {job}")
    time.sleep(.25)

print(f"{num_clients} clients started. They will continue running and ping the server.")
