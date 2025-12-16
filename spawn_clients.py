# spawn_clients.py
import subprocess
import time

num_clients = 3
job_types = ["xsbench", "comd", "minife"]
perf_data_path = "all_model_data.xlsx"
script_path = "Client/main.py"  # Adjust if needed
hpc_manager_host = "127.0.0.1"
hpc_manager_port = 8000  # Default port for the server
hpc_manager_flask_port = 5000  # Default port for the Flask server

client_processes = []

for i in range(num_clients):
    job = job_types[i % len(job_types)]
    process = subprocess.Popen(["python", script_path, "--job", job, "--host", hpc_manager_host, "--port",str(hpc_manager_port),
                                "--perf_data_path", perf_data_path,"--http_port", str(hpc_manager_flask_port)],)
    client_processes.append(process)
    print(f"Started client {i+1} with job: {job}")
    time.sleep(1)

print(f"{num_clients} clients started. They will continue running and ping the server.")