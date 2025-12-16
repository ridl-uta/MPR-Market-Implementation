import socket
import pickle
import pandas as pd
import argparse
import uuid
import requests
import threading
import time
import numpy as np
import os

CLIENT_NAME = f"Worker_{uuid.uuid4().hex[:6]}"
HPC_MANAGER_HOST = "127.0.0.1"
HPC_MANAGER_PORT = 8000
HPC_MANAGER_FLASK_SERVER_PORT = 5000

# # Load and tag all application jobs
# xls_path = "all_model_data.xlsx"
# sheets = ["comd", "xsbench", "minife"]
# job_dataframes = []

# for sheet in sheets:
#     df = pd.read_excel(xls_path, sheet_name=sheet)
#     df["Job_Type"] = sheet
#     job_dataframes.append(df)

# # Combine all jobs and reset index
# base_jobs_df = pd.concat(job_dataframes, ignore_index=True)


# --------------------- Message Framing ---------------------

def send_pickle_message(sock, obj):
    data = pickle.dumps(obj)
    length = len(data).to_bytes(4, byteorder='big')
    sock.sendall(length + data)

def recv_pickle_message(sock):
    # Receive 4-byte message length
    length_data = b""
    while len(length_data) < 4:
        more = sock.recv(4 - len(length_data))
        if not more:
            raise ConnectionError("Socket closed before receiving message length.")
        length_data += more

    msg_len = int.from_bytes(length_data, byteorder='big')

    # Receive full message
    data = b""
    while len(data) < msg_len:
        more = sock.recv(min(4096, msg_len - len(data)))
        if not more:
            raise ConnectionError("Socket closed before receiving full message.")
        data += more

    return pickle.loads(data)

# --------------------- Job Creation ---------------------

def create_dummy_job(job_name, perf_data=None):
    # perf_data = pd.DataFrame({
    #     "Resource Reduction": [0.0, 0.2, 0.4, 0.6],
    #     "Extra Execution": [0.0, 0.1, 0.25, 0.5],
    #     "Power": [100, 90, 80, 70]
    # })

    job_message = {
        "command": "create_job",
        "user_id": CLIENT_NAME,
        "job_name": job_name,
        "initial_utilization": 1.0,
        "perf_data": perf_data.to_json(orient="records")
    }
    return job_message, perf_data

# --------------------- Optimization ---------------------

def maximize_net_gain_with_data_brute(df, q, delta_max, resolution=500, min_skip_idx=5):
    def supply(b, q, delta):
        return max(delta - b / q, 0)

    def cost_from_data_unit(x, df, delta_m, epsilon=1e-6):
        if x < epsilon:
            return 0
        x_scaled = np.clip(x / delta_m, 0, 1)
        df_sorted = df.sort_values(by="Resource Reduction")
        L_x = np.interp(x_scaled, df_sorted["Resource Reduction"], df_sorted["Extra Execution"])
        return L_x / x_scaled

    def net_gain(b):
        x = supply(b, q, delta_max)
        return q * x - cost_from_data_unit(x, df, delta_max)

    b_vals = np.linspace(1e-6, q * delta_max, resolution)
    gains = [net_gain(b) for b in b_vals]

    gradient = np.gradient(gains)
    slope_change_idx = np.argmax(gradient[min_skip_idx:] > 0) + min_skip_idx
    best_idx = slope_change_idx + np.argmax(gains[slope_change_idx:])
    best_bid = b_vals[best_idx]
    best_gain = gains[best_idx]

    return best_bid, best_gain, net_gain

# --------------------- Persistent Connection ---------------------

def persistent_client_loop(job_msg, perf_df, delta_max, host, port, max_retries=None, backoff_base=1, backoff_cap=30):
    """Maintain a persistent connection; reconnect with backoff on failures."""
    attempt = 0

    while True:
        job_id = None  # reset per connection
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.connect((host, port))
                print(f"[{CLIENT_NAME}] Connected to server.")

                send_pickle_message(s, job_msg)
                response = recv_pickle_message(s)
                print(f"[{CLIENT_NAME}] Server response: {response.get('message')}")

                while True:
                    try:
                        message = recv_pickle_message(s)
                        if not message:
                            raise ConnectionError("No message received (connection closed?)")
                        
                        if "job_id" in message:
                            job_id = message["job_id"]
                            print(f"[{CLIENT_NAME}] Received job_id: {job_id}")

                        if job_id is not None:
                            command = message.get("command")

                            if command == "update_price":
                                q = message.get("q")
                                print(f"[{CLIENT_NAME}] Received q′ = {q:.4f}")
                                bid, gain, _ = maximize_net_gain_with_data_brute(perf_df, q, delta_max)

                                bid_response = {
                                    "command": "submit_bid",
                                    "job_id": job_id,
                                    "bid": bid
                                }
                                send_pickle_message(s, bid_response)
                                print(f"[{CLIENT_NAME}] Sent bid: {bid:.4f}")
                            
                            else:
                                print(f"[{CLIENT_NAME}] Unknown command received: {command}")

                    except (EOFError, ConnectionError) as e:
                        print(f"[{CLIENT_NAME}] Server closed connection or failed to parse: {e}")
                        break
                    except Exception as e:
                        print(f"[{CLIENT_NAME}] Unexpected error: {e}")
                        break

        except Exception as e:
            attempt += 1
            if max_retries is not None and attempt > max_retries:
                print(f"[{CLIENT_NAME}] Failed to connect or communicate after {attempt-1} retries: {e}")
                break

            sleep_for = min(backoff_cap, backoff_base * (2 ** (attempt - 1)))
            print(f"[{CLIENT_NAME}] Connection error ({e}), retrying in {sleep_for:.1f}s... (attempt {attempt})")
            time.sleep(sleep_for)
            continue

# --------------------- Main Entry ---------------------

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--job", type=str, default="test_job")
    parser.add_argument("--host", type=str, default=HPC_MANAGER_HOST)
    parser.add_argument("--port", type=int, default=HPC_MANAGER_PORT)
    parser.add_argument("--perf_data_path", type=str, required=True)
    parser.add_argument("--http_port", type=int, default=HPC_MANAGER_FLASK_SERVER_PORT)
    args = parser.parse_args()


    # Load the CSV file
    perf_data_path = os.path.abspath(args.perf_data_path)
    print(f"[{CLIENT_NAME}] Loading performance data from: {perf_data_path}")

    sheets = ["comd", "xsbench", "minife"]
    job_dataframes = []

    for sheet in sheets:
        df = pd.read_excel(perf_data_path, sheet_name=sheet)
        df["Job_Type"] = sheet
        job_dataframes.append(df)

    # Combine all jobs and reset index
    base_jobs_df = pd.concat(job_dataframes, ignore_index=True)
    performance_df = base_jobs_df[base_jobs_df["Job_Type"] == args.job]


    job_msg, perf_df = create_dummy_job(args.job, performance_df)
    delta_max = perf_df["Resource Reduction"].max()
    job_id = ""


    # Uncomment if your server supports /ping endpoint
    # threading.Thread(target=keep_alive, args=(args.host, args.http_port), daemon=True).start()

    persistent_client_loop(job_msg, perf_df, delta_max, args.host, args.port)
