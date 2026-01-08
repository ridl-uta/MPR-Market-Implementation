import socket
import threading
import pickle
import traceback
import numpy as np
import pandas as pd
import io
import time
from concurrent.futures import ThreadPoolExecutor

class ServerSocket:
    def __init__(self, job_manager, host="0.0.0.0", port=8000):
        self.job_manager = job_manager
        self.host = host
        self.port = port
        self.connected_clients = []
        self.mode = "accepting"
        self.mode_lock = threading.Lock()
        self.active_bids = {}
        self.bid_received_times = {}
        job_manager.server_socket = self

    def send_pickle_message(self, conn, obj):
        try:
            data = pickle.dumps(obj)
            length = len(data).to_bytes(4, byteorder='big')
            conn.sendall(length + data)
        except Exception as e:
            print(f"[Server] Failed to send message: {e}")

    def recv_pickle_message(self, conn):
        length_data = b""
        while len(length_data) < 4:
            more = conn.recv(4 - len(length_data))
            if not more:
                return None
            length_data += more
        msg_len = int.from_bytes(length_data, byteorder='big')

        data = b""
        while len(data) < msg_len:
            more = conn.recv(min(4096, msg_len - len(data)))
            if not more:
                return None
            data += more
        return pickle.loads(data)

    def start_server(self):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind((self.host, self.port))
            s.listen()
            print(f"[Server] Listening on {self.host}:{self.port}")

            while True:
                conn, addr = s.accept()
                with self.mode_lock:
                    if self.mode != "accepting":
                        self.send_pickle_message(conn, {"error": "Server is busy with negotiation."})
                        conn.close()
                        continue
                print(f"[Server] Accepted connection from {addr}")
                threading.Thread(target=self.handle_client, args=(conn, addr)).start()

    def handle_client(self, conn, addr):
        print(f"[Server] Connection from {addr}")
        with conn:
            try:
                self.send_pickle_message(conn, {"message": "Welcome to the HPC Manager Server."})
                while True:
                    message = self.recv_pickle_message(conn)
                    if not message:
                        print(f"[Server] Client {addr} disconnected.")
                        break

                    if not isinstance(message, dict) or "command" not in message:
                        self.send_pickle_message(conn, {"error": "Invalid message format."})
                        continue

                    command = message["command"]

                    with self.mode_lock:
                        if self.mode != "accepting" and command != "submit_bid":
                            self.send_pickle_message(conn, {"error": "Server is currently busy."})
                            continue

                    if command == "create_job":
                        self._handle_create_job(conn, message)
                        self.connected_clients.append(conn)  # Register persistent connection
                    elif command == "submit_bid":
                        job_id = message.get("job_id")
                        bid = message.get("bid")
                        if job_id and bid is not None:    
                            self.active_bids[job_id] = bid
                            self.bid_received_times[job_id] = time.perf_counter()
                            #print(f"[Server] Received bid from {job_id}: {bid:.4f}")
                            self.send_pickle_message(conn, {"message": "Bid received."})
                        else:
                            self.send_pickle_message(conn, {"error": "Missing job_id or bid."})
                    else:
                        self.send_pickle_message(conn, {"error": f"Unknown command: {command}"})

            except Exception as e:
                print(f"[Server] Error with client {addr}: {e}")
                traceback.print_exc()
                try:
                    self.send_pickle_message(conn, {"error": str(e)})
                except:
                    pass

    def _handle_create_job(self, conn, message):
        try:
            job_name = message["job_name"]
            initial_utilization = message["initial_utilization"]
            perf_data = message["perf_data"]
            job_id = self.job_manager.add_job(job_name, initial_utilization, perf_data)
            self.send_pickle_message(conn, {"message": "Job received and stored.",
                                            "job_id": job_id})
        except KeyError as e:
            self.send_pickle_message(conn, {"error": f"Missing key in create_job: {e}"})

    # def get_total_reduction_at_q(self, q, current_bids, job_dfs):
    #     def supply_function(b, q, delta_max):
    #         return max(delta_max - b / q, 0)

    #     total_reduction = 0.0
    #     for job, b in zip(self.job_manager.jobs, current_bids.values()):
    #         df = pd.read_json(io.StringIO(job["perf_data"]))
    #         delta_max = df["Resource Reduction"].max()
    #         reduction = supply_function(b, q, delta_max)
    #         power_max = df["Power"].max()
    #         total_reduction += power_max * reduction
    #     return total_reduction

    def get_total_reduction_at_q(self, q, current_bids, job_dfs):
        def supply_function(b, q, delta_max):
            return max(delta_max - b / q, 0)

        total_power = 0.0
        for job, b in zip(self.job_manager.jobs, current_bids.values()):
            # df = pd.read_json(io.StringIO(job["perf_data"]))
            # delta_max = df["Resource Reduction"].max()
            delta_max = job["max_reduction"]  # Assuming delta_max is stored in the job data
            delta_m = supply_function(b, q, delta_max)

            # Compute actual power using the job's interpolation function
            power_func = job.get("power_func")
            if power_func:
                power_reduction =job["power_max"] - float(power_func(delta_m))
                total_power += power_reduction
            else:
                print(f"[Warning] Missing power_func for job {job['job_id']}")

        return total_power

    def compute_final_supply_array(self, q_final, bid_dict):
        start_ts = time.time()
        supply_array = []
        total_jobs = len(self.job_manager.jobs)
        print(f"[MPR-INT] Building final supply array for {total_jobs} jobs (q_final={q_final:.6f})")

        for idx, job in enumerate(self.job_manager.jobs):
            job_id = job["job_id"]
            # print(f"[MPR-INT]   computing job {idx+1}/{total_jobs} -> {job_id}")
            # df = pd.read_json(io.StringIO(job["perf_data"]))
            # delta_max = df["Resource Reduction"].max()
            delta_max = job["max_reduction"]  # Assuming delta_max is stored in the job data
            bid = bid_dict.get(job_id, 0)
            delta = max(delta_max - bid / q_final, 0)
            supply_array.append({
                "job_id": job_id,
                "bid": round(bid, 4),
                "clearing_price_q": round(q_final, 4),
                "delta_m": round(delta, 4)
            })

        print(f"[MPR-INT] Final supply array built in {(time.time() - start_ts):.3f}s")
        return supply_array

    def initiate_mpr_int_negotiation(self, C_target, q_bounds=(0.1, 5), tolerance=1e-2, max_iters=12):
        with self.mode_lock:
            self.mode = "negotiation"
        print("[MPR-INT] Starting MPR-INT negotiation...Target C:", C_target)

        print("[MPR-INT] Starting negotiation mode. Rejecting new job submissions.")
        q_current = (q_bounds[0] + q_bounds[1]) / 2
        delta_q_current = None
        bidding_history = {}
        current_iteration = None
        last_valid_q = None
        last_valid_bids = None

        total_communication_time = 0
        total_negotiation_time = 0

        try:
            for iteration in range(max_iters):
                startTimeCommunication = time.time()
                current_iteration = iteration
                print(f"[MPR-INT] Iteration {iteration}, Sending q′ = {q_current:.4f}")
                current_bids = {job["job_id"]: None for job in self.job_manager.jobs}
                self.active_bids.clear()
                self.bid_received_times.clear()
                send_times = {job["job_id"]: time.perf_counter() for job in self.job_manager.jobs}

                # Pre-serialize the payload once and send in parallel to avoid slow sockets blocking others
                payload = {"command": "update_price", "q": q_current}

                def _send(conn):
                    try:
                        self.send_pickle_message(conn, payload)
                    except Exception as e:
                        print(f"[MPR-INT] Failed to send to client: {e}")

                with ThreadPoolExecutor(max_workers=min(32, len(self.connected_clients) or 1)) as pool:
                    list(pool.map(_send, self.connected_clients))

                remaining_jobs = set(current_bids.keys())
                while remaining_jobs:
                    for job_id in list(remaining_jobs):
                        if job_id in self.active_bids:
                            current_bids[job_id] = self.active_bids[job_id]
                            remaining_jobs.remove(job_id)
                    # time.sleep(0.05)

                # Log bid latencies before running bisection
                latencies = []
                missing = []
                for job_id in current_bids.keys():
                    if job_id in self.bid_received_times and job_id in send_times:
                        lat = self.bid_received_times[job_id] - send_times[job_id]
                        latencies.append(lat)
                    else:
                        missing.append(job_id)
                if latencies:
                    lat_arr = np.array(latencies)
                    print(f"[MPR-INT] Bid latencies (s): min={lat_arr.min():.3f}, median={np.median(lat_arr):.3f}, max={lat_arr.max():.3f}")
                if missing:
                    print(f"[MPR-INT] Missing bids from: {', '.join(missing)}")

                communication_time_delta = time.time() - startTimeCommunication
                print(f"[Time-log] communication time delta: {communication_time_delta}s")
                total_communication_time += communication_time_delta
                
                bidding_history[q_current] = current_bids.copy()

                def clearing_price_root(q_try):
                    total_reduction = self.get_total_reduction_at_q(q_try, current_bids, [job["perf_data"] for job in self.job_manager.jobs])
                    # print(f"[MPR-INT] Trying q={q_try:.6f}, Total Reduction={total_reduction:.6f} residual={total_reduction - C_target:.6f} ")
                    return total_reduction - C_target

                try:
                    # Manual bisection to find smallest q where residual >= 0
                    q_low, q_high = q_bounds
                    res_low = clearing_price_root(q_low)
                    res_high = clearing_price_root(q_high)

                    if res_low > 0:
                        # print("[MPR-INT] Lower bound already positive; using q_low")
                        q_new, residual = q_low, res_low
                    elif res_high < 0:
                        # print("[MPR-INT] Upper bound still negative; keeping q_high")
                        q_new, residual = q_high, res_high
                    else:
                        startNegotiation = time.time()
                        for _ in range(32):  # sufficient iterations for typical tolerances
                            if abs(q_high - q_low) < 1e-3:
                                negotiation_time_delta = time.time() - startNegotiation
                                print(f"[Time-log] negotiation time delta: {negotiation_time_delta}s")
                                total_negotiation_time += negotiation_time_delta  
                                break
                            q_mid = 0.5 * (q_low + q_high)
                            res_mid = clearing_price_root(q_mid)
                            if res_mid >= 0:
                                q_high, res_high = q_mid, res_mid
                            else:
                                q_low, res_low = q_mid, res_mid
                        q_new, residual = q_high, res_high
                        print(f"[MPR-INT] Chose smallest q′ with non-negative residual: q={q_new:.6f}, residual={residual:.6f}")
                except ValueError:
                    print("[MPR-INT] Bisection failed to converge.")
                    bidding_history.pop(q_current, None)
                    break

                delta_q = abs(q_new - q_current)
                print(f"[MPR-INT] Current q = {q_current:.4f} New q′ = {q_new:.4f}, Δq = {delta_q:.6f}")
                if delta_q_current:
                    if abs(delta_q_current-delta_q) < tolerance:
                        print("[MPR-INT] Clearing price converged.")
                        last_valid_q = q_new
                        last_valid_bids = current_bids.copy()
                        break

                q_current = q_new
                delta_q_current = delta_q
                last_valid_q = q_current
                last_valid_bids = current_bids.copy()

        finally:
            with self.mode_lock:
                self.mode = "accepting"
            print(f"[MPR-INT] Negotiation complete on iteration {current_iteration}. Resuming job acceptance mode.")
            # print("[MPR-INT] Bidding history:", bidding_history)

            if last_valid_q is None or last_valid_bids is None:
                print("[MPR-INT] Negotiation did not converge to a valid clearing price/bids.")
                return [], bidding_history

            print("[MPR-INT] final clearing price:", last_valid_q)
            print("[MPR-INT] total reduction at final clearing price:", self.get_total_reduction_at_q(last_valid_q, last_valid_bids, [job["perf_data"] for job in self.job_manager.jobs]))
            print("[MPR-INT] computing final supply array...")
            ts = time.time()
            final_supply_array = self.compute_final_supply_array(last_valid_q, last_valid_bids)
            print(f"[MPR-INT] Final Supply Array computed in {(time.time() - ts):.3f}s:")
            print("[MPR-INT][Time-log] Total negotiation time:", total_negotiation_time, "s")
            print("[MPR-INT][Time-log] Total communication time:", total_communication_time, "s")
        return final_supply_array,bidding_history   
