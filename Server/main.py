# server/main.py
from flask import Flask, render_template
import os
from flask_socketio import SocketIO, emit
import threading
from job_manager import JobManager
from server_socket import ServerSocket

app = Flask(__name__)
job_manager = JobManager()
socketio = SocketIO(app, async_mode='eventlet')

# @app.route('/')
# def view_logs():
#     log_path = "server.log"
#     if os.path.exists(log_path):
#         with open(log_path, 'r') as f:
#             content = f.read()
#     else:
#         content = "Log file not found."

#     # Build current job list and dummy allocations (e.g., all at 0.5)
#     job_status = []
#     for job in job_manager.jobs:
#                 job_id = job["job_id"]
#                 job_name = job["job_name"]
#                 resource_utilization = job["initial_utilization"]
#                 resource_reduction = 1 - resource_utilization
#                 power = float(job["power_func"](resource_reduction))  # Use the allocation value
#                 job_status.append({"job_id": job_id, "resource_reduction": round(resource_reduction,2),
#                                    "power": round(power, 2),"job_name":job_name,
#                                   "current_bid": round(job["current_bid"], 2) if job.get("current_bid") is not None else None})

#     total_power = job_manager.get_total_power()

#     return render_template("log.html", logs=content, jobs=job_status, total_power=round(total_power, 2))

@app.route('/')
def view_logs():
    return render_template("log.html")

def emit_job_updates():
    while True:
        socketio.sleep(3)  # every 3 seconds
        job_status = []
        for job in job_manager.jobs:
            job_id = job["job_id"]
            job_name = job["job_name"]
            resource_utilization = job["initial_utilization"]
            resource_reduction = 1 - resource_utilization
            power = float(job["power_func"](resource_reduction))
            current_bid = job.get("current_bid")
            job_status.append({
                "job_id": job_id,
                "job_name": job_name,
                "resource_reduction": round(resource_reduction, 2),
                "power": round(power, 2),
                "current_bid": round(current_bid, 2) if current_bid is not None else None
            })
        clearing_price = round(job_manager.current_clearing_price, 2) if job_manager.current_clearing_price is not None else None
        total_power = round(job_manager.get_total_power(), 2)
        socketio.emit("job_update", {"jobs": job_status, "total_power": total_power,"warning": total_power > job_manager.subscribed_power,
                                     "current_clearing_price": clearing_price,"subscribed_power": job_manager.subscribed_power,"execution_time": job_manager.market_clearing_execution_time})

# Start background job broadcasting
@socketio.on('connect')
def handle_connect():
    print("[SocketIO] Flask App Client connected.")

@app.route('/ping')
def ping():
    return "OK", 200

def run_socket_server():
    server = ServerSocket(job_manager)
    server.start_server()

def run_flask_app():
    # app.run(host="0.0.0.0", port=5000)
    socketio.start_background_task(emit_job_updates)
    socketio.run(app, host='0.0.0.0', port=5000)

if __name__ == "__main__":
    # Launch socket server thread
    socket_thread = threading.Thread(target=run_socket_server)
    socket_thread.daemon = True
    socket_thread.start()

    flask_thread = threading.Thread(target=run_flask_app, daemon=True)
    flask_thread.start()
    
    print("[Main] Both socket server and Flask app started.")

    # Keep the main thread alive (or do something else)
    socket_thread.join()
    flask_thread.join()