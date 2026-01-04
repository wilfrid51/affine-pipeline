#!/usr/bin/env python3
import socket
import time
import subprocess
import sys
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from types import SimpleNamespace
from typing import List
from pathlib import Path


# -------------------------
# CONFIG (edit these)
# -------------------------
SERVER2_IP = "66.153.184.201"   # <-- put Server2's IP here
SERVER2_PORT = 3032   # 3032-3038

BATCH_DELAY_SECONDS = 1.0  # delay between batches


# ------------------------
# Run the python code in the out and catch output
# example: run_with_venv("math/.venv/bin/python", "math/dot_product.py", args=["--flag", "1"], cwd="math", input=inp, run_id=i)
# ------------------------
def run_with_venv(venv_python, script, args=(), cwd=None, input=None, run_id=None):
    """Create and return a subprocess. Does not handle output."""
    base = Path.cwd()
    resolve = lambda p: base / p if not Path(p).is_absolute() else Path(p)
    cmd = [str(resolve(venv_python)), str(resolve(script)), *map(str, args)]
    cwd = str(resolve(cwd)) if cwd else None
    
    p = subprocess.Popen(cmd, cwd=cwd, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                        stdin=subprocess.PIPE if input else None, text=True, bufsize=1)
    if input:
        p.stdin.write(input)
        p.stdin.close()
    
    return p, run_id

# --------------------------
# Communication part
# --------------------------
def send_text(conn: socket.socket, data: str) -> None:
    try:
        conn.sendall(f"{data}\n".encode("utf-8"))
    except Exception as e:
        print(f"[ERROR] Failed to send text: {e}")
        return False
    return True

def send_batch(conn: socket.socket, data: List[str]) -> None:
    """
    Send numbers each on its own line, then -1 sentinel line.
    """
    success = 0
    for n in data:
        try:
            conn.sendall(f"{n}\n".encode("utf-8"))
            success += 1
        except Exception as e:
            print(f"[ERROR] Failed to send batch: {e}")
    try:
        conn.sendall(b"-1\n")
    except Exception as e:
        print(f"[ERROR] Failed to send -1: {e}")
    return success


def recv_line(conn: socket.socket) -> str:
    """
    Receive a single newline-terminated line.
    """
    buf = b""
    while True:
        data = conn.recv(1)
        if not data:
            return ""  # connection closed
        if data == b"\n":
            return buf.decode("utf-8", errors="replace").strip()
        buf += data


# ------------------------
# Run files and get evaluation fail tasks.
# ------------------------

def catch_output(process, conn, run_id=None):
    """Handle output from a process and identify which process it's from."""
    stdout_lines, stderr_lines = [], []
    prefix = f"[RUN_ID {run_id}]:" if run_id else ""
    fail_count = 0

    def read(stream, lines, is_stderr=False):
        for line in stream:
            line = line.rstrip()
            lines.append(line)
            if is_stderr:
                # When error occured, don't care about it.
                file=sys.stderr
                break
            else:
                file = sys.stdout
                if not send_text(conn, f"{prefix}{line[1:]}"):
                    fail_count += 1
            print(f"{prefix}{line[1:]}", file=sys.stderr if is_stderr else sys.stdout, flush=True)

    t1 = threading.Thread(target=read, args=(process.stdout, stdout_lines), daemon=True)
    t2 = threading.Thread(target=read, args=(process.stderr, stderr_lines, True), daemon=True)
    t1.start()
    t2.start()
    process.wait()
    t1.join()
    t2.join()

    return SimpleNamespace(
        returncode=process.returncode, 
        stdout='\n'.join(stdout_lines), 
        stderr='\n'.join(stderr_lines), 
        fail_count=fail_count
    )

def get_evaluate_fail(conn) -> List[int]:
    """
    Run evaluation scripts & get the fail result from them
    
    return: result statistic
    rtype: connection
    """
    # Run enough evaluataion scripts but for now, just run 5.

    # config
    max_concurrency = 5      # Only 5 processes running at the same time
    dpo_sample_count = 100 # at least 1e4 tasks for dpo

    def run_single(i, env, start_pos):
        print(f"[Run {i}] Starting...", flush=True)
        # evaluate.py --env ABD-V2 --model your-model --base-url http://172.17.0.1:30000/v1 --samples 10
        # TODO: change start & end here
        process, run_id = run_with_venv("af eval",
                                    # args=["--env", env, "--base-url", f"http://{SERVER2_IP}:{SERVER2_PORT+i%5}/v1", "--start", start_pos, "--end", start_pos + 100], cwd="cortex", input="", run_id=i)
                                    args=["--env", env, "--base-url", f"http://66.153.184.201:3039/v1", "--model", "testmodel"], cwd="cortex", input="", run_id=i)
        return i, catch_output(process, conn, run_id)

    # running...
    results, futures = {}, {}
    run_id = 0
    total_tasks, succ_task, fail_task, sent_task = 0, 0, 0, 0
    with ThreadPoolExecutor(max_workers=max_concurrency) as ex:
        # while enough dpo sample is ready
        while dpo_sample_count > 0:
            while len(futures) < max_concurrency:
                import random
                env_name = "ABD"
                start_pos = random.randint(0, 23000)
                print(f"[Run {run_id}] Starting... {env_name} {start_pos} -> {start_pos + 100}...")
                future = ex.submit(run_single, run_id, env_name, start_pos)
                futures[future] = run_id
                run_id += 1

            print("HERE")
            # time.sleep(100)

            for f in as_completed(futures):
                i, cp = f.result()
                results[i] = cp
                del futures[f]
                break

            for i in results:
                result = results[i]
                total_tasks += len(result.stdout.split("\n")) - 1
                for line in result.stdout.split("\n"):
                    if line.startswith("✅"):
                        succ_task += 1
                fail_task += result.fail_count
            results.clear()
                
        for f in as_completed(futures):
            i, cp = f.result()
            results[i] = cp
        for i in results:
            result = results[i]
            total_tasks += len(result.stdout.split("\n")) - 1
            for line in result.stdout.split("\n"):
                if line.startswith("✅"):
                    succ_task += 1
            fail_task += result.fail_count
        results.clear()

    send_batch(conn, [])

    sent_task = total_tasks - succ_task - fail_task
    return total_tasks, succ_task, sent_task, fail_task


def main() -> int:
    while True:
        try:
            print(f"[Server A] Connecting to Server B at {SERVER2_IP}:{SERVER2_PORT} ...")
            with socket.create_connection((SERVER2_IP, SERVER2_PORT), timeout=10) as conn:
                conn.settimeout(None)
                print("[Server A] Connected to Server B.")

                batch_id = 1
                while True:
                    total_tasks, succ_task, sent_num, fail_num = get_evaluate_fail(conn)
                    print(f"[Server B] Batch #{batch_id}:\n")
                    print(f"\t\t\t\tTotal tasks: {total_tasks} tasks evaluated.")
                    print(f"\t\t\t\tAccuracy:    {succ_task}/{total_tasks}({succ_task/total_tasks*100}%), {total_tasks-succ_task}/{total_tasks}({(total_tasks-succ_task)/total_tasks*100}%)")
                    print(f"\t\t\t\tSent & Fail: {sent_num}/{sent_num + fail_num}({sent_num/(sent_num+fail_num)*100}%), {fail_num}/{sent_num+fail_num}({fail_num/(sent_num+fail_num)*100}%)")

                    # Wait for Server2 signal
                    signal = recv_line(conn)
                    if signal == "":
                        print("[Server1] Connection closed by Server2.")
                        break

                    print(f"[Server1] Received signal from Server2: {signal!r}")
                    if signal == "Affine":
                        batch_id += 1
                        time.sleep(BATCH_DELAY_SECONDS)
                        continue
                    else:
                        # If ERROR or unknown signal, you can retry, break, etc.
                        print("[Server1] Server2 reported an error. Stopping.")
                        break

        except (ConnectionRefusedError, TimeoutError, OSError) as e:
            print(f"[Server1] Connect failed: {e}. Retrying in 3 seconds...")
            time.sleep(3)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
