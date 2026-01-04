#!/usr/bin/env python3
import socket
import subprocess
import sys
import time
from typing import Iterable, List, Tuple


# -------------------------
# CONFIG (edit these)
# -------------------------
LISTEN_HOST = "0.0.0.0"
# LISTEN_PORT = 31908
LISTEN_PORT = 32123

# SSH commands to run after the "-1" sentinel and "another tasks" finish.
# IMPORTANT: use key-based auth (ssh keys), and test these commands manually first.
COMMANDS_TO_RUN: List[List[str]] = [
    # ["ssh", "-o", "BatchMode=yes", "38.147.83.25", "python3", "inference.py", "--model-path", "dorekofu/Affine-1912-1936", "--port", "32122"],
]

READY_SIGNAL = "Affine\n"


# -------------------------
# TASK PLACEHOLDERS
# -------------------------
def process_number_task(n: int) -> None:
    """
    Task for each number received (until -1).
    Replace with your real logic.
    """
    # Example work:
    print(f"[ServerB] Processing number: {n}")
    time.sleep(0.05)


def after_end_tasks() -> bool:
    """
    Runs after -1 is received.
    Return True when finished successfully, False otherwise.
    Replace with your real logic.
    """
    print("[ServerB] Received -1. Running after-end tasks...")
    time.sleep(0.5)
    print("[ServerB] After-end tasks completed.")
    return True


def run_commands(commands: List[List[str]]) -> Tuple[bool, str]:
    """
    Runs SSH commands locally (which execute remote commands).
    Returns (success, log_text).
    """
    if not commands:
        return True, "[ServerB] No SSH commands configured.\n"

    logs = []
    for cmd in commands:
        try:
            logs.append(f"[ServerB] Running SSH: {' '.join(cmd)}\n")
            completed = subprocess.run(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                check=False,
                timeout=120,
            )
            logs.append(completed.stdout or "")
            if completed.returncode != 0:
                logs.append(f"[ServerB] SSH command failed (code={completed.returncode}).\n")
                return False, "".join(logs)
        except subprocess.TimeoutExpired:
            logs.append("[Server2] SSH command timed out.\n")
            return False, "".join(logs)
        except Exception as e:
            logs.append(f"[Server2] SSH command error: {e}\n")
            return False, "".join(logs)

    return True, "".join(logs)


# -------------------------
# NETWORK HELPERS
# -------------------------
def recv_lines(conn: socket.socket) -> Iterable[Tuple[str, int]]:
    """
    Generator that yields (env_name, number) from newline-delimited lines received on a socket,
    where each line is in the format "env_name:number".
    """
    buf = b""
    while True:
        data = conn.recv(4096)
        if not data:
            break
        buf += data
        while b"\n" in buf:
            line, buf = buf.split(b"\n", 1)
            line_decoded = line.decode("utf-8", errors="replace").strip()
            if ":" in line_decoded:
                env_name, number_str = line_decoded.split(":", 1)
                env_name = env_name.strip()
                try:
                    number = int(number_str.strip())
                    yield (env_name, number)
                except ValueError:
                    # Skip lines where the number part is not integer
                    continue
            else:
                # Skip lines that do not match the expected format
                continue


def main() -> int:
    print(f"[ServerB] Listening on {LISTEN_HOST}:{LISTEN_PORT} ...")

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.bind((LISTEN_HOST, LISTEN_PORT))
        s.listen(1)

        conn, addr = s.accept()
        conn.settimeout(None)
        with conn:
            print(f"[ServerB] Connected by {addr}")

            # Main loop: keep handling batches forever
            while True:
                got_any = False
                for line in recv_lines(conn):
                    if line == "":
                        continue

                    try:
                        n = int(line)
                    except ValueError:
                        print(f"[ServerB] Ignoring non-integer input: {line!r}")
                        continue

                    got_any = True

                    if n == -1:
                        # End of current batch
                        ok = after_end_tasks()
                        if ok:
                            commands_ok, commands_log = run_commands(COMMANDS_TO_RUN)
                            print(commands_log, end="")
                            if commands_ok:
                                # Tell Server1 it's ready for the next batch
                                conn.sendall(READY_SIGNAL.encode("utf-8"))
                                print("[ServerB] Sent READY to Server1.")
                            else:
                                # If SSH fails, you can decide what to do:
                                # For now, still send READY or send ERROR.
                                conn.sendall("ERROR\n".encode("utf-8"))
                                print("[ServerB] Sent ERROR to Server1 (SSH failed).")
                        else:
                            conn.sendall("ERROR\n".encode("utf-8"))
                            print("[ServerB] Sent ERROR to Server1 (after-end task failed).")

                        # Break out of the recv_lines loop to start waiting for next batch.
                        break
                    else:
                        process_number_task(n)

                if not got_any:
                    print("[ServerB] Connection closed by peer.")
                    break

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
