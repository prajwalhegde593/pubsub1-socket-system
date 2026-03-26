import asyncio
import json
import ssl
import time
from datetime import datetime

# ── broker connection config ─────────────────────────────────
# change HOST to broker laptop IP when using two laptops
HOST = "10.152.155.55"
PORT = 9000

# ── terminal colors ──────────────────────────────────────────
GREEN  = "\033[92m"
CYAN   = "\033[96m"
YELLOW = "\033[93m"
RESET  = "\033[0m"
BOLD   = "\033[1m"
DIM    = "\033[2m"

def now():
    return datetime.now().strftime("%H:%M:%S.%f")[:-3]

# ── send json message to broker ──────────────────────────────
async def send(writer, data):
    writer.write((json.dumps(data) + "\n").encode())
    await writer.drain()

# ── receive response from broker ─────────────────────────────
async def receive(reader):
    raw = await reader.readline()
    return json.loads(raw.decode().strip())


async def main():
    # ssl context — skip hostname check since we use self signed cert
    ssl_ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    ssl_ctx.check_hostname = False
    ssl_ctx.verify_mode    = ssl.CERT_NONE

    # connect to broker over ssl
    reader, writer = await asyncio.open_connection(HOST, PORT, ssl=ssl_ctx)

    # show ssl cipher being used
    ssl_obj = writer.get_extra_info("ssl_object")
    cipher  = ssl_obj.cipher()[0] if ssl_obj else "none"

    print(f"\n{BOLD}{CYAN}╔══════════════════════════════════════════╗{RESET}")
    print(f"{BOLD}{CYAN}║   Publisher  —  SSL enabled              ║{RESET}")
    print(f"{BOLD}{CYAN}║   Broker : {HOST}:{PORT}         ║{RESET}")
    print(f"{BOLD}{CYAN}║   Cipher : {cipher:<32}║{RESET}")
    print(f"{BOLD}{CYAN}╚══════════════════════════════════════════╝{RESET}\n")

    topic = input("Enter topic name: ")
    print(f"Type messages for '{topic}'. Type 'quit' to exit.\n")

    # ── main publish loop ────────────────────────────────────
    while True:
        msg = input(f"  {CYAN}>{RESET} ")

        if msg.lower() == "quit":
            break

        # embed timestamp in data so benchmark can measure latency
        payload = f"{time.perf_counter()}|{msg}"

        await send(writer, {
            "type":  "publish",
            "topic": topic,
            "data":  payload
        })

        # show delivery confirmation from broker
        resp      = await receive(reader)
        delivered = resp.get("delivered", 0)
        color     = GREEN if delivered > 0 else YELLOW
        print(f"  {DIM}{now()}{RESET}  {color}delivered to {delivered} subscriber(s){RESET}")

    writer.close()
    print(f"\nDisconnected.\n")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except ConnectionRefusedError:
        print("Cannot connect! Is broker.py running?")
