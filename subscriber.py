import asyncio
import json
import ssl
from datetime import datetime

# ── broker connection config ─────────────────────────────────
# change HOST to broker laptop IP when using two laptops
HOST = "192.168.56.1"
PORT = 9000

# ── terminal colors ──────────────────────────────────────────
GREEN  = "\033[92m"
CYAN   = "\033[96m"
YELLOW = "\033[93m"
BLUE   = "\033[94m"
RESET  = "\033[0m"
BOLD   = "\033[1m"
DIM    = "\033[2m"

def now():
    return datetime.now().strftime("%H:%M:%S.%f")[:-3]

# ── send json message to broker ──────────────────────────────
async def send(writer, data):
    writer.write((json.dumps(data) + "\n").encode())
    await writer.drain()


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

    print(f"\n{BOLD}{BLUE}╔══════════════════════════════════════════╗{RESET}")
    print(f"{BOLD}{BLUE}║   Subscriber  —  SSL enabled             ║{RESET}")
    print(f"{BOLD}{BLUE}║   Broker : {HOST}:{PORT}         ║{RESET}")
    print(f"{BOLD}{BLUE}║   Cipher : {cipher:<32}║{RESET}")
    print(f"{BOLD}{BLUE}╚══════════════════════════════════════════╝{RESET}\n")

    topic = input("Enter topic to subscribe to: ")

    # send subscribe request to broker
    await send(writer, {"type": "subscribe", "topic": topic})

    print(f"  {GREEN}Subscribed to '{topic}'. Waiting for messages...{RESET}\n")

    msg_count = 0

    # ── keep listening for messages forever ──────────────────
    try:
        while True:
            raw = await reader.readline()
            if not raw:
                break

            message = json.loads(raw.decode().strip())

            if message.get("event") == "message":
                msg_count += 1
                data = message["data"]

                # remove timestamp prefix added by publisher
                if "|" in data:
                    data = data.split("|", 1)[1]

                print(f"  {DIM}{message['timestamp']}{RESET}"
                      f"  [{YELLOW}{message['topic']}{RESET}]"
                      f"  {data}"
                      f"  {DIM}#{msg_count}{RESET}")

    except KeyboardInterrupt:
        pass
    finally:
        writer.close()
        print(f"\n  Disconnected. Received {msg_count} message(s).\n")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except ConnectionRefusedError:
        print("Cannot connect! Is broker.py running?")
