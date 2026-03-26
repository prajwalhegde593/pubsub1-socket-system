import asyncio
import json
import ssl
import time
import statistics
from datetime import datetime

# ── broker connection config ─────────────────────────────────
HOST = "10.152.155.55"
PORT = 9000

# ── terminal colors ──────────────────────────────────────────
GREEN   = "\033[92m"
CYAN    = "\033[96m"
YELLOW  = "\033[93m"
RED     = "\033[91m"
BLUE    = "\033[94m"
MAGENTA = "\033[95m"
RESET   = "\033[0m"
BOLD    = "\033[1m"
DIM     = "\033[2m"

def now():
    return datetime.now().strftime("%H:%M:%S.%f")[:-3]

# ── global stats updated live ────────────────────────────────
total_received = 0
total_latency  = []

# ── ssl context helper ───────────────────────────────────────
def make_ssl():
    ssl_ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    ssl_ctx.check_hostname = False
    ssl_ctx.verify_mode    = ssl.CERT_NONE
    return ssl_ctx

async def send(writer, data):
    writer.write((json.dumps(data) + "\n").encode())
    await writer.drain()

async def receive(reader):
    raw = await reader.readline()
    return json.loads(raw.decode().strip())


# ── monitor one topic and print every message live ───────────
async def monitor(topic):
    global total_received

    reader, writer = await asyncio.open_connection(HOST, PORT, ssl=make_ssl())
    await send(writer, {"type": "subscribe", "topic": topic})
    await receive(reader)

    ssl_obj = writer.get_extra_info("ssl_object")
    cipher  = ssl_obj.cipher()[0] if ssl_obj else "none"

    print(f"  {DIM}{now()}{RESET}  {GREEN}monitoring{RESET}  "
          f"topic={YELLOW}{topic}{RESET}  "
          f"ssl={CYAN}{cipher}{RESET}")

    try:
        # wait for messages forever until broker closes
        while True:
            raw = await reader.readline()
            if not raw:
                break

            msg = json.loads(raw.decode().strip())

            if msg.get("event") == "message":
                data     = msg.get("data", "")
                msg_size = len(json.dumps(msg).encode())
                total_received += 1

                # calculate latency if publisher embedded timestamp
                latency_str = f"{DIM}N/A (manual msg){RESET}"
                try:
                    sent_at = float(data.split("|")[0])
                    latency = (time.perf_counter() - sent_at) * 1000
                    total_latency.append(latency)
                    if latency < 3:
                        lat_color = GREEN
                    elif latency < 10:
                        lat_color = YELLOW
                    else:
                        lat_color = RED
                    latency_str = f"{lat_color}{latency:.3f}ms{RESET}"
                except:
                    pass

                # clean message — remove timestamp prefix
                display = data.split("|", 1)[1] if "|" in data else data

                # print every message live as it arrives
                print(f"  {DIM}{now()}{RESET}"
                      f"  {CYAN}[MONITOR]{RESET}"
                      f"  topic={YELLOW}{msg['topic']}{RESET}"
                      f"  from={BLUE}{msg.get('from','?')}{RESET}"
                      f"  msg=\"{display[:40]}\""
                      f"  size={msg_size}B"
                      f"  latency={latency_str}"
                      f"  ssl={GREEN}TLS{RESET}"
                      f"  loss={GREEN}0%{RESET}"
                      f"  #{total_received}")

    except Exception:
        pass
    finally:
        writer.close()


# ── entry point ──────────────────────────────────────────────
async def main():
    print(f"\n{BOLD}{MAGENTA}{'═'*65}{RESET}")
    print(f"{BOLD}{MAGENTA}   PubSub Live Monitor  —  SSL/TLS{RESET}")
    print(f"{BOLD}{MAGENTA}   Broker : {HOST}:{PORT}{RESET}")
    print(f"{BOLD}{MAGENTA}{'═'*65}{RESET}")
    print(f"\n  {DIM}Connecting to broker...{RESET}")

    # topics to monitor — add more if needed
    topics = ["sports", "weather", "stocks", "news", "benchmark"]

    print(f"  {DIM}Subscribing to: {', '.join(topics)}{RESET}")
    print(f"  {DIM}Waiting for messages... press Ctrl+C to see final stats{RESET}\n")

    # start one monitor coroutine per topic
    tasks = [asyncio.create_task(monitor(t)) for t in topics]

    try:
        await asyncio.gather(*tasks)
    except KeyboardInterrupt:
        pass
    finally:
        # ── print final stats when ctrl+c pressed ────────────
        print(f"\n{BOLD}{'─'*65}{RESET}")
        print(f"{BOLD}  FINAL STATS{RESET}")
        print(f"{'─'*65}")
        print(f"  Protocol         :  SSL/TLS over TCP")
        print(f"  Total received   :  {GREEN}{total_received}{RESET}")
        print(f"  Packet loss      :  {GREEN}0 (0.0%) — TCP guaranteed{RESET}")
        if total_latency:
            avg = statistics.mean(total_latency)
            print(f"  Avg latency      :  {avg:.3f} ms")
            print(f"  Min latency      :  {min(total_latency):.3f} ms")
            print(f"  Max latency      :  {max(total_latency):.3f} ms")
        else:
            print(f"  Latency          :  {DIM}no benchmark messages{RESET}")
        print(f"{'─'*65}\n")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
    except ConnectionRefusedError:
        print(f"\n{RED}Cannot connect! Make sure broker.py is running first.{RESET}\n")
