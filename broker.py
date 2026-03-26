import asyncio
import json
import ssl
from collections import defaultdict
from datetime import datetime

# ── server config ────────────────────────────────────────────
HOST     = "0.0.0.0"   # accept from any device on network
PORT     = 9000
CERTFILE = "cert.pem"
KEYFILE  = "key.pem"

# ── terminal colors ──────────────────────────────────────────
GREEN  = "\033[92m"
CYAN   = "\033[96m"
YELLOW = "\033[93m"
RED    = "\033[91m"
BLUE   = "\033[94m"
RESET  = "\033[0m"
BOLD   = "\033[1m"
DIM    = "\033[2m"

def now():
    return datetime.now().strftime("%H:%M:%S.%f")[:-3]

def log(tag, color, msg):
    print(f"{DIM}{now()}{RESET}  {color}{BOLD}[{tag}]{RESET}  {msg}")


# ── main broker class ────────────────────────────────────────
class PubSubBroker:
    def __init__(self):
        # topic -> set of connected subscribers
        self.subscriptions = defaultdict(set)
        # all active clients
        self.clients = {}
        # message counters
        self.stats = {"published": 0, "delivered": 0}

    def _get_timestamp(self):
        return datetime.now().strftime("%H:%M:%S.%f")[:-3]

    def _get_client_id(self, addr):
        host, port = addr
        return f"{host}:{port}"

    # ── called for every new client that connects ────────────
    async def handle_client(self, reader, writer):
        addr      = writer.get_extra_info("peername")
        client_id = self._get_client_id(addr)
        self.clients[client_id] = writer

        # show which ssl cipher is being used
        ssl_obj = writer.get_extra_info("ssl_object")
        cipher  = ssl_obj.cipher()[0] if ssl_obj else "none"
        log("CONNECT", GREEN, f"{client_id}  cipher={CYAN}{cipher}{RESET}")

        try:
            # keep reading messages from this client forever
            while True:
                raw = await reader.readline()
                if not raw:
                    break
                message = json.loads(raw.decode().strip())
                await self._handle_message(message, client_id, writer)
        except Exception as e:
            log("ERROR", RED, f"{client_id} — {e}")
        finally:
            await self._disconnect_client(client_id, writer)

    # ── decides what to do based on message type ─────────────
    async def _handle_message(self, message, client_id, writer):
        msg_type = message.get("type")

        if msg_type == "subscribe":
            topic = message.get("topic")
            self.subscriptions[topic].add((client_id, writer))
            log("SUBSCRIBE", CYAN, f"{client_id} → {YELLOW}{topic}{RESET}")
            await self._send(writer, {"status": "ok", "subscribed": topic})

        elif msg_type == "unsubscribe":
            topic = message.get("topic")
            self.subscriptions[topic].discard((client_id, writer))
            log("UNSUB", YELLOW, f"{client_id} ← {YELLOW}{topic}{RESET}")
            await self._send(writer, {"status": "ok", "unsubscribed": topic})

        elif msg_type == "publish":
            topic = message.get("topic")
            data  = message.get("data")
            self.stats["published"] += 1
            delivered = 0
            dead = set()

            # send message to every subscriber of this topic
            for (sub_id, sub_writer) in self.subscriptions.get(topic, set()):
                try:
                    await self._send(sub_writer, {
                        "event":     "message",
                        "topic":     topic,
                        "data":      data,
                        "from":      client_id,
                        "timestamp": self._get_timestamp()
                    })
                    delivered += 1
                except:
                    # subscriber disconnected, mark for removal
                    dead.add((sub_id, sub_writer))

            # remove dead connections
            self.subscriptions[topic] -= dead
            self.stats["delivered"]   += delivered
            log("PUBLISH", BLUE, f"{client_id} → {YELLOW}{topic}{RESET}  "
                f"delivered={GREEN}{delivered}{RESET}")
            await self._send(writer, {"status": "ok", "delivered": delivered})

        elif msg_type == "list_topics":
            topics = {t: len(s) for t, s in self.subscriptions.items() if s}
            await self._send(writer, {"topics": topics})

        else:
            await self._send(writer, {"status": "error", "reason": "unknown type"})

    # ── helper to send json to a client ──────────────────────
    async def _send(self, writer, data):
        try:
            writer.write((json.dumps(data) + "\n").encode())
            await writer.drain()
        except:
            pass

    # ── cleanup when client disconnects ──────────────────────
    async def _disconnect_client(self, client_id, writer):
        for topic in self.subscriptions:
            self.subscriptions[topic].discard((client_id, writer))
        self.clients.pop(client_id, None)
        try:
            writer.close()
            await writer.wait_closed()
        except:
            pass
        log("DISCONNECT", RED, f"{client_id}")


# ── start the server ─────────────────────────────────────────
async def main():
    broker = PubSubBroker()

    # load ssl certificate and key
    ssl_ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    ssl_ctx.load_cert_chain(CERTFILE, KEYFILE)

    server = await asyncio.start_server(
        broker.handle_client, HOST, PORT, ssl=ssl_ctx
    )

    print(f"\n{BOLD}{CYAN}╔══════════════════════════════════════════╗{RESET}")
    print(f"{BOLD}{CYAN}║   PubSub Broker  —  SSL/TLS enabled      ║{RESET}")
    print(f"{BOLD}{CYAN}║   Listening on 0.0.0.0:{PORT}             ║{RESET}")
    print(f"{BOLD}{CYAN}╚══════════════════════════════════════════╝{RESET}\n")

    async with server:
        await server.serve_forever()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print(f"\n{YELLOW}Broker shutting down.{RESET}\n")
