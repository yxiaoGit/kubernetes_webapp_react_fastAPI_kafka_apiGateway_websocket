import os
import json
import asyncio
import random
import hashlib
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import psycopg2
from aiokafka import AIOKafkaProducer, AIOKafkaConsumer
from aiokafka.admin import AIOKafkaAdminClient, NewTopic

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://db_user:db_pass@postgres-service:5432/product_db")
KAFKA_URL = os.getenv("KAFKA_URL", "kafka-service:9092")

# Global instances to reuse across connection handshakes
kafka_producer = None

# --- PRODUCTION LIFESPAN ENGINE ---
@asynccontextmanager
async def lifespan(app: FastAPI):
    global kafka_producer
    print("------ LAUNCHING STARTUP LIFESPAN ------")
    
    # 1. Wait out a small delay to ensure kafka broker finishes internal boot cycles
    await asyncio.sleep(5)
    
    # 2. Automated topic validation
    try:
        admin = AIOKafkaAdminClient(bootstrap_servers=KAFKA_URL)
        await admin.start()
        topic_list = [
            NewTopic(name="bids-topic", num_partitions=1, replication_factor=1),
            NewTopic(name="verdicts-topic", num_partitions=1, replication_factor=1)
        ]
        await admin.create_topics(new_topics=topic_list, validate_only=False)
        await admin.close()
        print("Kafka Topic structures successfully verified.")
    except Exception as e:
        print(f"Topic creation bypassed (already active): {e}")

    # 3. Share a unified persistent producer
    try:
        kafka_producer = AIOKafkaProducer(bootstrap_servers=KAFKA_URL)
        await kafka_producer.start()
        print("Global Event Stream Producer online.")
    except Exception as e:
        print(f"CRITICAL PRODUCER FAULT: {e}")

    yield # --- App runs here ---

    print("------ SHUTTING DOWN LIFESPAN ------")
    if kafka_producer:
        await kafka_producer.stop()

app = FastAPI(lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

class MessageRequest(BaseModel):
    message: str

def get_db_connection():
    try:
        return psycopg2.connect(DATABASE_URL)
    except Exception as e:
        print(f"Database direct link exception: {e}")
        return None

@app.post("/api/lookup")
def lookup_product(request: MessageRequest):
    print(f"Incoming product fetch search target text: {request.message}")
    hashed_string = hashlib.md5(request.message.encode()).hexdigest()
    
    conn = get_db_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="Database engine offline")
        
    cursor = conn.cursor()
    cursor.execute("SELECT name FROM products WHERE hash_code = %s;", (hashed_string,))
    result = cursor.fetchone()
    cursor.close()
    conn.close()
    
    if result:
        print(f"Database item matched: {result[0]}")
        return {"product": result[0], "hash": hashed_string}
    return {"product": "Unknown Product", "hash": hashed_string}

@app.websocket("/ws/trading")
async def websocket_trading_endpoint(websocket: WebSocket):
    global kafka_producer
    await websocket.accept()
    print("WebSocket client connected to trading desk pipeline.")
    
    tx_id = f"tx-{random.randint(1000, 9999)}"
    
    # Instantiate unique ephemere group to prevent packet dropping
    consumer = AIOKafkaConsumer(
        "verdicts-topic",
        bootstrap_servers=KAFKA_URL,
        group_id=f"group-{tx_id}",
        auto_offset_reset="latest"
    )
    await consumer.start()

    base_price = random.randint(150, 300)

    async def price_ticker_loop():
        nonlocal base_price
        try:
            while True:
                base_price += random.choice([-5, -2, 0, 2, 5])
                base_price = max(10, base_price)
                await websocket.send_json({"type": "PRICE_TICKER", "price": base_price})
                await asyncio.sleep(1)
        except asyncio.CancelledError:
            pass

    ticker_task = asyncio.create_task(price_ticker_loop())

    try:
        while True:
            data = await websocket.receive_text()
            event = json.loads(data)
            
            if event.get("type") == "SUBMIT_BID":
                user_bid = float(event.get("bid_price", 0))
                print(f"Bid submission registered: {user_bid} against market: {base_price}")
                
                payload = {"tx_id": tx_id, "bid_price": user_bid, "market_price": base_price}
                
                if kafka_producer:
                    await kafka_producer.send_and_wait("bids-topic", json.dumps(payload).encode('utf-8'))
                
                verdicts = []
                start_time = asyncio.get_event_loop().time()
                
                while len(verdicts) < 2 and (asyncio.get_event_loop().time() - start_time) < 3.0:
                    try:
                        msg_pack = await asyncio.wait_for(consumer.getone(), timeout=0.2)
                        verdict_data = json.loads(msg_pack.value.decode('utf-8'))
                        if verdict_data.get("tx_id") == tx_id:
                            verdicts.append(verdict_data.get("status"))
                    except asyncio.TimeoutError:
                        continue
                
                print(f"Collected Consensus responses: {verdicts}")
                accept_count = verdicts.count("ACCEPT")
                
                if accept_count == 2:
                    final_status = "SUCCESS (Both Sellers Accepted! Best Deal Secured)"
                elif accept_count == 1:
                    final_status = "SUCCESS (One Seller Accepted Your Offer!)"
                else:
                    final_status = "DENIED (Not able to find seller at this price threshold)"
                
                await websocket.send_json({"type": "BID_RESULT", "status": final_status})

    except WebSocketDisconnect:
        print("Socket channel handshake torn down cleanly.")
    finally:
        ticker_task.cancel()
        await consumer.stop()
