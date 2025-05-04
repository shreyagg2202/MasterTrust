import json
import time
import threading
import struct
import os
import pandas as pd
from websocket import WebSocketApp  # pip install websocket-client
from auth_handler import MasterSwiftAuth

# -------------------------
# Load instruments from JSON file
# -------------------------
with open("instruments.json", "r") as f:
    instruments_data = json.load(f)

instruments_list = instruments_data.get("NSE-OTH", [])
instrument_pairs = []
token_to_company = {}
for instrument in instruments_list:
    exchange_code = instrument["exchange_code"]
    try:
        token = int(instrument["code"])
    except Exception as e:
        print(f"Error converting code {instrument['code']} to int: {e}")
        continue
    instrument_pairs.append([exchange_code, token])
    token_to_company[token] = instrument["company"]

# -------------------------
# Create a DataFrame for market data snapshots.
# -------------------------
df_market = pd.DataFrame({
    "instrument_token": list(token_to_company.keys()),
    "company": [token_to_company[tok] for tok in token_to_company.keys()]
})
df_market.set_index("instrument_token", inplace=True)
excel_file = "marketdata.xlsx"
if os.path.exists(excel_file):
    df_market = pd.read_excel(excel_file, index_col="instrument_token")
    # Ensure index tokens are integers so that lookups match
    df_market.index = df_market.index.astype(int)

# -------------------------
# Global dictionary to keep the last known price for each token.
# Initialize with 0 for each token.
# -------------------------
global_last_prices = {token: 0 for token in df_market.index}

# -------------------------
# Shared data buffer for received messages
# -------------------------
data_buffer = []
buffer_lock = threading.Lock()

# -------------------------
# WebSocket Connection Setup
# -------------------------
auth = MasterSwiftAuth()
access_token = auth.access_token
ws_url = f"wss://masterswift-beta.mastertrust.co.in/ws/v1/feeds?token={access_token}"

def decode_marketdata_message(data):
    """
    Decode the binary marketdata message according to the API specification.
    Example structure (byte indices):
      Byte 0        : Mode (int8)
      Byte 1        : Exchange code (int8)
      Bytes 2-5     : Instrument Token (int32)
      Bytes 6-9     : Last Traded Price (int32)
      Bytes 10-13   : Last Traded Time (Unix Time, int32)
      Bytes 14-17   : Last Traded Quantity (int32)
    """
    try:
        if len(data) < 18:
            print("Received data is too short to decode.")
            return {}
        mode = data[0]
        exchange_code = data[1]
        instrument_token = struct.unpack(">I", data[2:6])[0]
        last_traded_price = struct.unpack(">I", data[6:10])[0]
        last_traded_time = struct.unpack(">I", data[10:14])[0]
        last_traded_qty = struct.unpack(">I", data[14:18])[0]

        return {
            "mode": mode,
            "exchange_code": exchange_code,
            "instrument_token": instrument_token,
            "last_traded_price": last_traded_price,
            "last_traded_time": last_traded_time,
            "last_traded_qty": last_traded_qty,
        }
    except Exception as e:
        print("Error decoding message:", e)
        return {}

def on_message(ws, message):
    """
    Callback when a message is received.
    Decodes the binary message and appends it to the shared data buffer.
    """
    decoded = decode_marketdata_message(message)
    if decoded:
        print(f"[DEBUG] Received message: Token {decoded.get('instrument_token')}, "
              f"Price {decoded.get('last_traded_price')}")
        with buffer_lock:
            data_buffer.append(decoded)

def on_error(ws, error):
    print("Error:", error)

def on_close(ws, close_status_code, close_msg):
    print("WebSocket connection closed")

def on_open(ws):
    """
    Callback when the connection opens.
    Sends a subscription message and starts the heartbeat thread.
    """
    print("WebSocket connection opened.")
    subscribe_msg = {"a": "subscribe", "v": instrument_pairs, "m": "marketdata"}
    ws.send(json.dumps(subscribe_msg))
    print("Subscribed to all instruments:")
    print(instrument_pairs)

    def send_heartbeat():
        while True:
            time.sleep(10)
            heartbeat_msg = {"a": "h", "v": [], "m": ""}
            print(f"[DEBUG] Sending heartbeat at {time.strftime('%Y-%m-%d %H:%M:%S')}")
            try:
                ws.send(json.dumps(heartbeat_msg))
            except Exception as e:
                print("Error sending heartbeat:", e)
                break

    heartbeat_thread = threading.Thread(target=send_heartbeat)
    heartbeat_thread.daemon = True
    heartbeat_thread.start()

def process_buffer_periodically():
    global df_market, global_last_prices
    while True:
        print(f"[DEBUG] Waiting 20 seconds for next batch...")
        time.sleep(20)
        with buffer_lock:
            buffer_size = len(data_buffer)
            print(f"[DEBUG] Buffer size before processing: {buffer_size}")
            if data_buffer:
                # Temporary dict for updates during this cycle
                updated_prices = {}
                for msg in data_buffer:
                    token = msg.get("instrument_token")
                    price = msg.get("last_traded_price")
                    updated_prices[token] = price  # Last update in the current batch
                print(f"[DEBUG] Updated prices in current batch: {updated_prices}")
                # Update global_last_prices: if a token has a new update, use it.
                global_last_prices.update(updated_prices)
                data_buffer.clear()
            # If no new updates, global_last_prices remain unchanged

        # Create new snapshot column from the global_last_prices.
        col_name = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        # This ensures that if a token did not get a new update,
        # its previous value is used.
        df_market[col_name] = df_market.index.map(lambda token: global_last_prices.get(token, 0))
        print(f"\nUpdated snapshot at {col_name}:")
        print(df_market)
        df_market.to_excel(excel_file)

buffer_thread = threading.Thread(target=process_buffer_periodically)
buffer_thread.daemon = True
buffer_thread.start()

if __name__ == "__main__":
    ws_app = WebSocketApp(ws_url,
                          on_open=on_open,
                          on_message=on_message,
                          on_error=on_error,
                          on_close=on_close)
    ws_app.run_forever()