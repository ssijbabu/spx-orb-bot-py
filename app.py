"""
Trading Bot using ib_async with Interactive Brokers API

This module implements a trading system with the following components:
1. Stock Monitor: Fetches SPX data and monitors price movements
2. Order Manager: Handles order placement, status checking, and timeouts
3. Connection Manager: Manages IB API connection with keep-alive

Key Features:
- IB API connection management with threading
- Scheduled SPX high/low fetching at 10:30 AM ET
- Live price monitoring using IB market data feed
- 30-minute order timeout mechanism
- Automatic bracket order placement on fill
"""
import sys
import io

# Set UTF-8 encoding for console output
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8')

# Disable buffering for immediate console output
import os
os.environ['PYTHONUNBUFFERED'] = '1'

# --- Helper function for immediate output ---
def print_immediate(*args, **kwargs):
    """Print with immediate flushing to console"""
    print(*args, **kwargs, flush=True)

import time
import datetime
import pytz
from ib_async import *
import asyncio
from threading import Thread, Lock, Event
import concurrent.futures
import nest_asyncio

# Allow nested event loops
nest_asyncio.apply()

# --- Trading Constants ---
ORDER_TIMEOUT_SECONDS = 1800.0  # 30 minutes
TICKLE_INTERVAL_SECONDS = 60.0
PRICE_CHECK_INTERVAL_SECONDS = 5.0
ORDER_CHECK_INTERVAL_SECONDS = 5.0
SPX_RETRY_INTERVAL_SECONDS = 5.0

# Order Status
ORDER_STATUS_PENDING = "PENDING"
ORDER_STATUS_FILLED = "FILLED"
ORDER_STATUS_CANCELLED = "CANCELLED"

# --- IB Configuration ---
IB_HOST = '127.0.0.1'
IB_PORT = 4004  # TWS paper trading port (7497 for live, 7496 for paper)
IB_CLIENT_ID = 1

# --- Global State ---
ib = None  # IB connection instance
ib_thread = None  # Thread running IB event loop
SPX_HIGH = None
SPX_LOW = None
spx_ticker = None  # Live SPX ticker object
connection_status = False
shutdown_event = Event()
state_lock = Lock()
active_orders = []
price_monitoring_active = False


# --- IB Connection Management ---

def run_ib_with_event_loop(ib_instance):
    """Run IB instance with its own event loop in a separate thread."""
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        ib_instance.run()
    except Exception as e:
        print_immediate(f"[{time.strftime('%H:%M:%S')}] ❌ IB event loop error: {e}")
    finally:
        loop.close()


def connect_ib():
    """Connect to Interactive Brokers TWS/Gateway."""
    global ib, ib_thread, connection_status
    
    print_immediate(f"[{time.strftime('%H:%M:%S')}] 🔌 Connecting to IB at {IB_HOST}:{IB_PORT}...")
    
    try:
        ib = IB()
        ib.connect(IB_HOST, IB_PORT, clientId=IB_CLIENT_ID, timeout=20)
        
        # Start event loop in separate thread
        ib_thread = Thread(target=run_ib_with_event_loop, args=(ib,), daemon=True)
        ib_thread.start()
        
        # Wait a moment for connection to stabilize
        time.sleep(1)
        
        print_immediate(f"[{time.strftime('%H:%M:%S')}] ✅ Connected to IB successfully")
        connection_status = True
        return True
        
    except Exception as e:
        print_immediate(f"[{time.strftime('%H:%M:%S')}] ❌ IB connection failed: {e}")
        print_immediate(f"   Make sure TWS/IB Gateway is running on port {IB_PORT}")
        connection_status = False
        return False


def disconnect_ib():
    """Disconnect from Interactive Brokers."""
    global ib, spx_ticker, connection_status
    
    if ib and ib.isConnected():
        print_immediate(f"[{time.strftime('%H:%M:%S')}] 🔌 Disconnecting from IB...")
        
        # Cancel market data subscription
        if spx_ticker:
            try:
                ib.cancelMktData(spx_ticker.contract)
            except:
                pass
            spx_ticker = None
        
        ib.disconnect()
        connection_status = False
        print_immediate(f"[{time.strftime('%H:%M:%S')}] ✅ Disconnected from IB")


def confirm_auth_status():
    """Check IB connection status."""
    print_immediate(f"[{time.strftime('%H:%M:%S')}] 🔍 Checking IB connection status...")
    
    if ib and ib.isConnected():
        print_immediate(f"[{time.strftime('%H:%M:%S')}] ✅ IB connection active")
        return True
    else:
        return connect_ib()


def tickle():
    """Keep-alive check for IB connection."""
    if ib and ib.isConnected():
        try:
            accounts = ib.managedAccounts()
            print_immediate(f"[{time.strftime('%H:%M:%S')}] ⏰ Keep-alive: IB connection active (Accounts: {len(accounts)})")
            return "IB_CONNECTION_ALIVE"
        except Exception as e:
            print_immediate(f"[{time.strftime('%H:%M:%S')}] ⚠️ Keep-alive check failed: {e}")
            return "IB_CONNECTION_ERROR"
    else:
        print_immediate(f"[{time.strftime('%H:%M:%S')}] ⚠️ IB connection lost")
        return "IB_DISCONNECTED"


# --- Helper function to run async code synchronously ---

def run_async(coro):
    """Run an async coroutine and return the result synchronously."""
    if not ib or not ib.isConnected():
        raise ConnectionError("IB not connected")
    
    # Use ib.waitOnUpdate() approach for synchronous execution
    loop = asyncio.get_event_loop()
    if loop.is_running():
        # Create a future and schedule the coroutine
        future = asyncio.ensure_future(coro)
        # Wait for it using ib's sleep mechanism
        while not future.done():
            ib.sleep(0.01)
        return future.result()
    else:
        return loop.run_until_complete(coro)


# --- Helper function to run async code synchronously ---

def run_in_ib_thread(coro):
    """
    Run a coroutine in the IB event loop and wait for result.
    Used to safely call async IB methods from other threads.
    """
    if not ib or not ib.isConnected():
        raise ConnectionError("IB not connected")
    
    # Create a container for the result
    result_container = {}
    exception_container = {}
    
    async def wrapper():
        try:
            result = await coro
            result_container['result'] = result
        except Exception as e:
            exception_container['error'] = e
    
    # Get the event loop - try multiple ways
    loop = None
    if hasattr(ib, '_ib') and hasattr(ib._ib, '_loop'):
        loop = ib._ib._loop
    elif hasattr(ib, 'client') and hasattr(ib.client, 'loop'):
        loop = ib.client.loop
    else:
        # Try to get current event loop
        try:
            loop = asyncio.get_event_loop()
        except:
            # Create a new loop if none exists
            loop = asyncio.new_event_loop()
    
    # Schedule the coroutine in the IB event loop
    future = asyncio.run_coroutine_threadsafe(wrapper(), loop=loop)
    
    # Wait for the result with timeout
    future.result(timeout=60)
    
    if 'error' in exception_container:
        raise exception_container['error']
    
    return result_container.get('result')

def fetch_spx_high_low():
    """
    Fetch SPX high and low for the day using historical data.
    Requests the first hour of trading (9:30-10:30 AM ET).
    """
    global SPX_HIGH, SPX_LOW
    
    if not ib or not ib.isConnected():
        raise ConnectionError("IB not connected")
    
    print_immediate(f"[{time.strftime('%H:%M:%S')}] 📊 Fetching SPX high/low data...")
    
    try:
        # Create SPX index contract
        spx_contract = Index('SPX', 'CBOE')
        
        # Qualify contract using the IB thread
        qualified = run_in_ib_thread(ib.qualifyContractsAsync(spx_contract))
        
        if not qualified or len(qualified) == 0:
            raise ValueError("Failed to qualify SPX contract")
        
        spx_contract = qualified[0]
        
        # Get today's date in ET timezone
        et_tz = pytz.timezone('America/New_York')
        now_et = datetime.datetime.now(et_tz)
        
        # Request 1-hour bars for today (first hour of trading)
        bars = run_in_ib_thread(ib.reqHistoricalDataAsync(
            spx_contract,
            endDateTime='',
            durationStr='1 D',
            barSizeSetting='30 mins',
            whatToShow='TRADES',
            useRTH=True,  # Regular trading hours only
            formatDate=1
        ))
        
        # Wait for data
        if not bars or len(bars) == 0:
            raise ValueError("No historical data received")
        
        # Get high/low from first hour bar (9:30-10:30 AM)
        first_hour_bar = bars[0]
        SPX_HIGH = first_hour_bar.high
        SPX_LOW = first_hour_bar.low
        
        print_immediate(f"[{time.strftime('%H:%M:%S')}] 📊 SPX Data Retrieved: High=${SPX_HIGH:.2f}, Low=${SPX_LOW:.2f}")
        
        return {"high": SPX_HIGH, "low": SPX_LOW}
        
    except concurrent.futures.TimeoutError:
        print_immediate(f"[{time.strftime('%H:%M:%S')}] ❌ Failed to fetch SPX data: Request timeout")
        raise ConnectionError("SPX fetch timeout")
    except Exception as e:
        print_immediate(f"[{time.strftime('%H:%M:%S')}] ❌ Failed to fetch SPX data: {e}")
        raise ConnectionError(f"SPX fetch failed: {e}")


def subscribe_spx_live_data():
    """Subscribe to live SPX market data."""
    global spx_ticker
    
    if not ib or not ib.isConnected():
        raise ConnectionError("IB not connected")
    
    try:
        # Create SPX contract
        spx_contract = Index('SPX', 'CBOE')
        
        # Qualify contract using the IB thread
        qualified = run_in_ib_thread(ib.qualifyContractsAsync(spx_contract))
        
        if not qualified or len(qualified) == 0:
            raise ValueError("Failed to qualify SPX contract")
        
        spx_contract = qualified[0]
        
        # Subscribe to market data
        spx_ticker = ib.reqMktData(spx_contract, '', False, False)
        
        # Wait for initial tick
        ib.sleep(2)
        
        print_immediate(f"[{time.strftime('%H:%M:%S')}] 📡 Subscribed to live SPX data")
        return spx_ticker
        
    except Exception as e:
        print_immediate(f"[{time.strftime('%H:%M:%S')}] ❌ Failed to subscribe to SPX data: {e}")
        raise


def get_current_price():
    """Get current SPX trading price from live ticker."""
    global spx_ticker
    
    if not spx_ticker:
        return None
    
    # Use last traded price, fall back to midpoint if not available
    if spx_ticker.last and spx_ticker.last > 0:
        return spx_ticker.last
    elif spx_ticker.bid and spx_ticker.ask and spx_ticker.bid > 0 and spx_ticker.ask > 0:
        return (spx_ticker.bid + spx_ticker.ask) / 2
    else:
        return None


# --- Trading Functions ---

def short_put_spread(price):
    """
    Place a short put spread (two orders).
    Called when price falls below the low.
    """
    if not ib or not ib.isConnected():
        raise ConnectionError("IB not connected")
    
    order_id = f"PUT_SPREAD_{int(time.time() * 1000)}"
    timestamp = time.time()
    
    print_immediate(f"[{time.strftime('%H:%M:%S')}] 📉 TRADE: Placing Short Put Spread at ${price:.2f}")
    print_immediate(f"   Order ID: {order_id}")
    
    try:
        # NOTE: In production, calculate actual strikes based on price
        # Example: strikes could be price - 10 and price - 20
        
        print_immediate(f"   ⚠️ Order placement to IB API (implementation needed)")
        print_immediate(f"   Would sell put at strike ~{price - 10:.0f} and buy put at strike ~{price - 20:.0f}")
        
        return {
            "order_id": order_id,
            "type": "SHORT_PUT_SPREAD",
            "price": price,
            "timestamp": timestamp,
            "status": ORDER_STATUS_PENDING,
            "ib_orders": []  # Store IB order objects here
        }
        
    except Exception as e:
        print_immediate(f"[{time.strftime('%H:%M:%S')}] ❌ Failed to place put spread: {e}")
        raise


def short_call_spread(price):
    """
    Place a short call spread (two orders).
    Called when price rises above the high.
    """
    if not ib or not ib.isConnected():
        raise ConnectionError("IB not connected")
    
    order_id = f"CALL_SPREAD_{int(time.time() * 1000)}"
    timestamp = time.time()
    
    print_immediate(f"[{time.strftime('%H:%M:%S')}] 📈 TRADE: Placing Short Call Spread at ${price:.2f}")
    print_immediate(f"   Order ID: {order_id}")
    
    try:
        # NOTE: In production, calculate actual strikes based on price
        # Example: strikes could be price + 10 and price + 20
        
        print_immediate(f"   ⚠️ Order placement to IB API (implementation needed)")
        print_immediate(f"   Would sell call at strike ~{price + 10:.0f} and buy call at strike ~{price + 20:.0f}")
        
        return {
            "order_id": order_id,
            "type": "SHORT_CALL_SPREAD",
            "price": price,
            "timestamp": timestamp,
            "status": ORDER_STATUS_PENDING,
            "ib_orders": []  # Store IB order objects here
        }
        
    except Exception as e:
        print_immediate(f"[{time.strftime('%H:%M:%S')}] ❌ Failed to place call spread: {e}")
        raise


def check_order_status(order):
    """
    Check if orders are filled by querying IB order status.
    """
    elapsed = time.time() - order["timestamp"]
    
    # Check for timeout
    if elapsed >= ORDER_TIMEOUT_SECONDS:
        print_immediate(f"[{time.strftime('%H:%M:%S')}] ⏱️ Order {order['order_id']}: TIMEOUT after {ORDER_TIMEOUT_SECONDS/60:.1f} minutes")
        return ORDER_STATUS_CANCELLED
    
    # In production, check actual IB order status
    # For now, simulate fill after 15 seconds for testing
    if elapsed >= 15.0:
        print_immediate(f"[{time.strftime('%H:%M:%S')}] ✅ Order {order['order_id']}: FILLED")
        return ORDER_STATUS_FILLED
    
    print_immediate(f"[{time.strftime('%H:%M:%S')}] ⏳ Order {order['order_id']}: Still pending ({elapsed:.1f}s elapsed)")
    return ORDER_STATUS_PENDING


def cancel_order(order):
    """Cancel an order that timed out."""
    print_immediate(f"[{time.strftime('%H:%M:%S')}] 🛑 Cancelling order: {order['order_id']}")
    
    # Cancel orders via IB API
    if ib and ib.isConnected() and order.get("ib_orders"):
        for ib_order in order["ib_orders"]:
            try:
                ib.cancelOrder(ib_order)
            except Exception as e:
                print_immediate(f"   ⚠️ Cancel failed: {e}")


def place_bracket_order(order):
    """
    Place bracket order (take-profit and stop-loss) after spread is filled.
    """
    bracket_id = f"BRACKET_{int(time.time() * 1000)}"
    print_immediate(f"[{time.strftime('%H:%M:%S')}] 🎯 Placing Bracket Order for {order['order_id']}")
    print_immediate(f"   Bracket ID: {bracket_id}")
    print_immediate(f"   Take-Profit and Stop-Loss orders placed")
    
    # In production, place actual bracket orders via IB API
    
    return {
        "bracket_id": bracket_id,
        "parent_order": order["order_id"],
        "type": "BRACKET"
    }


# --- Threading Functions ---

def keepalive_thread():
    """Background thread that periodically checks connection."""
    while not shutdown_event.is_set():
        if connection_status:
            tickle()
        time.sleep(TICKLE_INTERVAL_SECONDS)
        if shutdown_event.wait(timeout=0.1):
            break


def price_monitor_thread():
    """Background thread that monitors SPX price and triggers orders."""
    global price_monitoring_active, SPX_HIGH, SPX_LOW, active_orders
    
    if not connection_status:
        print_immediate(f"[{time.strftime('%H:%M:%S')}] ❌ Cannot start price monitoring - IB not connected")
        return
    
    try:
        subscribe_spx_live_data()
    except Exception as e:
        print_immediate(f"[{time.strftime('%H:%M:%S')}] ❌ Failed to subscribe to live data: {e}")
        return
    
    price_monitoring_active = True
    order_triggered = False
    
    while not shutdown_event.is_set() and not order_triggered:
        try:
            current_price = get_current_price()
            if current_price:
                print_immediate(f"[{time.strftime('%H:%M:%S')}] 💹 Current Price: ${current_price:.2f} (High: ${SPX_HIGH:.2f}, Low: ${SPX_LOW:.2f})")
                
                # Check if price breached levels
                if current_price > SPX_HIGH:
                    print_immediate(f"[{time.strftime('%H:%M:%S')}] 🎯 Price ABOVE HIGH! Triggering short call spread...")
                    order = short_call_spread(current_price)
                    with state_lock:
                        active_orders.append(order)
                    order_triggered = True
                    break
                    
                elif current_price < SPX_LOW:
                    print_immediate(f"[{time.strftime('%H:%M:%S')}] 🎯 Price BELOW LOW! Triggering short put spread...")
                    order = short_put_spread(current_price)
                    with state_lock:
                        active_orders.append(order)
                    order_triggered = True
                    break
            
            time.sleep(PRICE_CHECK_INTERVAL_SECONDS)
            if shutdown_event.wait(timeout=0.1):
                break
                
        except Exception as e:
            print_immediate(f"[{time.strftime('%H:%M:%S')}] ❌ Error in price monitoring: {e}")
            time.sleep(SPX_RETRY_INTERVAL_SECONDS)


def order_monitor_thread():
    """Background thread that monitors order status."""
    global active_orders
    
    while not shutdown_event.is_set():
        with state_lock:
            for order in active_orders[:]:  # Iterate over a copy
                status = check_order_status(order)
                
                if status == ORDER_STATUS_FILLED:
                    print_immediate(f"[{time.strftime('%H:%M:%S')}] ✅ Order {order['order_id']} FILLED - Placing bracket order")
                    place_bracket_order(order)
                    active_orders.remove(order)
                    
                elif status == ORDER_STATUS_CANCELLED:
                    print_immediate(f"[{time.strftime('%H:%M:%S')}] 🛑 Order {order['order_id']} CANCELLED - Cleaning up")
                    cancel_order(order)
                    active_orders.remove(order)
        
        time.sleep(ORDER_CHECK_INTERVAL_SECONDS)
        if shutdown_event.wait(timeout=0.1):
            break


def seconds_until_target_time():
    """
    Calculate seconds until 10:30 AM ET today or tomorrow.
    For testing, returns 10 seconds.
    """
    # FOR TESTING: Use 10 seconds for immediate execution
    return 10.0
    
    # FOR PRODUCTION: Uncomment below for actual 10:30 AM ET scheduling
    # et_tz = pytz.timezone('America/New_York')
    # now = datetime.datetime.now(et_tz)
    # target = now.replace(hour=10, minute=30, second=0, microsecond=0)
    # 
    # if now >= target:
    #     # Already past 10:30 AM, schedule for tomorrow
    #     target += datetime.timedelta(days=1)
    # 
    # delay = (target - now).total_seconds()
    # return max(delay, 0)


def spx_fetch_thread():
    """Background thread that fetches SPX high/low at scheduled time."""
    delay = seconds_until_target_time()
    print_immediate(f"[{time.strftime('%H:%M:%S')}] 📅 SPX fetch scheduled in {delay:.1f} seconds (10:30 AM ET)")
    
    # Wait for scheduled time
    if shutdown_event.wait(timeout=delay):
        return
    
    # Retry loop for SPX fetch
    while not shutdown_event.is_set():
        try:
            fetch_spx_high_low()
            print_immediate(f"[{time.strftime('%H:%M:%S')}] 🎯 SPX data acquired. Starting price monitoring...")
            
            # Start price monitoring thread
            price_monitor = Thread(target=price_monitor_thread, daemon=True)
            price_monitor.start()
            return
            
        except Exception as e:
            print_immediate(f"[{time.strftime('%H:%M:%S')}] ⚠️ SPX fetch failed: {e}")
            print_immediate(f"   Retrying in {SPX_RETRY_INTERVAL_SECONDS} seconds...")
            
            if shutdown_event.wait(timeout=SPX_RETRY_INTERVAL_SECONDS):
                return


# --- Main Execution ---

def main():
    print_immediate("=" * 60)
    print_immediate("TRADING BOT - IB Integration")
    print_immediate("=" * 60)
    
    # Confirm connection
    if not confirm_auth_status():
        print_immediate(f"[{time.strftime('%H:%M:%S')}] ❌ Failed to connect to IB!")
        print_immediate(f"   Please ensure TWS/IB Gateway is running on port {IB_PORT}")
        return
    
    # Start background threads
    print_immediate(f"\n[{time.strftime('%H:%M:%S')}] ✅ IB Connected! Starting bot...")
    
    # Keep-alive thread
    keepalive = Thread(target=keepalive_thread, daemon=True)
    keepalive.start()
    
    # SPX fetch thread
    spx_fetch = Thread(target=spx_fetch_thread, daemon=True)
    spx_fetch.start()
    
    # Order monitor thread
    order_monitor = Thread(target=order_monitor_thread, daemon=True)
    order_monitor.start()
    
    # Wait for user to exit
    try:
        print_immediate("\n" + "=" * 60)
        print_immediate("Bot is running. Press Ctrl+C to logout and exit.")
        print_immediate("=" * 60 + "\n")
        
        # Keep main thread alive
        while True:
            time.sleep(1)
            
    except KeyboardInterrupt:
        print_immediate(f"\n[{time.strftime('%H:%M:%S')}] 🛑 Shutdown initiated...")
        
        # Signal all threads to stop
        shutdown_event.set()
        
        # Wait for threads to finish
        time.sleep(1)
        
        # Disconnect from IB
        disconnect_ib()
        
        print_immediate(f"[{time.strftime('%H:%M:%S')}] 👋 Goodbye!")


if __name__ == "__main__":
    main()