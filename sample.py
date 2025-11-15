"""
Session Manager for a Mock Trading Bot (Pure Python)

This module implements a core trading loop using pure Python with threading.
It manages session authentication, retries for initial data fetching, 
continuous price monitoring, order placement, and includes a critical 
order timeout mechanism.

The trading logic transitions between two main states:
1. Price Monitoring: Runs every 5 seconds, looking for trade entry signals.
2. Order Checking: Starts immediately after an order is placed, monitoring 
   the fill status until the order is FILLED or TIMED OUT.
"""
import sys
import io
import time
import datetime
import math
import random
import threading
from threading import Thread, Event, Lock

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

# --- Global Trading Constants ---
# 30 seconds for demo testing (30.0 seconds). Use 1800.0 for production (30 minutes)
ORDER_TIMEOUT_SECONDS = 30.0

# Status codes for internal order tracking
ORDER_STATUS_PENDING = 0
ORDER_STATUS_FILLED = 1
ORDER_STATUS_TIMEOUT = 2

# --- Mock State and API Endpoints ---
# NOTE: Set this to False to test the unauthenticated flow
IS_AUTHENTICATED = True
SESSION_URL = "https://localhost:5000"

# State for SPX retry logic
SPX_ATTEMPTS = 0
SPX_MAX_ATTEMPTS = 12  # 12 attempts * 5 seconds = 60 seconds max retry time
SPX_START_TIME = None

# New Global State for High/Low and Trading State Machine
SPX_HIGH = 0.0
SPX_LOW = 0.0
ORDER_PLACED = False  # Tracks if an initial spread order has been initiated
ORDER_FILLED = False  # Tracks if the initial spread order has been filled
ORDER_ID = None  # Stores the ID of the active order being monitored

# Thread synchronization
state_lock = Lock()
shutdown_event = Event()
trading_stop_event = Event()


def confirm_auth_status():
    """
    Mock function: Confirms the current authentication status.
    Simulates a brief API call.
    """
    print_immediate(f"[{time.strftime('%H:%M:%S')}] Checking authentication status...")
    time.sleep(0.5)  # Simulate network delay
    return IS_AUTHENTICATED


def tickle():
    """
    Mock function: Calls the keep-alive endpoint.
    This runs every 60 seconds to maintain the session.
    """
    print_immediate(f"[{time.strftime('%H:%M:%S')}] Keep-alive: Session refreshed.")
    return "Session Refreshed"


def logout():
    """
    Mock function: Calls the logout endpoint.
    Terminates the simulated session.
    """
    print_immediate(f"[{time.strftime('%H:%M:%S')}] Logout: Session terminated.")


def get_spx_high_low():
    """
    Mock function: Calls the API to get SPX high/low data.
    
    Simulates API failure for the first 3 attempts to demonstrate the retry mechanism.
    If successful, sets the global SPX_HIGH and SPX_LOW values.
    
    Raises:
        ConnectionError: Simulated failure on early attempts.
    """
    global SPX_ATTEMPTS, SPX_HIGH, SPX_LOW, SPX_START_TIME
    
    SPX_ATTEMPTS += 1
    
    if not SPX_START_TIME:
        SPX_START_TIME = datetime.datetime.now()
    
    current_time = datetime.datetime.now()
    time_elapsed = (current_time - SPX_START_TIME).total_seconds()
    
    print_immediate(f"[{time.strftime('%H:%M:%S')}] SPX API attempt #{SPX_ATTEMPTS}. Time elapsed: {time_elapsed:.1f}s")
    
    # Simulate API failure for the first few attempts
    if SPX_ATTEMPTS <= 3:
        raise ConnectionError("Failed to connect to SPX data feed.")
    
    # Successful call: Store the mock values
    SPX_HIGH = 5200.50
    SPX_LOW = 5150.25
    return f"SPX High: ${SPX_HIGH:.2f}, Low: ${SPX_LOW:.2f} (Received after {SPX_ATTEMPTS} attempts)"

# --- New Trading Functions ---

def get_current_price():
    """
    Mock function: Gets the current SPX trading price.
    
    Simulates a dynamic price using a sine wave and random noise to ensure
    price crossings (above high/below low) happen for testing.
    
    Returns:
        float: The simulated current price.
    """
    global SPX_HIGH, SPX_LOW
    
    if SPX_HIGH == 0.0:
        return 0.0  # Safety check if the high/low wasn't set yet

    current_time = datetime.datetime.now()
    t = current_time.timestamp()
    
    # Simulate price movement around the midpoint
    base_price = (SPX_HIGH + SPX_LOW) / 2
    
    # Large swing based on sine wave to simulate crossing limits
    swing = 50.0 * math.sin(t / 10.0)
    
    # Add a small random noise
    noise = random.uniform(-1.0, 1.0)
    
    current_price = base_price + swing + noise
    
    return current_price


def short_put_spread(price):
    """
    Mock function: Places a short put spread order.
    
    Only places the order if ORDER_PLACED is False.
    Returns the mock order ID.
    """
    global ORDER_PLACED, ORDER_ID
    
    with state_lock:
        if not ORDER_PLACED:
            ORDER_PLACED = True
            ORDER_ID = f"PUT_{int(time.time() * 1000)}"
            print_immediate(f"[{time.strftime('%H:%M:%S')}] TRADE ALERT! Price {price:.2f} below Low. Placing Short Put Spread (ID: {ORDER_ID}).")
            return ORDER_ID
        return ORDER_ID


def short_credit_spread(price):
    """
    Mock function: Places a short credit spread order.
    
    Only places the order if ORDER_PLACED is False.
    Returns the mock order ID.
    """
    global ORDER_PLACED, ORDER_ID
    
    with state_lock:
        if not ORDER_PLACED:
            ORDER_PLACED = True
            ORDER_ID = f"CALL_{int(time.time() * 1000)}"
            print_immediate(f"[{time.strftime('%H:%M:%S')}] TRADE ALERT! Price {price:.2f} above High. Placing Short Credit Spread (ID: {ORDER_ID}).")
            return ORDER_ID
        return ORDER_ID

def check_order_status(order_id):
    """
    Checks order status against fill time and timeout duration.
    
    The order is simulated to FILL after 15 seconds.
    The order TIMES OUT after ORDER_TIMEOUT_SECONDS (30.0s in demo).
    
    Args:
        order_id (str): The ID of the active order.
        
    Returns:
        int: One of the ORDER_STATUS constants (PENDING, FILLED, TIMEOUT).
    """
    order_start_timestamp_ms = int(order_id.split('_')[1])
    time_elapsed = (time.time() - (order_start_timestamp_ms / 1000.0))
    
    print_immediate(f"[{time.strftime('%H:%M:%S')}] Checking status for Order ID: {order_id}... Elapsed: {time_elapsed:.1f}s")
    
    # 1. Check for FILLED (Simulated to fill after 15 seconds)
    if time_elapsed >= 15.0 and time_elapsed < ORDER_TIMEOUT_SECONDS:
        global ORDER_FILLED
        with state_lock:
            if not ORDER_FILLED:
                ORDER_FILLED = True
                print_immediate(f"[{time.strftime('%H:%M:%S')}] Order ID: {order_id} is FILLED!")
        return ORDER_STATUS_FILLED
    
    # 2. Check for TIMEOUT
    if time_elapsed >= ORDER_TIMEOUT_SECONDS:
        return ORDER_STATUS_TIMEOUT
    
    # 3. PENDING
    return ORDER_STATUS_PENDING


def cancel_order(order_id):
    """
    Handles order cancellation after a timeout.
    
    Resets the global state flags (ORDER_PLACED, ORDER_ID) to allow the 
    main price monitoring loop to resume immediately.
    """
    global ORDER_PLACED, ORDER_ID
    
    with state_lock:
        # Reset state to allow price monitoring to resume
        ORDER_PLACED = False
        ORDER_ID = None
    
    print_immediate(f"[{time.strftime('%H:%M:%S')}] ORDER TIMEOUT! Order ID: {order_id} not filled after {ORDER_TIMEOUT_SECONDS} sec. Cancelling and resuming price monitoring.")
    return f"Order {order_id} cancelled. Resuming price monitoring."


def place_bracket_order(order_id):
    """
    Mock function: Places the final bracket order after the spread is filled.
    
    Resets all state flags and signals the entire SPX trading loop to stop.
    """
    print_immediate(f"[{time.strftime('%H:%M:%S')}] Initial spread filled. Placing final BRACKET order for {order_id}.")
    
    global ORDER_PLACED, ORDER_FILLED, ORDER_ID
    
    with state_lock:
        # Reset state
        ORDER_PLACED = False
        ORDER_FILLED = False
        ORDER_ID = None
    
    # Signal the continuous trading loop to stop permanently
    trading_stop_event.set()
    
    return f"Bracket Order Placed for {order_id}. Trading loop terminated."


# --- Keep-Alive Thread ---

def keep_alive_thread():
    """
    Background thread that periodically calls the keep-alive endpoint.
    Runs every 60 seconds until the shutdown event is set.
    """
    if not confirm_auth_status():
        print_immediate(f"[{time.strftime('%H:%M:%S')}] Authentication failed. Please open {SESSION_URL} to login.")
        return
    
    print_immediate(f"[{time.strftime('%H:%M:%S')}] Authenticated. Starting infinite keep-alive timer (60s interval).")
    
    while not shutdown_event.is_set():
        tickle()
        
        # Wait for 60 seconds or until shutdown
        if shutdown_event.wait(timeout=60.0):
            break


# --- Order Monitoring Thread ---

def order_check_loop(order_id):
    """
    Background thread that monitors a single order's status.
    
    This thread runs every 5 seconds until the order reaches a terminal state (FILLED or TIMEOUT).
    """
    while not shutdown_event.is_set() and not trading_stop_event.is_set():
        status = check_order_status(order_id)
        
        if status == ORDER_STATUS_FILLED:
            place_bracket_order(order_id)
            break
        elif status == ORDER_STATUS_TIMEOUT:
            cancel_order(order_id)
            break
        
        # Wait 5 seconds before checking again
        if shutdown_event.wait(timeout=5.0):
            break


# --- Price Monitoring Thread ---

def price_monitor_thread():
    """
    Background thread that monitors SPX price and triggers orders.
    
    Runs every 5 seconds checking for trade entry signals until trading stops.
    """
    global ORDER_PLACED, ORDER_ID, SPX_HIGH, SPX_LOW
    
    order_monitor_thread = None
    
    while not shutdown_event.is_set() and not trading_stop_event.is_set():
        current_price = get_current_price()
        
        with state_lock:
            order_placed = ORDER_PLACED
            order_id = ORDER_ID
            high = SPX_HIGH
            low = SPX_LOW
        
        # If an order is already active, make sure the monitor thread is running
        if order_placed and order_id:
            print_immediate(f"[{time.strftime('%H:%M:%S')}] Price Monitoring Tick: Order {order_id} is active. Monitoring fill status.")
            
            if order_monitor_thread is None or not order_monitor_thread.is_alive():
                order_monitor_thread = Thread(target=order_check_loop, args=(order_id,), daemon=True)
                order_monitor_thread.start()
        else:
            # Check for SHORT CREDIT SPREAD (price > high)
            if current_price > high:
                trade_result = short_credit_spread(current_price)
                print_immediate(f"[{time.strftime('%H:%M:%S')}] Trade placed ({trade_result}). Switching to Order Check Loop.")
                
                # Start order monitoring
                order_monitor_thread = Thread(target=order_check_loop, args=(trade_result,), daemon=True)
                order_monitor_thread.start()
            
            # Check for SHORT PUT SPREAD (price < low)
            elif current_price < low:
                trade_result = short_put_spread(current_price)
                print_immediate(f"[{time.strftime('%H:%M:%S')}] Trade placed ({trade_result}). Switching to Order Check Loop.")
                
                # Start order monitoring
                order_monitor_thread = Thread(target=order_check_loop, args=(trade_result,), daemon=True)
                order_monitor_thread.start()
            else:
                print_immediate(f"[{time.strftime('%H:%M:%S')}] Price monitoring: {current_price:.2f}")
        
        # Wait 5 seconds before next check
        if shutdown_event.wait(timeout=5.0):
            break


# --- SPX Fetch Thread with Retry ---

def spx_fetch_with_retry():
    """
    Background thread that fetches SPX data with retry logic.
    
    Attempts to fetch SPX high/low data, retrying up to 60 seconds.
    On success, starts the price monitoring thread.
    """
    global SPX_ATTEMPTS, SPX_START_TIME
    
    SPX_START_TIME = datetime.datetime.now()
    
    while not shutdown_event.is_set():
        try:
            result = get_spx_high_low()
            print_immediate(f"[{time.strftime('%H:%M:%S')}] Initial SPX Data Found: {result}. Starting 5s Trading Loop.")
            
            # Start price monitoring thread
            price_monitor = Thread(target=price_monitor_thread, daemon=True)
            price_monitor.start()
            return
            
        except ConnectionError as e:
            current_time = datetime.datetime.now()
            time_elapsed = (current_time - SPX_START_TIME).total_seconds()
            
            # Check if we've exceeded the limits
            if time_elapsed >= 60.0 or SPX_ATTEMPTS >= SPX_MAX_ATTEMPTS:
                print_immediate(f"[{time.strftime('%H:%M:%S')}] SPX Data Final Error: Retries exhausted after {SPX_ATTEMPTS} attempts and {time_elapsed:.1f}s.")
                return
            
            print_immediate(f"[{time.strftime('%H:%M:%S')}] SPX fetch failed. Retrying in 5 seconds...")
            
            # Wait 5 seconds before retrying
            if shutdown_event.wait(timeout=5.0):
                return


# --- Main Execution ---

def main():
    print_immediate("="*60)
    print_immediate("SESSION MANAGER - Mock Trading Bot")
    print_immediate("="*60)
    print_immediate("Starting session manager. Keep-alive runs every 60 seconds if authenticated.\n")
    
    # Start keep-alive thread
    keepalive = Thread(target=keep_alive_thread, daemon=True)
    keepalive.start()
    
    # Schedule SPX fetch to start in 10 seconds (for demo)
    print_immediate(f"[{time.strftime('%H:%M:%S')}] Scheduling SPX fetch to start in 10.0 seconds.")
    
    # Wait for initial delay
    if shutdown_event.wait(timeout=10.0):
        return
    
    # Start SPX fetch thread
    spx_fetch = Thread(target=spx_fetch_with_retry, daemon=True)
    spx_fetch.start()
    
    # Wait for user to exit
    try:
        print_immediate("\n" + "="*60)
        print_immediate("Manager running. Press Ctrl+C to log out and exit...")
        print_immediate("="*60 + "\n")
        
        while True:
            time.sleep(1)
            
    except KeyboardInterrupt:
        print_immediate(f"\n[{time.strftime('%H:%M:%S')}] Shutdown initiated...")
        
        # Signal all threads to stop
        shutdown_event.set()
        trading_stop_event.set()
        
        # Call logout
        logout()
        
        # Give threads time to clean up
        time.sleep(1)
        
        print_immediate(f"[{time.strftime('%H:%M:%S')}] Exiting session manager.")


if __name__ == "__main__":
    main()
