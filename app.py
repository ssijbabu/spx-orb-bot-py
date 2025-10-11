"""
Reactive Session Manager for a Mock Trading Bot (rxPy)

This module implements a core trading loop using ReactiveX (rxPy) observables.
It manages session authentication, retries for initial data fetching, 
continuous price monitoring, order placement, and includes a critical 
order timeout mechanism.

The trading logic transitions between two main states:
1. Price Monitoring: Runs every 5 seconds, looking for trade entry signals.
2. Order Checking: Starts immediately after an order is placed, monitoring 
   the fill status until the order is FILLED or TIMED OUT.
"""
import rx
from rx import operators as ops
from rx.subject import Subject
import time
import datetime
import math
import random

# --- Global Trading Constants ---
# 30 minutes (1800.0) in a real application. Set to 30.0 seconds for demo testing.
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
SPX_MAX_ATTEMPTS = 12 # 12 attempts * 5 seconds = 60 seconds max retry time
SPX_START_TIME = None

# New Global State for High/Low and Trading State Machine
SPX_HIGH = 0.0
SPX_LOW = 0.0
ORDER_PLACED = False # Tracks if an initial spread order has been initiated (Primary state flag)
ORDER_FILLED = False # Tracks if the initial spread order has been filled (Secondary flag for processing)
ORDER_ID = None # Stores the ID of the active order being monitored

def confirm_auth_status(scheduler):
    """
    Mock function: Confirms the current authentication status.
    Simulates a brief API call.
    """
    print(f"[{time.strftime('%H:%M:%S')}] 🔍 Calling confirm_auth_status()...")
    time.sleep(0.5) # Simulate network delay
    return IS_AUTHENTICATED

def tickle():
    """
    Mock function: Calls the keep-alive endpoint.
    This runs inside the reactive stream every 60 seconds to maintain the session.
    """
    print(f"[{time.strftime('%H:%M:%S')}] ⏳ Tickle endpoint called. Session refreshed.")
    return "Session Refreshed"

def logout():
    """
    Mock function: Calls the logout endpoint.
    Terminates the simulated session.
    """
    print(f"[{time.strftime('%H:%M:%S')}] 🚪 Calling logout endpoint. Session terminated.")
    # In a real application, this would send an API request to invalidate the session token.

def get_spx_high_low():
    """
    Mock function: Calls the API to get SPX high/low data.
    
    Simulates API failure for the first 3 attempts to demonstrate the retry mechanism.
    If successful, sets the global SPX_HIGH and SPX_LOW values.
    
    Raises:
        ConnectionError: Simulated failure on early attempts.
    """
    global SPX_ATTEMPTS, SPX_HIGH, SPX_LOW
    SPX_ATTEMPTS += 1
    
    current_time = datetime.datetime.now()
    # Ensure SPX_START_TIME is initialized before calculating time_elapsed
    time_elapsed = (current_time - SPX_START_TIME).total_seconds() if SPX_START_TIME else 0
    
    print(f"[{time.strftime('%H:%M:%S')}] 📈 SPX API attempt #{SPX_ATTEMPTS}. Time elapsed: {time_elapsed:.1f}s")
    
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
        return 0.0 # Safety check if the high/low wasn't set yet

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
    if not ORDER_PLACED:
        ORDER_PLACED = True
        ORDER_ID = f"PUT_{int(time.time() * 1000)}"
        print(f"[{time.strftime('%H:%M:%S')}] 📉 TRADE ALERT! Price {price:.2f} below Low. Placing Short Put Spread (ID: {ORDER_ID}).")
        return ORDER_ID
    return ORDER_ID

def short_credit_spread(price):
    """
    Mock function: Places a short credit spread order.
    
    Only places the order if ORDER_PLACED is False.
    Returns the mock order ID.
    """
    global ORDER_PLACED, ORDER_ID
    if not ORDER_PLACED:
        ORDER_PLACED = True
        ORDER_ID = f"CALL_{int(time.time() * 1000)}"
        print(f"[{time.strftime('%H:%M:%S')}] 📈 TRADE ALERT! Price {price:.2f} above High. Placing Short Credit Spread (ID: {ORDER_ID}).")
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
    
    print(f"[{time.strftime('%H:%M:%S')}] 🔎 Checking status for Order ID: {order_id}... Elapsed: {time_elapsed:.1f}s")

    # 1. Check for FILLED (Simulated to fill after 15 seconds)
    if time_elapsed >= 15.0 and time_elapsed < ORDER_TIMEOUT_SECONDS: 
        global ORDER_FILLED
        if not ORDER_FILLED:
            ORDER_FILLED = True
            print(f"[{time.strftime('%H:%M:%S')}] ✅ Order ID: {order_id} is FILLED!")
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
    # Reset state to allow price monitoring to resume
    ORDER_PLACED = False
    ORDER_ID = None 
    
    print(f"[{time.strftime('%H:%M:%S')}] 🛑 ORDER TIMEOUT! Order ID: {order_id} not filled after {ORDER_TIMEOUT_SECONDS} sec. Cancelling and resuming price monitoring.")
    
    # This return value is emitted back to the main stream, completing the cycle.
    return f"Order {order_id} cancelled. Resuming price monitoring."


def place_bracket_order(order_id):
    """
    Mock function: Places the final bracket order after the spread is filled.
    
    Resets all state flags and signals the entire SPX trading loop to stop.
    """
    print(f"[{time.strftime('%H:%M:%S')}] 🚀 Initial spread filled. Placing final BRACKET order for {order_id}.")
    global ORDER_PLACED, ORDER_FILLED, ORDER_ID
    
    # Reset state
    ORDER_PLACED = False
    ORDER_FILLED = False
    ORDER_ID = None
    
    # Signal the continuous trading loop to stop permanently
    trading_stop_signal.on_next(True)
    trading_stop_signal.on_completed()

    return f"Bracket Order Placed for {order_id}. Trading loop terminated."

# --- Global Signals ---
# Subject used to terminate the keep-alive flow on user logout/exit
logout_signal = Subject()
# Subject used to terminate the main SPX trading loop after a bracket order is placed
trading_stop_signal = Subject() 


# --- Reactive Stream Definition: Session Keep-Alive ---

def create_keep_alive_observable():
    """
    Creates the continuous observable for session keep-alive.
    
    - Runs a one-time auth check using rx.defer.
    - If successful, switches to an rx.interval(60.0) that calls the tickle function.
    - Uses ops.take_until(logout_signal) to ensure clean termination.
    """
    def deferred_auth_check(scheduler):
        if not confirm_auth_status(scheduler):
            return rx.just(f"Authentication failed. Please open {SESSION_URL} to login and then rerun the program.")
        else:
            print(f"[{time.strftime('%H:%M:%S')}] ✅ Authenticated. Starting infinite keep-alive timer (60s interval).")
            
            keep_alive_observable = rx.interval(60.0).pipe(
                ops.start_with(-1), # Execute immediately upon subscription
                ops.flat_map(lambda _: rx.just(tickle())),
                ops.take_until(logout_signal) # Stop the stream when the logout signal fires
            )
            return keep_alive_observable

    return rx.defer(deferred_auth_check)


# --- Helper Function for Trading Loop Logic ---

def create_order_check_loop(order_id):
    """
    Creates a temporary, inner stream that monitors a single order's status.
    
    This stream runs every 5 seconds until the order reaches a terminal state (FILLED or TIMEOUT).
    
    Args:
        order_id (str): The ID of the order to monitor.
        
    Returns:
        Observable: Emits the result of `place_bracket_order` (on fill) or 
                    `cancel_order` (on timeout), then completes.
    """
    return rx.interval(5.0).pipe(
        ops.start_with(-1), # Check immediately
        ops.map(lambda _: check_order_status(order_id)),
        
        # Filter to only pass through terminal states (FILLED or TIMEOUT)
        ops.filter(lambda status: status != ORDER_STATUS_PENDING), 
        ops.take(1), # Take the first terminal state and complete the interval
        
        # Handle the terminal state transition
        ops.flat_map(lambda status: 
            # If filled, place bracket order and stop all trading (signal to main loop)
            rx.just(place_bracket_order(order_id)) if status == ORDER_STATUS_FILLED
            # If timed out, cancel order and resume price monitoring (emits result and completes this inner loop)
            else rx.just(cancel_order(order_id))
        )
    )

def process_price_emission(price):
    """
    Core state machine logic executed on every 5-second tick of the main trading loop.
    
    It branches the stream based on the global state (ORDER_PLACED).
    
    Args:
        price (float): The current market price.
        
    Returns:
        Observable: Either a new `create_order_check_loop` stream or a simple 
                    monitoring message.
    """
    global ORDER_PLACED, ORDER_ID, SPX_HIGH, SPX_LOW

    # 1. Order Checking State (An order is already active, re-route to order fill check)
    if ORDER_PLACED and ORDER_ID:
        # This occurs if the main 5s interval fires while the order is still pending.
        print(f"[{time.strftime('%H:%M:%S')}] 🔄 Price Monitoring Tick: Order {ORDER_ID} is active. Resuming fill check loop.")
        # Return the inner order checking loop to continue monitoring
        return create_order_check_loop(ORDER_ID)
    
    # 2. Price Monitoring/Trade Placement State (No order is currently active)
    else:
        trade_result = None
        
        # Check for SHORT CREDIT SPREAD (price > high)
        if price > SPX_HIGH:
            trade_result = short_credit_spread(price)
        
        # Check for SHORT PUT SPREAD (price < low)
        elif price < SPX_LOW:
            trade_result = short_put_spread(price)

        if trade_result:
            # TRADE PLACED: Immediately switch the stream to checking the order fill status.
            print(f"[{time.strftime('%H:%M:%S')}] 🚨 Trade placed ({trade_result}). Switching immediately to Order Check Loop.")
            # Start the order checking stream
            return create_order_check_loop(trade_result)
        else:
            # No trade placed, continue price monitoring
            return rx.just(f"Price monitoring: {price:.2f}")


# --- Reactive Stream Definition: SPX Trading Loop (New) ---

def get_spx_trade_loop():
    """
    Creates a continuous observable that runs the core trading logic.
    
    - Runs every 5 seconds using rx.interval.
    - Uses ops.flat_map to call `process_price_emission`, which handles the 
      state transition (monitoring -> order check -> monitoring/stop).
    - Stops the entire loop when the `trading_stop_signal` is emitted.
    """
    # Main price monitoring stream (runs every 5 seconds)
    return rx.interval(5.0).pipe(
        ops.start_with(-1), # Check price immediately
        ops.map(lambda _: get_current_price()), # Get the current price
        # Use the named helper function to handle the complex state logic
        ops.flat_map(process_price_emission),
        # Stop the entire loop when the bracket order is placed
        ops.take_until(trading_stop_signal)
    )

# --- Reactive Stream Definition: SPX Scheduled Task with Retry ---

def get_spx_observable_with_retry():
    """
    Core observable logic that attempts to fetch SPX data and implements the retry policy.
    
    Uses a recursive definition with ops.catch to handle connection errors 
    and retry attempts up to a 60-second limit.
    
    Returns:
        Observable: Emits the SPX data result on success or an error on failure.
    """
    global SPX_START_TIME
    global SPX_ATTEMPTS

    # 1. Start the 60 second clock on the very first attempt
    if not SPX_START_TIME:
        SPX_START_TIME = datetime.datetime.now()

    def attempt_fetch_logic(scheduler):
        """Observable that attempts to call the API."""
        return rx.just(get_spx_high_low())
    
    # 2. Define the error handler function (catch)
    def handle_error(error, source_observable):
        """Checks retry conditions and schedules the next attempt or terminates."""
        current_time = datetime.datetime.now()
        time_elapsed = (current_time - SPX_START_TIME).total_seconds()

        # Check if we've exceeded the limits
        if time_elapsed >= 60.0 or SPX_ATTEMPTS >= SPX_MAX_ATTEMPTS:
            print(f"[{time.strftime('%H:%M:%S')}] ❌ SPX Data Final Error: Retries exhausted after {SPX_ATTEMPTS} attempts and {time_elapsed:.1f}s.")
            # Return rx.throw to propagate the error and terminate the stream
            return rx.throw(error)

        # Log and schedule the next attempt
        print(f"[{time.strftime('%H:%M:%S')}] 🚨 SPX fetch failed. Retrying in 5 seconds...")
        
        # Schedule the next attempt recursively after 5 seconds delay
        return rx.timer(5.0).pipe(
            # On the timer emission, start a new retry attempt stream
            ops.flat_map(lambda _: get_spx_observable_with_retry())
        )

    # 3. Create the observable chain: attempt -> catch (for retry)
    return rx.defer(attempt_fetch_logic).pipe(
        ops.catch(handle_error)
    )


def create_spx_observable():
    """
    Creates the main scheduled observable that initiates the trading flow.
    
    - Schedules the start time 10 seconds in the future (for demo).
    - Starts the SPX retry stream.
    - On success, uses ops.flat_map to switch from the retry stream to 
      the continuous `get_spx_trade_loop`.
    """
    
    # --- SCHEDULING LOGIC ---
    # For DEMO purposes, we schedule 10 seconds from now.
    NOW = datetime.datetime.now()
    TARGET_TIME = NOW + datetime.timedelta(seconds=10)
    INITIAL_DELAY_SECONDS = (TARGET_TIME - NOW).total_seconds()
    
    print(f"[{time.strftime('%H:%M:%S')}] 🗓️ Scheduling SPX fetch to start in {INITIAL_DELAY_SECONDS:.1f} seconds.")

    # rx.timer(delay) waits for the initial delay, then emits one value and completes.
    return rx.timer(INITIAL_DELAY_SECONDS).pipe(
        # 1. Start the retry stream
        ops.flat_map(lambda _: get_spx_observable_with_retry()),
        
        # 2. On SUCCESS of the SPX fetch, switch the stream to the continuous trading loop
        ops.flat_map(lambda spx_data_result: rx.concat(
            # Emit a notification message first
            rx.just(f"Initial SPX Data Found: {spx_data_result}. Starting 5s Trading Loop."),
            # Then switch to the infinite trading loop
            get_spx_trade_loop()
        ))
    )


# --- Execution ---

keep_alive_flow = create_keep_alive_observable()
spx_data_flow = create_spx_observable()

print("Starting session manager. Keep-alive runs every 60 seconds if authenticated.")

# 1. Subscribe to the continuous keep-alive flow
keep_alive_flow.subscribe(
    on_next=lambda i: print(f"[{time.strftime('%H:%M:%S')}] ✨ Keep-Alive Received: {i}"),
    on_error=lambda e: print(f"[{time.strftime('%H:%M:%S')}] ❌ Keep-Alive Error: {e}"),
    on_completed=lambda: print(f"[{time.strftime('%H:%M:%S')}] \n✔️ Keep-Alive Flow completed.")
)

# 2. Subscribe to the one-time scheduled SPX flow (which switches to the trading loop)
spx_data_flow.subscribe(
    on_next=lambda i: print(f"[{time.strftime('%H:%M:%S')}] 🌟 SPX Trading Flow: {i}"),
    on_error=lambda e: print(f"[{time.strftime('%H:%M:%S')}] ❌ SPX Trading Flow Final Error: {e}"),
    on_completed=lambda: print(f"[{time.strftime('%H:%M:%S')}] \n✔️ SPX Trading Flow completed. (Price checking stopped)")
)


# ----------------- CONTINUOUS EXECUTION & LOGOUT -----------------
# Block the main thread to allow rxpy schedulers to run all concurrent streams.
if IS_AUTHENTICATED:
    try:
        print("\nManager running indefinitely. Press Enter or Ctrl+C to log out...")
        input() 
    except (KeyboardInterrupt, EOFError):
        # 1. Call the logout function
        logout()
        
        # 2. Signal the reactive stream (via the Subject) to stop immediately
        logout_signal.on_next(None)
        logout_signal.on_completed()
        
        print(f"[{time.strftime('%H:%M:%S')}] Exiting session manager after signal.")
