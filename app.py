"""
Reactive Trading Bot using RxPY with Separate Stock and Order Streams

This module implements a trading system with two primary reactive streams:
1. Stock Stream: Handles SPX data fetching and price monitoring
2. Order Stream: Manages order placement, status checking, and timeouts

Key Features:
- Authentication management with session keep-alive
- Scheduled SPX high/low fetching at 10:30 AM ET
- Continuous price monitoring with conditional order triggers
- 30-minute order timeout mechanism
- Automatic bracket order placement on fill
"""
import rx
from rx import operators as ops
from rx.subject import Subject, BehaviorSubject
import time
import datetime
import pytz
import math
import random

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

# --- Configuration ---
IS_AUTHENTICATED = True  # Set to False to test unauthenticated flow
SESSION_URL = "https://localhost:5000"

# --- Global State ---
SPX_HIGH = None
SPX_LOW = None

# --- Subjects for Reactive Streams ---
# Subject to signal logout/exit
logout_signal = Subject()

# BehaviorSubject for stock stream state (holds latest SPX data)
stock_stream = BehaviorSubject({"high": None, "low": None, "current_price": None})

# Subject for order stream events
order_stream = Subject()

# Subject to control price monitoring (stop when order placed)
stop_price_monitoring = Subject()

# Subject to signal when orders are filled/cancelled
order_completed = Subject()


# --- Mock API Functions ---

def confirm_auth_status():
    """Check authentication status."""
    print(f"[{time.strftime('%H:%M:%S')}] 🔍 Checking authentication status...")
    time.sleep(0.3)
    return IS_AUTHENTICATED


def tickle():
    """Keep-alive endpoint call."""
    print(f"[{time.strftime('%H:%M:%S')}] ⏰ Tickle: Session refreshed")
    return "SESSION_ALIVE"


def logout():
    """Logout endpoint call."""
    print(f"[{time.strftime('%H:%M:%S')}] 🚪 Logout: Terminating session")
    time.sleep(0.2)


def fetch_spx_high_low():
    """
    Fetch SPX high and low for the day.
    Simulates occasional failures for demonstration.
    """
    # Simulate 30% failure rate for testing retry logic
    if random.random() < 0.3:
        raise ConnectionError("Failed to fetch SPX data")
    
    high = 5200.50 + random.uniform(-10, 10)
    low = 5150.25 + random.uniform(-10, 10)
    
    print(f"[{time.strftime('%H:%M:%S')}] 📊 SPX Data Retrieved: High=${high:.2f}, Low=${low:.2f}")
    
    global SPX_HIGH, SPX_LOW
    SPX_HIGH = high
    SPX_LOW = low
    
    return {"high": high, "low": low}


def get_current_price():
    """Get current SPX trading price with simulated movement."""
    if SPX_HIGH is None or SPX_LOW is None:
        return None
    
    # Simulate price movement around midpoint with sine wave
    t = time.time()
    base_price = (SPX_HIGH + SPX_LOW) / 2
    swing = 50.0 * math.sin(t / 10.0)
    noise = random.uniform(-2.0, 2.0)
    
    price = base_price + swing + noise
    return price


def short_put_spread(price):
    """
    Place a short put spread (two orders).
    Called when price falls below the low.
    """
    order_id = f"PUT_SPREAD_{int(time.time() * 1000)}"
    timestamp = time.time()
    
    print(f"[{time.strftime('%H:%M:%S')}] 📉 TRADE: Placing Short Put Spread at ${price:.2f}")
    print(f"   Order ID: {order_id}")
    
    return {
        "order_id": order_id,
        "type": "SHORT_PUT_SPREAD",
        "price": price,
        "timestamp": timestamp,
        "status": ORDER_STATUS_PENDING
    }


def short_call_spread(price):
    """
    Place a short call spread (two orders).
    Called when price rises above the high.
    """
    order_id = f"CALL_SPREAD_{int(time.time() * 1000)}"
    timestamp = time.time()
    
    print(f"[{time.strftime('%H:%M:%S')}] 📈 TRADE: Placing Short Call Spread at ${price:.2f}")
    print(f"   Order ID: {order_id}")
    
    return {
        "order_id": order_id,
        "type": "SHORT_CALL_SPREAD",
        "price": price,
        "timestamp": timestamp,
        "status": ORDER_STATUS_PENDING
    }


def check_order_status(order):
    """
    Check if orders are filled.
    Simulates fill after 15 seconds for testing.
    """
    elapsed = time.time() - order["timestamp"]
    
    # Check for timeout
    if elapsed >= ORDER_TIMEOUT_SECONDS:
        print(f"[{time.strftime('%H:%M:%S')}] ⏱️ Order {order['order_id']}: TIMEOUT after {ORDER_TIMEOUT_SECONDS/60:.1f} minutes")
        return ORDER_STATUS_CANCELLED
    
    # Simulate fill after 15 seconds (for testing)
    if elapsed >= 15.0:
        print(f"[{time.strftime('%H:%M:%S')}] ✅ Order {order['order_id']}: FILLED")
        return ORDER_STATUS_FILLED
    
    return ORDER_STATUS_PENDING


def cancel_order(order):
    """Cancel an order that timed out."""
    print(f"[{time.strftime('%H:%M:%S')}] 🛑 Cancelling order: {order['order_id']}")
    time.sleep(0.2)


def place_bracket_order(order):
    """
    Place bracket order (take-profit and stop-loss) after spread is filled.
    """
    bracket_id = f"BRACKET_{int(time.time() * 1000)}"
    print(f"[{time.strftime('%H:%M:%S')}] 🎯 Placing Bracket Order for {order['order_id']}")
    print(f"   Bracket ID: {bracket_id}")
    print(f"   Take-Profit and Stop-Loss orders placed")
    
    return {
        "bracket_id": bracket_id,
        "parent_order": order["order_id"],
        "type": "BRACKET"
    }


# --- Helper Functions ---

def seconds_until_target_time():
    """
    Calculate seconds until 10:30 AM ET today or tomorrow.
    For testing, returns 10 seconds.
    """
    # FOR PRODUCTION: Use actual 10:30 AM ET calculation
    # et_tz = pytz.timezone('America/New_York')
    # now = datetime.datetime.now(et_tz)
    # target = now.replace(hour=10, minute=30, second=0, microsecond=0)
    # if now >= target:
    #     target += datetime.timedelta(days=1)
    # return (target - now).total_seconds()
    
    # FOR TESTING: 10 seconds delay
    return 10.0


# --- Reactive Stream: Authentication & Keep-Alive ---

def create_auth_and_keepalive_stream():
    """
    Creates the authentication check and keep-alive stream.
    - Checks auth on startup
    - If authenticated, starts 60-second tickle interval
    - Stops on logout signal
    """
    def check_and_start_keepalive(scheduler):
        if not confirm_auth_status():
            print(f"[{time.strftime('%H:%M:%S')}] ❌ Not authenticated!")
            print(f"   Please sign in at {SESSION_URL} and restart the program.")
            return rx.empty()
        
        print(f"[{time.strftime('%H:%M:%S')}] ✅ Authenticated! Starting keep-alive...")
        
        # Start keep-alive ticker
        return rx.interval(TICKLE_INTERVAL_SECONDS).pipe(
            ops.start_with(-1),  # Trigger immediately
            ops.map(lambda _: tickle()),
            ops.take_until(logout_signal)
        )
    
    return rx.defer(check_and_start_keepalive)


# --- Reactive Stream: Stock Data (SPX High/Low + Price Monitoring) ---

def create_spx_fetch_stream():
    """
    Fetches SPX high/low with retry logic.
    Retries every 5 seconds on failure.
    """
    def attempt_fetch():
        try:
            return rx.of(fetch_spx_high_low())
        except Exception as e:
            print(f"[{time.strftime('%H:%M:%S')}] ⚠️ SPX fetch failed: {e}")
            print(f"   Retrying in {SPX_RETRY_INTERVAL_SECONDS} seconds...")
            return rx.timer(SPX_RETRY_INTERVAL_SECONDS).pipe(
                ops.flat_map(lambda _: create_spx_fetch_stream())
            )
    
    return rx.defer(lambda _: attempt_fetch())


def create_price_monitoring_stream():
    """
    Monitors SPX price every 5 seconds.
    Emits prices to stock_stream and triggers orders based on conditions.
    """
    return rx.interval(PRICE_CHECK_INTERVAL_SECONDS).pipe(
        ops.start_with(-1),
        ops.map(lambda _: get_current_price()),
        ops.filter(lambda p: p is not None),
        ops.take_until(stop_price_monitoring),
        ops.do_action(
            on_next=lambda price: print(f"[{time.strftime('%H:%M:%S')}] 💹 Current Price: ${price:.2f} (High: ${SPX_HIGH:.2f}, Low: ${SPX_LOW:.2f})")
        ),
        ops.do_action(
            on_next=lambda price: stock_stream.on_next({
                "high": SPX_HIGH,
                "low": SPX_LOW,
                "current_price": price
            })
        ),
        # Check trading conditions
        ops.map(lambda price: {
            "price": price,
            "trigger": "ABOVE_HIGH" if price > SPX_HIGH else "BELOW_LOW" if price < SPX_LOW else None
        }),
        ops.filter(lambda data: data["trigger"] is not None),
        ops.take(1),  # Only trigger once
        ops.map(lambda data: 
            short_call_spread(data["price"]) if data["trigger"] == "ABOVE_HIGH"
            else short_put_spread(data["price"])
        ),
        ops.do_action(on_next=lambda order: order_stream.on_next(order))
    )


def create_stock_stream():
    """
    Main stock stream: schedules SPX fetch, then starts price monitoring.
    """
    delay = seconds_until_target_time()
    print(f"[{time.strftime('%H:%M:%S')}] 📅 SPX fetch scheduled in {delay:.1f} seconds (10:30 AM ET)")
    
    return rx.timer(delay).pipe(
        ops.flat_map(lambda _: create_spx_fetch_stream()),
        ops.do_action(on_next=lambda data: print(f"[{time.strftime('%H:%M:%S')}] 🎯 SPX data acquired. Starting price monitoring...")),
        ops.flat_map(lambda _: create_price_monitoring_stream())
    )


# --- Reactive Stream: Order Management ---

def create_order_monitoring_stream(order):
    """
    Monitors order status every 5 seconds until filled or timeout.
    """
    return rx.interval(ORDER_CHECK_INTERVAL_SECONDS).pipe(
        ops.start_with(-1),
        ops.map(lambda _: check_order_status(order)),
        ops.filter(lambda status: status != ORDER_STATUS_PENDING),
        ops.take(1),
        ops.flat_map(lambda status: 
            rx.of(status).pipe(
                ops.do_action(on_next=lambda s: 
                    place_bracket_order(order) if s == ORDER_STATUS_FILLED
                    else cancel_order(order)
                ),
                ops.do_action(on_next=lambda _: order_completed.on_next(status))
            )
        )
    )


def create_order_stream():
    """
    Order stream: listens for order placements and monitors them.
    """
    return order_stream.pipe(
        ops.do_action(on_next=lambda order: stop_price_monitoring.on_next(True)),
        ops.flat_map(lambda order: create_order_monitoring_stream(order)),
        ops.take_until(logout_signal)
    )


# --- Main Execution ---

def main():
    print("=" * 60)
    print("SPX ORB - TRADING BOT - Starting")
    print("=" * 60)
    
    # Subscribe to authentication and keep-alive
    auth_subscription = create_auth_and_keepalive_stream().subscribe(
        on_next=lambda x: None,
        on_error=lambda e: print(f"[{time.strftime('%H:%M:%S')}] ❌ Auth Error: {e}"),
        on_completed=lambda: print(f"[{time.strftime('%H:%M:%S')}] ✔️ Auth stream completed")
    )
    
    if not IS_AUTHENTICATED:
        return
    
    # Subscribe to stock stream (SPX data + price monitoring)
    stock_subscription = create_stock_stream().subscribe(
        on_next=lambda x: None,
        on_error=lambda e: print(f"[{time.strftime('%H:%M:%S')}] ❌ Stock Error: {e}"),
        on_completed=lambda: print(f"[{time.strftime('%H:%M:%S')}] ✔️ Stock stream completed")
    )
    
    # Subscribe to order stream
    order_subscription = create_order_stream().subscribe(
        on_next=lambda x: None,
        on_error=lambda e: print(f"[{time.strftime('%H:%M:%S')}] ❌ Order Error: {e}"),
        on_completed=lambda: print(f"[{time.strftime('%H:%M:%S')}] ✔️ Order stream completed")
    )
    
    # Wait for user to exit
    try:
        print("\n" + "=" * 60)
        print("Bot is running. Press Ctrl+C to logout and exit.")
        print("=" * 60 + "\n")
        
        # Keep main thread alive
        while True:
            time.sleep(1)
            
    except KeyboardInterrupt:
        print(f"\n[{time.strftime('%H:%M:%S')}] 🛑 Shutdown initiated...")
        logout()
        logout_signal.on_next(True)
        logout_signal.on_completed()
        
        # Give streams time to clean up
        time.sleep(1)
        
        print(f"[{time.strftime('%H:%M:%S')}] 👋 Goodbye!")


if __name__ == "__main__":
    main()