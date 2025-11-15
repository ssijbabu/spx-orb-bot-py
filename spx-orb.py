import asyncio
import time
import datetime as dt
import pytz # Need to install: pip install pytz
from ib_async import IB, RealTimeBar
from ib_async.contract import Index

# --- IB Configuration ---
IB_HOST = '127.0.0.1'
IB_PORT = 4002  # TWS paper trading port (7497 for live, 7496 for paper)
IB_CLIENT_ID = 1

# --- Global Trading Variables ---
ib = None
CONTRACT_SYMBOL = "SPX"
CONTRACT_EXCHANGE = "CBOE"
CONTRACT_CURRENCY = "USD"
CONTRACT_HIGH = None
CONTRACT_LOW = None

# --- NEW GLOBAL FLAG ---
IS_TESTING = True # Set to True to run fetch_opening_range immediately
# -------------------------

# The rest of the support functions (connect_ib, disconnect_ib, monitor_with_breakout) remain the same.

async def connect_ib():
    """Connect to Interactive Brokers and return IB instance"""
    global ib
    print(f"[{time.strftime('%H:%M:%S')}] 🔌 Connecting to IB at {IB_HOST}:{IB_PORT}...")
    
    try:
        ib = IB()
        await ib.connectAsync(IB_HOST, IB_PORT, IB_CLIENT_ID) 
        
        await asyncio.sleep(1)
        
        print(f"[{time.strftime('%H:%M:%S')}] ✅ Connected to IB successfully")
        return True
        
    except Exception as e:
        print(f"[{time.strftime('%H:%M:%S')}] ❌ IB connection failed: {e}")
        print(f"   Make sure TWS/IB Gateway is running on port {IB_PORT}")
        return False


async def disconnect_ib():
    """Disconnect from Interactive Brokers"""
    global ib
    if ib and ib.isConnected():
        print(f"[{time.strftime('%H:%M:%S')}] 🔌 Disconnecting from IB...")
        ib.disconnect()
        print(f"[{time.strftime('%H:%M:%S')}] ✅ Disconnected from IB")


async def fetch_opening_range():
    """
    Fetch high and low for the day using historical data (first hour of trading).
    This function will now only execute after the market open check passes.
    """
    global CONTRACT_HIGH, CONTRACT_LOW
    
    if not ib or not ib.isConnected():
        raise ConnectionError("IB not connected")
    
    print(f"[{time.strftime('%H:%M:%S')}] 📊 Fetching opening range...")

    try:
        contract = Index(CONTRACT_SYMBOL, CONTRACT_EXCHANGE, CONTRACT_CURRENCY)
        
        bars = await ib.reqHistoricalDataAsync(
            contract,
            endDateTime="",
            durationStr="1 D",
            barSizeSetting="30 mins",
            whatToShow="TRADES",
            useRTH=True
        )

        if not bars or len(bars) < 2:
             print(f"[{time.strftime('%H:%M:%S')}] ⚠️ Not enough historical data for opening range (needed 2 bars).")
             raise ValueError("Insufficient historical data")

        opening_range_bars = bars[:2]

        print(f"\n== Opening Range Bars ==")
        for bar in opening_range_bars:
            print(
                f"{bar.date.strftime('%H:%M')}  "
                f"O={bar.open}  "
                f"H={bar.high}  "
                f"L={bar.low}  "
                f"C={bar.close}"
            )

        highs = [b.high for b in opening_range_bars]
        lows = [b.low for b in opening_range_bars]
        CONTRACT_HIGH, CONTRACT_LOW = max(highs), min(lows)
        
        print(f"\n[{time.strftime('%H:%M:%S')}] ✅ Opening Range --> High: {CONTRACT_HIGH}, Low: {CONTRACT_LOW}")
        
    except Exception as e:
        print(f"[{time.strftime('%H:%M:%S')}] ❌ Failed to fetch data: {e}")
        CONTRACT_HIGH, CONTRACT_LOW = None, None
        raise


async def monitor_with_breakout():
    """
    Monitors real-time 5-second bars for a price breakout.
    Stops monitoring after the first breakout or if the task is cancelled.
    """
    global CONTRACT_HIGH, CONTRACT_LOW

    if not ib or not ib.isConnected():
        raise ConnectionError("IB not connected")
    
    if CONTRACT_HIGH is None or CONTRACT_LOW is None:
        print(f"[{time.strftime('%H:%M:%S')}] 🛑 Breakout levels not set. Skipping monitoring.")
        return

    print(f"[{time.strftime('%H:%M:%S')}] 📊 Monitoring breakout...")

    ticker = None
    breakout_detected = asyncio.Event()
    
    try:
        contract = Index(CONTRACT_SYMBOL, CONTRACT_EXCHANGE, CONTRACT_CURRENCY)

        ticker = ib.reqRealTimeBars(
            contract,
            barSize=5,
            whatToShow="TRADES",
            useRTH=True
        )
        
        def on_bar(bars: list, hasNewBar: bool):
            if breakout_detected.is_set():
                return

            last_bar = bars[-1]
            idx = len(bars) - 1
            
            print(f"[{idx:2d}] {CONTRACT_SYMBOL} {last_bar.time.strftime('%H:%M:%S')}  "
                  f"O={last_bar.open_:.2f}  H={last_bar.high:.2f}  "
                  f"L={last_bar.low:.2f}  C={last_bar.close:.2f}")

            if last_bar.close > CONTRACT_HIGH:
                print(f"\n📈 HIGH BREAKOUT {CONTRACT_SYMBOL} @ {last_bar.time.strftime('%H:%M:%S')} "
                      f"C={last_bar.close:.2f} (High: {CONTRACT_HIGH})\n")
                breakout_detected.set()
                
            elif last_bar.close < CONTRACT_LOW:
                print(f"\n📉 LOW BREAKOUT {CONTRACT_SYMBOL} @ {last_bar.time.strftime('%H:%M:%S')} "
                      f"C={last_bar.close:.2f} (Low: {CONTRACT_LOW})\n")
                breakout_detected.set()

        ticker.updateEvent += on_bar
        
        await breakout_detected.wait()
        
    except asyncio.CancelledError:
        print(f"\n[{time.strftime('%H:%M:%S')}] 🛑 Monitoring task cancelled.")
        raise
        
    except Exception as e:
        print(f"[{time.strftime('%H:%M:%S')}] ❌ Failed to monitor breakout: {e}")
        raise
        
    finally:
        if ticker:
            try:
                if 'on_bar' in locals() and on_bar in ticker.updateEvent:
                    ticker.updateEvent -= on_bar
                ib.cancelRealTimeBars(ticker.contract)
                print(f"[{time.strftime('%H:%M:%S')}] 🧹 Real-time data stream cancelled.")
            except Exception as e:
                print(f"[{time.strftime('%H:%M:%S')}] ⚠️ Failed to clean up IB data stream: {e}")

# --- NEW MARKET OPEN WAITING FUNCTION ---
async def wait_for_market_open():
    """
    Checks the global IS_TESTING flag and either runs immediately or waits
    until 9:30 AM New York time.
    """
    if IS_TESTING:
        print(f"[{time.strftime('%H:%M:%S')}] 🧪 IS_TESTING is True. Running fetch_opening_range immediately.")
        return
        
    NY_TZ = pytz.timezone('America/New_York')
    target_hour = 9
    target_minute = 30
    
    # Get current time in New York
    now_ny = dt.datetime.now(NY_TZ)
    
    # Set today's target time
    target_time_ny = NY_TZ.localize(
        dt.datetime(now_ny.year, now_ny.month, now_ny.day, target_hour, target_minute, 0)
    )
    
    # Check if target time has passed for today
    if now_ny >= target_time_ny:
        # If it has passed, set target for the next day (usually market opens at 9:30 AM)
        target_time_ny = target_time_ny + dt.timedelta(days=1)
        print(f"[{time.strftime('%H:%M:%S')}] ⚠️ Market open time (9:30 AM NY) already passed for today.")
        print(f"[{time.strftime('%H:%M:%S')}] 📅 Waiting for next day's open at {target_time_ny.strftime('%Y-%m-%d %H:%M:%S')}.")
    
    delay_seconds = (target_time_ny - now_ny).total_seconds()
    
    delay_hms = str(dt.timedelta(seconds=int(delay_seconds)))
    
    print(f"[{time.strftime('%H:%M:%S')}] ⏳ Waiting for market open (9:30 AM NY).")
    print(f"[{time.strftime('%H:%M:%S')}] ➡️ Delay remaining: {delay_hms}")
    
    # Wait for the calculated delay
    await asyncio.sleep(delay_seconds)
    
    print(f"[{time.strftime('%H:%M:%S')}] ✅ Market open time reached.")


async def main():
    print("=" * 60)
    print("SPX 1H Open Range Breakout - Trading Bot")
    print("=" * 60)
    
    monitor_task = None
    
    if not await connect_ib():
        print(f"[{time.strftime('%H:%M:%S')}] ⚠️  Failed to connect to IB. Exiting...")
        return
    
    try:
        # --- MODIFIED EXECUTION FLOW ---
        await wait_for_market_open() # Wait until 9:30 AM NY or run immediately if IS_TESTING=True
        await fetch_opening_range()  # Execute only after the wait is over
        # -------------------------------
        
        # Start monitoring immediately after fetching the range
        monitor_task = asyncio.create_task(monitor_with_breakout())
        
        await monitor_task
        
        print(f"[{time.strftime('%H:%M:%S')}] ✅ Monitoring task completed.")
        
    except KeyboardInterrupt:
        print(f"\n[{time.strftime('%H:%M:%S')}] 🛑 KeyboardInterrupt caught. Initiating shutdown...")
        # Cancel the monitoring task if it's still running
        if monitor_task and not monitor_task.done():
            monitor_task.cancel()
            try:
                # Wait for the task to finish its cancellation cleanup
                await monitor_task
            except asyncio.CancelledError:
                pass
    
    except Exception as e:
        print(f"\n[{time.strftime('%H:%M:%S')}] 💥 An unhandled error occurred in main: {e}")
        if monitor_task and not monitor_task.done():
            monitor_task.cancel()
            
    finally:
        await disconnect_ib()
        print(f"[{time.strftime('%H:%M:%S')}] 👋 Goodbye!")


if __name__ == "__main__":
    if hasattr(asyncio, 'WindowsSelectorEventLoopPolicy'):
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass