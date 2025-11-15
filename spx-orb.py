import asyncio
import time
from ib_async import IB, RealTimeBar
from ib_async.contract import Index

# --- IB Configuration ---
IB_HOST = '127.0.0.1'
IB_PORT = 4002  # TWS paper trading port (7497 for live, 7496 for paper)
IB_CLIENT_ID = 1

# Global variables
ib = None
CONTRACT_SYMBOL = "SPX"
CONTRACT_EXCHANGE = "CBOE"
CONTRACT_CURRENCY = "USD"
CONTRACT_HIGH = None
CONTRACT_LOW = None

async def connect_ib():
    """Connect to Interactive Brokers and return IB instance"""
    global ib
    print(f"[{time.strftime('%H:%M:%S')}] 🔌 Connecting to IB at {IB_HOST}:{IB_PORT}...")
    
    try:
        ib = IB()
        await ib.connectAsync(IB_HOST, IB_PORT, IB_CLIENT_ID)        
        
        # Wait a moment for connection to stabilize
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
    Fetch high and low for the day using historical data.
    Requests the first hour of trading (9:30-10:30 AM ET).
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

        opening_range_bars = bars[:2]  # First two 30-min bars for opening range

        print(f"\n== Opening Range ==")
        for bar in opening_range_bars:
            print(
                f"{bar.date}  "
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
        raise

async def monitor_with_breakout(ib): # Pass 'ib' as an argument if it's not global
    """
    Monitors real-time 5-second bars for a price breakout above CONTRACT_HIGH
    or below CONTRACT_LOW and stops monitoring after the first breakout.
    """

    if not ib or not ib.isConnected():
        raise ConnectionError("IB not connected")
    
    print(f"[{time.strftime('%H:%M:%S')}] 📊 Monitoring breakout...")

    ticker = None # Initialize ticker outside try block for cleanup
    
    try:
        # Index is a specific Contract type for indices
        contract = Index(CONTRACT_SYMBOL, CONTRACT_EXCHANGE, CONTRACT_CURRENCY)

        # ib_insync's reqRealTimeBars returns a Ticker object
        ticker = ib.reqRealTimeBars(
            contract,
            barSize=5,
            whatToShow="TRADES",
            useRTH=True
        )

        def on_bar(bars: list, hasNewBar: bool): # Assuming bars contains RealTimeBar objects
            # Check if monitoring has already stopped to prevent errors
            if on_bar not in ticker.updateEvent:
                return

            last_bar = bars[-1]
            idx = len(bars) - 1
            
            print(f"[{idx:2d}] {CONTRACT_SYMBOL} {last_bar.time.strftime('%H:%M:%S')}  "
                  f"O={last_bar.open_:.2f}  H={last_bar.high:.2f}  "
                  f"L={last_bar.low:.2f}  C={last_bar.close:.2f}")

            # Check for high breakout
            if last_bar.close > CONTRACT_HIGH:
                print(f"\n📈 HIGH BREAKOUT {CONTRACT_SYMBOL} @ {last_bar.time.strftime('%H:%M:%S')} "
                      f"C={last_bar.close:.2f}\n")
                
                # Unsubscribe the event handler
                ticker.updateEvent -= on_bar
                # Stop the real-time data request
                ib.cancelRealTimeBars(ticker.contract)
                
            # Use elif to ensure only one action is taken per bar (high OR low)
            elif last_bar.close < CONTRACT_LOW:
                print(f"\n📉 LOW BREAKOUT {CONTRACT_SYMBOL} @ {last_bar.time.strftime('%H:%M:%S')} "
                      f"C={last_bar.close:.2f}\n")
                
                # Unsubscribe the event handler
                ticker.updateEvent -= on_bar
                # Stop the real-time data request
                ib.cancelRealTimeBars(ticker.contract)

        ticker.updateEvent += on_bar
        
    except Exception as e:
        print(f"[{time.strftime('%H:%M:%S')}] ❌ Failed to monitor breakout: {e}")
        # Clean up if an error occurred before reaching the main loop
        if ticker and on_bar in ticker.updateEvent:
            ticker.updateEvent -= on_bar
        if ticker:
            ib.cancelRealTimeBars(ticker.contract)
        raise

    # keep this coroutine alive indefinitely while the ticker is active
    # The 'on_bar' handler now stops the data stream when a breakout occurs,
    # but the coroutine needs to wait for the main loop to handle the cancellation
    await asyncio.Event().wait()

async def main():
    print("=" * 60)
    print("SPX 1H Open Range Breakout - Trading Bot")
    print("=" * 60)

    if not await connect_ib():
        print(f"[{time.strftime('%H:%M:%S')}] ⚠️  Failed to connect to IB. Exiting...")
        return
    
    try:
        await fetch_opening_range()
        
        # Keep the connection alive until Ctrl+C is pressed
        while True:
            await asyncio.sleep(1)
    except (KeyboardInterrupt, asyncio.CancelledError):
        print(f"\n[{time.strftime('%H:%M:%S')}] 🛑 Shutdown initiated...")
    finally:
        await disconnect_ib()
        print(f"[{time.strftime('%H:%M:%S')}] 👋 Goodbye!")


if __name__ == "__main__":
    asyncio.run(main())