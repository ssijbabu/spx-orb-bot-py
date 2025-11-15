import asyncio
import time
import datetime as dt
import pytz
from ib_async import IB, RealTimeBar
from ib_async.contract import Index

# ============================================================
# CONFIGURATION
# ============================================================
IS_TESTING = True   # Skip waiting for market open

IB_HOST = "127.0.0.1"
IB_PORT = 4002
IB_CLIENT_ID = 1

CONTRACT = {
    "symbol": "SPX",
    "exchange": "CBOE",
    "currency": "USD"
}

NY_TZ = pytz.timezone("America/New_York")


# ============================================================
# STATE CONTAINER (instead of globals)
# ============================================================
class BotState:
    ib: IB = None
    high: float | None = None
    low: float | None = None
    rtb_ticker = None


state = BotState()


# ============================================================
# UTIL
# ============================================================
def now():
    return time.strftime("%H:%M:%S")


def make_contract():
    return Index(CONTRACT["symbol"], CONTRACT["exchange"], CONTRACT["currency"])


# ============================================================
# IB CONNECTION
# ============================================================
async def connect_ib():
    print(f"[{now()}] 🔌 Connecting to IB at {IB_HOST}:{IB_PORT}...")

    try:
        state.ib = IB()
        await state.ib.connectAsync(IB_HOST, IB_PORT, IB_CLIENT_ID)
        await asyncio.sleep(0.5)

        print(f"[{now()}] ✅ Connected to IB")
        return True
    except Exception as e:
        print(f"[{now()}] ❌ Connection failed: {e}")
        return False


async def disconnect_ib():
    ib = state.ib
    if ib and ib.isConnected():
        print(f"[{now()}] 🔌 Disconnecting from IB...")
        ib.disconnect()
        print(f"[{now()}] ✅ Disconnected")


# ============================================================
# MARKET OPEN WAIT
# ============================================================
async def wait_for_market_open():
    """Fast-interruptible wait for 9:30 AM NY time."""
    if IS_TESTING:
        print(f"[{now()}] 🧪 Testing mode: skipping market wait.")
        return

    NY_TZ = pytz.timezone("America/New_York")
    now_ny = dt.datetime.now(NY_TZ)
    target = now_ny.replace(hour=9, minute=30, second=0, microsecond=0)

    if now_ny >= target:
        target += dt.timedelta(days=1)
        print(f"[{now()}] ⚠️ Market already opened today; waiting for tomorrow.")

    print(f"[{now()}] ⏳ Waiting for market open at 9:30 NY...")

    # --- FAST INTERRUPTIBLE WAIT LOOP ---
    try:
        while True:
            now_ny = dt.datetime.now(NY_TZ)
            remaining = (target - now_ny).total_seconds()

            if remaining <= 0:
                break

            # Show progress every minute
            if int(remaining) % 60 == 0:
                print(f"[{now()}] ⏳ {int(remaining // 60)} min left...")

            # Short sleep = fast Ctrl+C
            await asyncio.sleep(0.5)

    except asyncio.CancelledError:
        print(f"[{now()}] 🛑 Wait cancelled by user.")
        raise

    print(f"[{now()}] ✅ Market open time reached.")

# ============================================================
# HISTORICAL OPENING RANGE
# ============================================================
async def fetch_opening_range():
    ib = state.ib
    if not ib or not ib.isConnected():
        raise RuntimeError("IB not connected")

    print(f"[{now()}] 📊 Fetching opening range...")

    try:
        bars = await ib.reqHistoricalDataAsync(
            make_contract(),
            endDateTime="",
            durationStr="1 D",
            barSizeSetting="30 mins",
            whatToShow="TRADES",
            useRTH=True
        )

        if len(bars) < 2:
            raise ValueError("Not enough bars (need 2).")

        open_range = bars[:2]

        print("\n== Opening Range Bars ==")
        for bar in open_range:
            print(
                f"{bar.date.strftime('%H:%M')}  "
                f"O={bar.open}  H={bar.high}  L={bar.low}  C={bar.close}"
            )

        state.high = max(b.high for b in open_range)
        state.low = min(b.low for b in open_range)

        print(f"\n[{now()}] ✅ Opening Range → High: {state.high}, Low: {state.low}")

    except Exception as e:
        print(f"[{now()}] ❌ Error fetching opening range: {e}")
        state.high = state.low = None
        raise


# ============================================================
# REAL-TIME BREAKOUT MONITOR
# ============================================================
async def monitor_with_breakout():
    ib = state.ib

    if not ib.isConnected():
        raise RuntimeError("IB not connected")
    if state.high is None or state.low is None:
        print(f"[{now()}] 🛑 No breakout levels; skipping monitor.")
        return

    print(f"[{now()}] 📊 Monitoring breakout levels...")

    breakout_event = asyncio.Event()
    ticker = ib.reqRealTimeBars(
        make_contract(),
        barSize=5,
        whatToShow="TRADES",
        useRTH=True,
    )
    state.rtb_ticker = ticker

    def on_bar(bars, hasNewBar):
        if not hasNewBar or breakout_event.is_set():
            return

        bar = bars[-1]
        ts = bar.time.strftime("%H:%M:%S")

        print(
            f"{len(bars)-1:2d} | {CONTRACT['symbol']} {ts}  "
            f"O={bar.open_:.2f} H={bar.high:.2f} L={bar.low:.2f} C={bar.close:.2f}"
        )

        if bar.close > state.high:
            print(f"\n📈 HIGH BREAKOUT at {ts} → {bar.close:.2f}\n")
            breakout_event.set()

        elif bar.close < state.low:
            print(f"\n📉 LOW BREAKOUT at {ts} → {bar.close:.2f}\n")
            breakout_event.set()

    ticker.updateEvent += on_bar

    try:
        # ⚡ Fast interruptible waiting loop
        while not breakout_event.is_set():
            await asyncio.sleep(0.5)  # checks every 0.5s for Ctrl+C

    except asyncio.CancelledError:
        print(f"[{now()}] 🛑 Monitor cancelled by user.")
        raise

    finally:
        # Cleanup
        if on_bar in ticker.updateEvent:
            ticker.updateEvent -= on_bar
        ib.cancelRealTimeBars(ticker.contract)
        print(f"[{now()}] 🧹 Real-time bars cancelled.")



# ============================================================
# MAIN APP
# ============================================================
async def main():
    print("=" * 60)
    print(" SPX Opening Range Breakout Bot")
    print("=" * 60)

    if not await connect_ib():
        return

    try:
        await wait_for_market_open()
        await fetch_opening_range()

        monitor_task = asyncio.create_task(monitor_with_breakout())
        await monitor_task

    except Exception as e:
        print(f"[{now()}] 💥 Error in main: {e}")

    finally:
        await disconnect_ib()
        print(f"[{now()}] 👋 Goodbye!")


if __name__ == "__main__":
    if hasattr(asyncio, "WindowsSelectorEventLoopPolicy"):
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("🛑 Program interrupted by user. Exiting cleanly.")

