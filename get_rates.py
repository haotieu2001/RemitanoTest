import pandas as pd
import requests
import json
from datetime import datetime, timedelta
import time
import os

# Read transactions data
transactions = pd.read_csv('./transactions.csv')

# 1. Get all currencies from transactions
all_currencies = set(transactions['destination_currency'].unique())


print(f"Total currencies in data: {len(all_currencies)}")
print(f"Currencies: {sorted(all_currencies)}")

# 2. Get time range
transactions['created_at'] = pd.to_datetime(transactions['created_at'])
start_date = transactions['created_at'].min().date()
end_date = transactions['created_at'].max().date()

print(f"\nTime range: {start_date} to {end_date}")

# 3. Get available USDT pairs from Binance
def get_binance_usdt_pairs():
    """Get all USDT trading pairs from Binance"""
    url = "https://api.binance.com/api/v3/exchangeInfo"
    try:
        response = requests.get(url, timeout=10)
        data = response.json()
        
        usdt_pairs = {}
        for symbol_info in data['symbols']:
            if (symbol_info['status'] == 'TRADING' and 
                symbol_info['quoteAsset'] == 'USDT'):
                base_asset = symbol_info['baseAsset']
                symbol = symbol_info['symbol']
                usdt_pairs[base_asset] = symbol
        
        print(f"Found {len(usdt_pairs)} USDT pairs on Binance")
        return usdt_pairs
        
    except Exception as e:
        print(f"Error fetching Binance data: {e}")
        return {}

# Get available USDT pairs
available_pairs = get_binance_usdt_pairs()

# 4. Filter currencies that have USDT pairs on Binance
supported_currencies = []
unsupported_currencies = []

for currency in all_currencies:
    # Special case: USDT itself
    if currency == 'USDT':
        supported_currencies.append(('USDT', 'USDTUSDT'))
    elif currency in available_pairs:
        supported_currencies.append((currency, available_pairs[currency]))
    else:
        unsupported_currencies.append(currency)

print(f"\nSupported currencies with USDT pairs: {len(supported_currencies)}")
for curr, symbol in sorted(supported_currencies):
    print(f"  {curr} -> {symbol}")

print(f"\nUnsupported currencies (will be skipped): {len(unsupported_currencies)}")
for curr in sorted(unsupported_currencies):
    print(f"  {curr}")

# 5. Function to fetch hourly klines from Binance
def fetch_klines(symbol, start_time, end_time):
    """Fetch hourly klines data from Binance"""
    base_url = "https://api.binance.com/api/v3/klines"
    all_data = []
    
    current_start = start_time
    batch_count = 0
    
    print(f"  Fetching {symbol}...", end="", flush=True)
    
    while current_start < end_time and batch_count < 50:  # Limit to 50 batches
        batch_end = min(current_start + timedelta(days=30), end_time)
        limit = 1000

        params = {
            'symbol': symbol,
            'interval': '1h',
            'startTime': int(current_start.timestamp() * 1000),
            'endTime': int(batch_end.timestamp() * 1000),
            'limit': limit
        }
        
        try:
            response = requests.get(base_url, params=params, timeout=30)
            data = response.json()
            
            if isinstance(data, list) and len(data) > 0:
                all_data.extend(data)
                print(".", end="", flush=True)
                
                # Advance start to AFTER the last candle to avoid overlap
                last_timestamp = data[-1][0]
                current_start = datetime.fromtimestamp(last_timestamp / 1000) + timedelta(milliseconds=1)
                
                # Only stop early if:
                # - we requested up to the *global* end_time, AND
                # - Binance returned less than the page limit
                if batch_end >= end_time and len(data) < limit:
                    break
            else:
                print("X", end="", flush=True)
                break
                
            batch_count += 1
            time.sleep(0.1)  # Rate limiting
            
        except Exception as e:
            print("E", end="", flush=True)
            break
    
    print()  # New line
    return all_data

# 6. Create output directory
os.makedirs('output/raw_rates', exist_ok=True)

# 7. Fetch rates for supported currencies
start_dt = datetime.combine(start_date, datetime.min.time())
end_dt = datetime.combine(end_date, datetime.max.time())

print(f"\nFetching exchange rates from {start_dt} to {end_dt}")
print(f"This may take a few minutes...\n")

success_count = 0
failed_count = 0

for currency, symbol in sorted(supported_currencies):
    print(f"Processing {currency} ({symbol}):")
    
    if symbol == 'USDTUSDT':
        # Special handling for USDT (rate is always 1)
        print(f"  USDT rate is always 1, generating synthetic data...")
        
        # Generate synthetic hourly rates (all 1.0)
        synthetic_data = []
        current = start_dt
        
        while current < end_dt:
            timestamp_ms = int(current.timestamp() * 1000)
            synthetic_data.append([
                timestamp_ms,           # open_time
                '1.0', '1.0', '1.0', '1.0',  # open, high, low, close
                '0',                    # volume
                timestamp_ms + 3599000, # close_time (+1 hour - 1 second)
                '0', '0', '0', '0', '0'  # other fields
            ])
            current += timedelta(hours=1)
        
        klines_data = synthetic_data
    else:
        # Fetch actual data from Binance
        klines_data = fetch_klines(symbol, start_dt, end_dt)
    
    if klines_data:
        # Convert to DataFrame
        df = pd.DataFrame(klines_data, columns=[
            'open_time', 'open', 'high', 'low', 'close', 'volume',
            'close_time', 'quote_asset_volume', 'number_of_trades',
            'taker_buy_base_asset_volume', 'taker_buy_quote_asset_volume', 'ignore'
        ])
        
        # Add metadata
        df['symbol'] = symbol
        df['base_currency'] = currency
        df['quote_currency'] = 'USDT'
        
        
        # Convert numeric columns
        for col in ['open', 'high', 'low', 'close']:
            df[col] = pd.to_numeric(df[col], errors='coerce')
        
        # Save to JSONL file
        output_file = f'output/raw_rates/{currency}_rates.json'
        df.to_json(output_file, orient='records', lines=True)
        
        print(f"  ✓ Saved {len(df)} hourly records to {output_file}")
        success_count += 1
    else:
        print(f"  ✗ Failed to fetch data for {currency}")
        failed_count += 1

# 8. Combine all JSON files into a single CSV
all_json_dir = 'output/raw_rates'
all_dfs = []

for filename in os.listdir(all_json_dir):
    if filename.endswith('_rates.json'):
        file_path = os.path.join(all_json_dir, filename)
        df_part = pd.read_json(file_path, lines=True)
        all_dfs.append(df_part)

if all_dfs:
    combined_df = pd.concat(all_dfs, ignore_index=True)
    csv_output_path = 'output/rates.csv'
    combined_df.to_csv(csv_output_path, index=False)
    print(f"\nSaved combined CSV with {len(combined_df)} rows to {csv_output_path}")
else:
    print("\nNo JSON rate files found to combine into CSV.")