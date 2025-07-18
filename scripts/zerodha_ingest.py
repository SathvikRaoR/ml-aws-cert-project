import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
import pandas as pd
from kiteconnect import KiteConnect
from dotenv import load_dotenv
from scripts.s3_etl import *

load_dotenv()
api_key = os.getenv('ZERODHA_API_KEY')
access_token = os.getenv('ZERODHA_ACCESS_TOKEN')
kite = KiteConnect(api_key=api_key)
kite.set_access_token(access_token)

# Fetch holding data
holdings = kite.holdings()
df = pd.DataFrame(holdings)
filename = f"data/zerodha_holdings_{pd.Timestamp.now().strftime('%Y%m%d')}.csv"
df.to_csv(filename, index=False)
upload_file(filename, f"raw/zerodha/{os.path.basename(filename)}")