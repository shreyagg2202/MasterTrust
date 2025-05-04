import requests
import pandas as pd
from auth_handler import MasterSwiftAuth
from typing import Optional, Dict, Any
from datetime import datetime

class MasterSwiftClient:
    def __init__(self):
        self.base_url = "https://masterswift.mastertrust.co.in/api/v1"
        self.auth = MasterSwiftAuth()
        
    def get_chart_data(self, token: str, exchange: str, 
                       starttime: int, endtime: int, 
                       candletype: int = 1, 
                       data_duration: int = 1) -> Optional[pd.DataFrame]:
        """
        Get chart data from the API and return as a pandas DataFrame
        Returns:
            DataFrame with columns: timestamp, open, high, low, close, volume
        """
        endpoint = f"{self.base_url}/charts/tdv"
        params = {
            "token": token,
            "exchange": exchange,
            "starttime": starttime,
            "endtime": endtime,
            "candletype": candletype,
            "data_duration": data_duration
        }
        
        try:
            headers = {"Authorization": f"Bearer {self.auth.access_token}"}
            response = requests.get(endpoint, params=params, headers=headers)
            
            if response.status_code == 401:
                # Force token refresh and retry once
                self.auth.get_new_access_token()
                headers = {"Authorization": f"Bearer {self.auth.access_token}"}
                response = requests.get(endpoint, params=params, headers=headers)
            
            response.raise_for_status()
            json_data = response.json()
            
            # Convert JSON to DataFrame
            return self._process_chart_data(json_data)
            
        except requests.exceptions.RequestException as e:
            print(f"Error fetching chart data: {e}")
            return None

    def _process_chart_data(self, json_data: Dict[str, Any]) -> Optional[pd.DataFrame]:
        """
        Process the JSON response into a pandas DataFrame
        """
        try:
            # Extract candles data from JSON
            candles = json_data.get('data', {}).get('candles', [])
            
            if not candles:
                print("No candle data found in response")
                return None
            
            # Create DataFrame
            df = pd.DataFrame(candles, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
            
            # Convert timestamp to datetime
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            
            # Set timestamp as index
            df.set_index('timestamp', inplace=True)
            
            # Convert price columns to float
            price_columns = ['open', 'high', 'low', 'close']
            df[price_columns] = df[price_columns].astype(float)
            
            # Convert volume to int
            df['volume'] = df['volume'].astype(int)
            
            df.to_csv('historical_data.csv')

            return df
            
        except Exception as e:
            print(f"Error processing chart data: {e}")
            return None

# Example usage
if __name__ == "__main__":
    client = MasterSwiftClient()
    
    # Example request
    df = client.get_chart_data(
        token="35001",
        exchange="NFO",
        starttime=1741132800,
        endtime=1741219199,
        candletype=1,
        data_duration=1
    )
    
    if df is not None:
        print("\nFirst few rows of the data:")
        print(df.head())
        
        print("\nDataFrame info:")
        print(df.info())
        
        print("\nBasic statistics:")
        print(df.describe())