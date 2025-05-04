import requests
import time
from typing import Optional

class MasterSwiftAuth:
    def __init__(self):
        self.auth_url = "https://masterswift-beta.mastertrust.co.in/oauth2/auth"
        self.token_url = "https://masterswift-beta.mastertrust.co.in/oauth2/token"
        self.callback_url = "https://oauth.pstmn.io/v1/callback"
        self.client_id = "dA7xiZTefv"
        self.client_secret = "u1cpF7NUv098OSCympBUxLFY9vuGLtPHJzSVKGpuyav3vRHqaZLego854svBZCAN"
        self.state = "MyRandomSt123"
        self._access_token = "vdwHlyglMZcV-PqUtLIE6zyQcZsC41rYXhknTULdDXY.3CfqKJNN0fc5yravajxMPJXcs4zmiQ8QmuWyHHiuyjU"
        self._token_expiry = None

    @property
    def access_token(self) -> Optional[str]:
        """Get current access token, only refresh if the default token doesn't work"""
        return self._access_token or self.get_new_access_token()

    def get_new_access_token(self) -> Optional[str]:
        """Get a new access token using OAuth 2.0 credentials"""
        try:
            data = {
                'grant_type': 'client_credentials',
                'client_id': self.client_id,
                'client_secret': self.client_secret,
                'state': self.state
            }
            
            response = requests.post(self.token_url, data=data)
            response.raise_for_status()
            
            token_data = response.json()
            self._access_token = token_data.get('access_token')
            self._token_expiry = time.time() + token_data.get('expires_in', 3600)
            
            return self._access_token
            
        except requests.exceptions.RequestException as e:
            print(f"Error getting access token: {e}")
            return None