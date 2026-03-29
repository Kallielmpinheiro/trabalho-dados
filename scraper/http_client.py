import requests

class HttpClient:
    def __init__(self):
        self.session = requests.Session()

    def get(self, url):
        try:
            return self.session.get(url, timeout=10)
        except:
            return None