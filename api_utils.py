import requests

def generate_upi_qr_url(upi_id: str, amount: int) -> str:
    return f"https://api.qrserver.com/v1/create-qr-code/?size=300x300&data=upi://pay?pa={upi_id}&am={amount}"

def fetch_url_data(url: str, timeout: int = 10) -> dict:
    try:
        response = requests.get(url, timeout=timeout)
        if response.status_code == 200:
            return {"status": True, "data": response.text}
        return {"status": False, "error": f"HTTP {response.status_code}"}
    except Exception as e:
        return {"status": False, "error": str(e)}

def rotate_proxy_request(url: str, proxies: list = None, timeout: int = 10) -> dict:
    if not proxies:
        return fetch_url_data(url, timeout=timeout)

    for proxy in proxies:
        try:
            proxy_dict = {"http": proxy, "https": proxy}
            response = requests.get(url, proxies=proxy_dict, timeout=timeout)
            if response.status_code == 200:
                return {"status": True, "data": response.text, "proxy": proxy}
        except Exception:
            continue

    return {"status": False, "error": "All proxies failed"}
