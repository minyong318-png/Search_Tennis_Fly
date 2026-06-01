import json
import socket
import ssl
import subprocess
import urllib.error
import urllib.request


HOST = "www.gytennis.or.kr"
SERVER_IP = socket.gethostbyname(HOST)
PATH = "/daily/1/2026-06-02"


def fetch(url: str, host_header: str | None = None) -> dict:
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/137.0.0.0 Safari/537.36"
        ),
        "Accept": "text/html,application/xhtml+xml",
        "Referer": "https://www.gytennis.or.kr/daily",
    }
    if host_header:
        headers["Host"] = host_header

    request = urllib.request.Request(url, headers=headers)
    context = ssl._create_unverified_context()
    try:
        with urllib.request.urlopen(request, timeout=20, context=context) as response:
            body = response.read().decode("utf-8", errors="replace")
            return {
                "url": url,
                "host_header": host_header,
                "status": response.status,
                "server": response.headers.get("Server"),
                "location": response.headers.get("Location"),
                "body_len": len(body),
                "body_head": body[:180],
            }
    except urllib.error.HTTPError as error:
        body = error.read().decode("utf-8", errors="replace")
        return {
            "url": url,
            "host_header": host_header,
            "status": error.code,
            "server": error.headers.get("Server"),
            "location": error.headers.get("Location"),
            "body_len": len(body),
            "body_head": body[:180],
        }
    except Exception as error:
        return {"url": url, "host_header": host_header, "error": repr(error)}


def curl_resolve(path: str) -> dict:
    url = f"https://{HOST}{path}"
    command = [
        "curl",
        "--silent",
        "--show-error",
        "--insecure",
        "--max-time",
        "20",
        "--resolve",
        f"{HOST}:443:{SERVER_IP}",
        "--write-out",
        "\n__STATUS__:%{http_code}",
        url,
    ]
    try:
        result = subprocess.run(
            command,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=25,
            check=False,
        )
        body, _, status = result.stdout.rpartition("\n__STATUS__:")
        return {
            "url": url,
            "resolve": f"{HOST}:443:{SERVER_IP}",
            "status": status,
            "returncode": result.returncode,
            "body_len": len(body),
            "body_head": body[:180],
            "stderr": result.stderr[:180],
        }
    except Exception as error:
        return {"url": url, "resolve": f"{HOST}:443:{SERVER_IP}", "error": repr(error)}


def main() -> None:
    probes = [
        fetch(f"https://{HOST}/"),
        fetch(f"https://{HOST}/daily"),
        fetch(f"https://{HOST}/daily/1"),
        fetch(f"https://{HOST}{PATH}"),
        fetch(f"https://gytennis.or.kr{PATH}"),
        fetch(f"https://{SERVER_IP}/", HOST),
        fetch(f"https://{SERVER_IP}/daily", HOST),
        fetch(f"https://{SERVER_IP}/daily/1", HOST),
        fetch(f"https://{SERVER_IP}{PATH}", HOST),
        fetch(f"http://{SERVER_IP}/", HOST),
        fetch(f"http://{SERVER_IP}{PATH}", HOST),
    ]
    curl_probes = [curl_resolve("/"), curl_resolve("/daily"), curl_resolve("/daily/1"), curl_resolve(PATH)]
    print(
        json.dumps(
            {"resolved_ip": SERVER_IP, "urllib_probes": probes, "curl_resolve_probes": curl_probes},
            ensure_ascii=False,
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
