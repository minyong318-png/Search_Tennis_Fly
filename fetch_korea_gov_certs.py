import ssl
import sys

TARGETS = [
    ("www.gytennis.or.kr", 443),
    ("daehwa.gys.or.kr", 451),
]
OUTFILE = "korea_gov_ca.pem"


def main() -> int:
    cert_blobs: list[str] = []

    for host, port in TARGETS:
        try:
            pem = ssl.get_server_certificate((host, port))
            cert_blobs.append(pem.strip() + "\n")
            print(f"[CERT] fetched {host}:{port}")
        except Exception as e:
            print(f"[CERT][WARN] failed {host}:{port}: {e}")

    try:
        with open(OUTFILE, "w", encoding="utf-8") as f:
            f.write("\n".join(cert_blobs).strip() + ("\n" if cert_blobs else ""))
        print(f"[CERT] wrote {OUTFILE} certs={len(cert_blobs)}")
    except Exception as e:
        print(f"[CERT][WARN] write failed: {e}")

    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as e:
        print(f"[CERT][WARN] unexpected: {e}")
        sys.exit(0)
