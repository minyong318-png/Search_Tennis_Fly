# Oracle Cloud Free (Seoul) Goyang Runner Setup

This repo is configured so `crawl_goyang.yml` runs on:
- `runs-on: [self-hosted, oracle-kr-goyang]`

## 1) Create OCI VM (Always Free)
- Region: Seoul (ap-seoul-1)
- Shape: Ampere A1 (Always Free)
- OS: Ubuntu 22.04
- Network: public subnet with public IP
- Ingress: allow TCP 22 (SSH)

## 2) Install base packages
```bash
sudo apt-get update
sudo apt-get install -y git curl unzip python3 python3-pip
```

## 3) Register self-hosted runner (repo-level)
In GitHub repo:
- Settings -> Actions -> Runners -> New self-hosted runner -> Linux/ARM64
- Follow generated commands on VM
- Add custom label: `oracle-kr-goyang`

## 4) Run runner as service
```bash
sudo ./svc.sh install
sudo ./svc.sh start
sudo ./svc.sh status
```

## 5) Trigger test
- Run GitHub Action: `Crawl Goyang & Notify`
- Expect logs to include `[GYT][STATS]` with non-zero `ok`.

## 6) Troubleshooting
- If job stays queued, label mismatch.
- If VM reboots, ensure service is active.
- If still all-empty/403, collect `gytennis-debug-goyang` artifact.
