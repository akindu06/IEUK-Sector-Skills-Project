#!/usr/bin/env python3
import argparse, re
import pandas as pd

LOG_RE = re.compile(
    r'(?P<ip>\S+)\s+-\s+(?P<country>\S+)\s+-\s+'
    r'\[(?P<raw_time>[^\]]+)\]\s+'
    r'"(?P<method>\S+)\s+(?P<path>\S+)\s+(?P<protocol>[^"]+)"\s+'
    r'(?P<status>\d{3})\s+'
    r'(?P<size>\d+)\s+'
    r'"(?P<ref>[^"]*)"\s+'
    r'"(?P<agent>[^"]*)"\s+'
    r'(?P<resp_time>\d+)'
)

def load_and_parse(path: str) -> pd.DataFrame:
    rows = []
    with open(path, encoding="utf8") as f:
        for i, line in enumerate(f, 1):
            m = LOG_RE.match(line)
            if not m:
                # uncomment if you want to see unparsed lines:
                # print(f"WARN line {i} unparsable: {line.strip()}")
                continue
            rows.append(m.groupdict())
    if not rows:
        raise RuntimeError("No log lines parsed. Check format?")
    df = pd.DataFrame(rows)
    df['time'] = pd.to_datetime(df['raw_time'], format='%d/%m/%Y:%H:%M:%S')
    df['status'] = df['status'].astype(int)
    df['size'] = df['size'].astype(int)
    df['resp_time'] = df['resp_time'].astype(int)
    return df

def top_ip_counts(df, n=10):
    return df['ip'].value_counts().head(n)

def slowest_requests(df, n=10):
    return df.nlargest(n, 'resp_time')[['ip','time','path','resp_time']]

def per_minute_counts(df):
    return df.set_index('time').groupby('ip').resample('1T').size().reset_index(name='count')

def peak_rate_per_ip(df):
    per_min = per_minute_counts(df)
    peak = per_min.groupby('ip')['count'].max().sort_values(ascending=False)
    return peak

def user_agent_diversity(df, n=10):
    # number of distinct user-agents per IP
    ua = df.groupby('ip')['agent'].nunique().sort_values(ascending=False)
    return ua.head(n)

def top_paths(df, n=10):
    return df['path'].value_counts().head(n)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('logfile', help='Path to raw text log file')
    ap.add_argument('--rpm-threshold', type=int, default=100,
                    help='Requests per minute threshold for bot flagging (default 100)')
    args = ap.parse_args()

    df = load_and_parse(args.logfile)

    print("\n▶ Total log lines parsed:", len(df))

    print("\n▶ Top 10 IPs by request count:")
    print(top_ip_counts(df), "\n")

    print("▶ Top 10 Slowest Requests:")
    print(slowest_requests(df), "\n")

    print("▶ Top 10 Paths (most requested):")
    print(top_paths(df), "\n")

    print("▶ User-Agent diversity (distinct UAs per IP):")
    print(user_agent_diversity(df), "\n")

    peak = peak_rate_per_ip(df)
    print("▶ Peak requests-per-minute (RPM) per IP (top 10):")
    print(peak.head(10), "\n")

    bots = peak[peak > args.rpm_threshold].index.tolist()
    print(f"▶ IPs exceeding {args.rpm_threshold} RPM: {len(bots)} found")
    for ip in bots:
        print("  -", ip)

if __name__ == "__main__":
    main()
