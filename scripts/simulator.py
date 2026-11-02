"""Drop small PaySim files into HDFS /stream/in, one per interval."""
import argparse
import subprocess
import time

import pandas as pd


def put_atomic(csv_text: str, name: str) -> None:
    """Write to /stream/tmp then rename into /stream/in (atomic)."""
    cmd = (
        f"hdfs dfs -put - /stream/tmp/{name} && "
        f"hdfs dfs -mv /stream/tmp/{name} /stream/in/{name}"
    )
    subprocess.run(
        ["docker", "exec", "-i", "hadoop-master", "sh", "-c", cmd],
        input=csv_text.encode(),
        check=True,
    )


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--source", default="data/paysim_stream.csv")
    p.add_argument("--rows", type=int, default=20, help="rows per file")
    p.add_argument("--interval", type=float, default=1.0, help="seconds between files")
    p.add_argument("--max-files", type=int, default=0, help="0 = until the end")
    p.add_argument("--start", type=int, default=0, help="first chunk index (to resume)")
    args = p.parse_args()

    df = pd.read_csv(args.source)
    n_chunks = (len(df) + args.rows - 1) // args.rows
    sent = 0

    for i in range(args.start, n_chunks):
        chunk = df.iloc[i * args.rows : (i + 1) * args.rows]
        t0 = time.time()
        put_atomic(chunk.to_csv(index=False), f"tx_{i:07d}.csv")
        sent += 1
        print(f"[{time.strftime('%H:%M:%S')}] sent tx_{i:07d}.csv ({len(chunk)} rows)", flush=True)
        if args.max_files and sent >= args.max_files:
            break
        time.sleep(max(0.0, args.interval - (time.time() - t0)))


if __name__ == "__main__":
    main()