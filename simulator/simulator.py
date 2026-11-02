import argparse
import subprocess
import time

import pandas as pd

CONTAINER = "fraud-hadoop-master"


def put_atomic(csv_text, name, container):
    cmd = ("hdfs dfs -put -f - /stream/tmp/{n} && "
           "hdfs dfs -mv /stream/tmp/{n} /stream/in/{n}").format(n=name)
    subprocess.run(["docker", "exec", "-i", container, "bash", "-c", cmd],
                   input=csv_text.encode("utf-8"), check=True)


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--source", default="data/paysim_stream.csv")
    p.add_argument("--rows", type=int, default=200, help="transactions par fichier")
    p.add_argument("--interval", type=float, default=2.0, help="secondes entre deux fichiers")
    p.add_argument("--max-files", type=int, default=0, help="0 = jusqu'à la fin du fichier")
    p.add_argument("--start", type=int, default=0, help="numéro du premier morceau (pour reprendre)")
    p.add_argument("--container", default=CONTAINER)
    args = p.parse_args()

    df = pd.read_csv(args.source)
    n_chunks = (len(df) + args.rows - 1) // args.rows
    run_id = time.strftime("%H%M%S")     # noms uniques d'une exécution à l'autre
    sent = 0
    print("Source : %s, %d transactions, %d fichiers de %d lignes"
          % (args.source, len(df), n_chunks, args.rows), flush=True)

    for i in range(args.start, n_chunks):
        t0 = time.time()
        chunk = df.iloc[i * args.rows:(i + 1) * args.rows]
        name = "tx_%s_%06d.csv" % (run_id, i)
        put_atomic(chunk.to_csv(index=False), name, args.container)
        sent += 1
        print("[%s] %s envoyé (%d transactions, %d fraudes)"
              % (time.strftime("%H:%M:%S"), name, len(chunk), int(chunk.isFraud.sum())), flush=True)
        if args.max_files and sent >= args.max_files:
            break
        time.sleep(max(0.0, args.interval - (time.time() - t0)))


if __name__ == "__main__":
    main()
