import argparse

import pandas as pd


def stratified_sample(df, frac, seed=42):
    """Tire la même fraction dans chaque classe (fraude / normale) : le taux de fraude est conservé."""
    return pd.concat([df[df.isFraud == k].sample(frac=frac, random_state=seed) for k in (0, 1)])


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--source", default="data/paysim.csv")
    p.add_argument("--stream-frac", type=float, default=0.2,
                   help="part de PaySim réservée au flux (jamais vue à l'entraînement)")
    p.add_argument("--train-rows", type=int, default=500000,
                   help="taille de l'échantillon d'entraînement (0 = tout le reste)")
    p.add_argument("--demo-normals", type=int, default=20000,
                   help="transactions normales ajoutées aux fraudes dans le fichier de démo")
    args = p.parse_args()

    df = pd.read_csv(args.source)
    print("PaySim :", len(df), "lignes,", int(df.isFraud.sum()), "fraudes")

    # 1. Flux : 20 % tirés au hasard, en gardant le même taux de fraude (stratifié)
    stream = stratified_sample(df, args.stream_frac)
    rest = df.drop(stream.index)

    # 2. Entraînement : échantillon stratifié du reste (aucune ligne du flux)
    if args.train_rows and args.train_rows < len(rest):
        frac = args.train_rows / float(len(rest))
        train = stratified_sample(rest, frac)
    else:
        train = rest

    # 3. Démo : toutes les fraudes du flux + un échantillon de normales, dans l'ordre du temps
    demo = pd.concat([stream[stream.isFraud == 1],
                      stream[stream.isFraud == 0].sample(n=min(args.demo_normals, len(stream)),
                                                         random_state=42)])

    train.sort_values("step").to_csv("data/paysim_train.csv", index=False)
    stream.sort_values("step").to_csv("data/paysim_stream.csv", index=False)
    demo.sort_values("step").to_csv("data/paysim_stream_demo.csv", index=False)

    assert not set(train.index) & set(stream.index), "recouvrement entraînement / flux"
    for name, part in [("entraînement", train), ("flux", stream), ("démo", demo)]:
        print("%-13s %9d lignes, %5d fraudes" % (name, len(part), int(part.isFraud.sum())))


if __name__ == "__main__":
    main()
