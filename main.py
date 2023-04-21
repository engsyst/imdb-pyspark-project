from imdb.ioutil import load
from imdb.pipeline.task1 import task1


def main():
    task1()


def show_all():
    path = "resources/name.basics.tsv.gz"
    print(path)
    load(path).show(1, truncate=False)
    path = "resources/title.akas.tsv.gz"
    print(path)
    load(path).show(1, truncate=False)
    path = "resources/title.basics.tsv.gz"
    print(path)
    load(path).show(1, truncate=False)
    path = "resources/title.crew.tsv.gz"
    print(path)
    load(path).show(1, truncate=False)
    path = "resources/title.episode.tsv.gz"
    print(path)
    load(path).show(1, truncate=False)
    path = "resources/title.principals.tsv.gz"
    print(path)
    load(path).show(1, truncate=False)
    path = "resources/title.ratings.tsv.gz"
    print(path)
    load(path).show(1, truncate=False)


if __name__ == '__main__':
    main()
