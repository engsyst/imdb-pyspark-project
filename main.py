from imdb.ioutil import load
from imdb.pipeline.task1 import task1
from imdb.pipeline.task2 import task2
from imdb.pipeline.task3 import task3
from imdb.pipeline.task4 import task4


def main():
    task1().show()
    task2().show()
    task3().show()
    task4().show()


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
