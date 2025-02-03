# Rapport Groupe 1

## Membres

- Anto BENEDETTI
- Johan RIBEIRO--LAMY
- Romain NEROT
- Louis SEILLIER
- David ZHOU

## Jobs

- `no_udf.py`: traitement des données avec les fonctions natives de Spark
- `python_udf.py`: traitement des données avec une UDF pur Python
- `scala_udf.py`: traitement des données avec une UDF Scala

## Temps d'exécution des jobs

Tout d'abord, nous avons mesuré le temps d'exécution de chaque job avec la
fonction `perf_counter` du module `time` de Python.
Le problème est qu'avec la nature lazy de Spark, l'exécution de notre traitement
se fait uniquement lorsque nous en avons besoin, comme lors de l'écriture des
résultats dans des fichiers parquets.
Par conséquent, `perf_counter` ne mesure pas seulement l'exécution du schéma.
L'écriture des résultats dans les fichiers est également prise en compte lors du
benchmark.
Pour éviter ceci, nous avons cherché sur internet le moyen le plus précis pour
mesure le temps d'exécution d'un job Spark.
D'après la recherche, Spark UI serait l'outil le plus adapté.