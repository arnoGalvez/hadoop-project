#  Hadoop
## Compiler

```sh
mvn package
```

# Executer

```sh
yarn jar target/kmeans-nd.jar <absolute-path-input> <absolute-path-output> k c1 c2 c3
```

**Attention** : les colones commencent à **0** !!

# Interprétation

La sortie est dans le fichier `<absolute-path-output>/part-r-00000`.
Le fichier de sortie est le fichier d'entrée et le cluster attribué (qui commence à 0) est en dernière colonne.
