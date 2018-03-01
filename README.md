# Projet de BigData

### Wanyanyuan Tang:LMFI   &  Mounia Bouhriz: SSSR

<br><br/>

## 1. Impala et Hive :

Préparation :


- On importe le fichier contenant les logs en HDFS :

 ```
hdfs dfs -put new_auth_10000000.txt /user/hive/warehouse/new_auth_10000000.txt
 ```
 
- On crée une table Project avec les champs correspondant retrouvés dans le fichier de logs : 

```
CREATE EXTERNAL TABLE project
(temps INT,
usersource STRING,
userdest STRING,
pcsource STRING,
pcdest STRING,
authtype STRING,
cnxtype STRING,
authorient STRING,
decision STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LOCATION '/user/hive/warehouse/new_auth_10000000.txt'
```

- On démarre les services nécessaires : 

```
sudo service zookeeper-server start
sudo service hive-server2 start
```

### 1) Le nombre de ligne de logs où les champs n’ont pas de valeur valide représentés par le symbole «? » : 

 ```
SELECT count FROM project WHERE usersource = '?' or userdest = '?' or pcsource = '?' or pcdest = '?' or cnxtype = '?' or authtype = '?' or authorient = '?' or decision = '?'
 ```
 
Remarque : On a remarqué que seuls les champs « cnxtype » et « authtype » contiennent le symbole « ? », on peut donc lancer la requête suivante également:
 
 ```
SELECT count FROM project WHERE cnxtype = '?' or authtype = '?'
 ```

⇒ Résultat : 5813280



### 2) Le nombre d’utilisateur qui se connectent au système :  

 ```
SELECT count( DISTINCT usersource) FROM project
 ```
<font color="blue"> 
⇒ Résultat : 32292
<font>

### 3) Calculer le nombre de connexions par utilisateur : 

Pour calculer ce nombre, on a considéré uniquement les connexions qui ont abouti, c’est à dire, les connexions avec la valeur « Success » dans le champs « decision »

 ```
SELECT usersource, count from projet WHERE decision ='Success' group by usersource 
 ```

⇒ Le résultat de cette requête est stocké dans le fichier « query-impala-117.csv » attaché au projet. 

Tri par ordre décroissant : 

 ```
SELECT usersource, count as number from project WHERE decision ='Success' group by usersource ORDER BY number DESC
 ```
⇒ Le résultat de cette requête est stocké dans le fichier « query-impala-118.csv ».

## 2. Spark 
Pour cette partie du projet, nous avons choisi d’utiliser le langage Scala. 

command:
 ```
spark-shell --packages graphframes:graphframes:0.1.0-spark1.6 -i BigData_Wanyanyuan_Mounia.scala
 ```
### 1.  lire le fichier de logs et le stocker dans la variable "file".

```
val file = sc.textFile("/user/hive/warehouse/new_auth_10000000.txt") 
 ```

### 2. supprimer les lignes qui contiennent un "?" et stocker le résultat dans la variable "cleanfile"
 ```
val cleanfile = file.filter(line => !(line.contains("?")))  
 ```
### 3.  calculer le nombre d'accès d'un utilisateur à une machine 
 ```val trifile = cleanfile.map(line=>line.split(",")).map(fields=>((fields(1),fields(3)),1)).reduceByKey((v1,v2) => v1+v2)    ```

### 4. Afficher les Top10 des accès les plus fréquents, on fait un sortby avec la valeur « False » pour trier les éléments du haut en bas, et un take(10) pour afficher uniquement les 10 premiers. 

 ```
 cleanfile.map(line=>line.split(",")).map(fields=>((fields(1),fields(3)),1)).reduceByKey((v1,v2) => v1+v2).sortBy(_._2,false).take(10)
 ```
 ### 5. Sauvegarder les résultats dans HDFS 
 
 ```
 trifile.saveAsTextFile("/home/cloudera/result")
 ```
 Récupérer le résultat du HDFS au disk local 
 
 ```
 hdfs dfs -get /home/cloudera/result result
 ```
⇒ Le résultat est stocké dans les fichiers : Part-00000, Part-00001, Part-00002, Part-00003, Part-00004, attachés au projet. 

### 6. Création de graphe : 

```
import org.graphframes.GraphFrame
 ```
On récupère le champs usersource et on met sa valeur dans Id, et son type « utilisateur » dans type. 
 
 ```
 val userfile = file.map(line=>line.split(",")).map(fields=>((fields(1),"utilisateur"))) 
 ```
on crée le vertex correspondant 
 
 ```
 val userfileg=userfile.toDF("id","type")
 ```
On récupère le champs pcsource et on met sa valeur dans Id, et son type «machine» dans type.
 
 ```
 val machinefile = file.map(line=>line.split(",")).map(fields=>((fields(3),"machine")))
 ```
on crée le vertex correspondant

 ```
 val machinefileg=machinefile.toDF("id","type")
 ```
on rassemble les deux parties dans un même vertex 

 ```
 val totalfile=userfileg.unionAll(machinefileg)
    val v=totalfile.toDF("id", "type").select("id","type").distinct()
 ```
Récupérer les éléments du fichier Trifile un à un et le mettre dans le fichier newtrifile 
 
 ```
 val newtrifile = trifile.map(fields=>(fields._1._1, fields._1._2, fields._2))
 ```
Créer les arcs et stocker le résultat dans la variable e
 
 ```
 val e=newtrifile.toDF("src","dst","weight")
 ```
Créer le graphe : 
 
 ```
 val g = GraphFrame(v,e) 
 ```
vérifier qu’on a bien créé les vertex et les arcs :
 
 ```
g.vertices.show()
g.edges.show()
```

### 7.Calcul et tri du nombre d’arcs entrants et sortants pour chaque nœud : 

```
val indre=g.inDegrees.sort(desc("inDegree"))
val outdre=g.outDegrees.sort(desc("outDegree"))
indre.rdd.map(_.toString()).saveAsTextFile("/home/cloudera/indre")
outdre.rdd.map(_.toString()).saveAsTextFile("/home/cloudera/outdre")
```

### 8. Calcul du nombre des composants conennexes de chaque sommet :

```
val component = g.connectedComponents.run()
component.select("id", "component").orderBy("component").show()
component.rdd.map(_.toString()).saveAsTextFile("/home/cloudera/component")
```

### 9. Calcul du nombre des composants fortement connexes de chaque sommet :

```
val result = g.stronglyConnectedComponents.maxIter(10).run()
result.select("id", "component").orderBy("component").show()
result.rdd.map(_.toString()).saveAsTextFile("/home/cloudera/result")
```
### 10. Calcul de la page rank de chaque sommet :


exécuter PageRank jusqu’à la convergence à la tolerance "tol":
```
val results = g.pageRank.resetProbability(0.15).tol(0.01).run()
```

afficher les pageranks résultant et le poids des arc final
```
results.vertices.select("id", "pagerank").show()
results.edges.select("src", "dst", "weight").show()
```

