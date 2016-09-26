#Cypher Implementation
This is the code for my master thesis in Eindhoven University of Technology. And in this project, a basic subset of Cypher (a graph query language, developed by [Neo4j](https://neo4j.com/)) clauses is implemented on Apach Flink. Besides,
two execution strategies for processing graph queries brought by us in the thesis are also implemented.

Please read the [thesis](thesis.pdf) for more details.

##Labeled Property Graph
A labeled property graph consists of a list of edges and a list of vertices, both with labels and properties. We extend the representation of a graph in Flink graph API, [Gelly](https://ci.apache.org/projects/flink/flink-docs-release-1.1/apis/batch/libs/gelly.html).
To be specific, a vertex is a 3-tuple shown as:

```
vertex(id: long, labels: Set<String>, properties: Map<String, String>)
```

Here the `id` indicates the unique ID of a vertex in a graph model. The `labels` represent all labels within a vertex. And the `properties` are comprised of all key-value pairs indicting the features of a vertex.

Similarly an edge is a 5-tuple:

```
edge(id: long, sourceId: long, targetId: long, label: String, properties: Map<String, String>)
```

The `id` and the `properties` defined in an edge have similar meanings in those of a vertex. Note that an edge in a labeled property graph defined by Neo4j at most contains one label, thus the data type is defined as `String`. Besides, the `sourceId`

All these representations could be found [here](https://github.com/jiujieti/CypherImplementation/tree/master/src/main/java/operators/datastructures).

##Basic Operators
Basic operators are defined to perform queries on a labeled property graph. All the queries performed on a graph database are based on graph pattern matching. After the execution of a basic operator, paths, each of which consists of IDs of all vertices and edges on it, will be returned (except the projection operator). Note that a path here may only contain one vertex ID, which is returned by a scan operator.

1. Scan operator:
A scan operator is used to find all vertices by various types of [filtering conditions](#filtering-conditions).

2. Edge-join operator:
An edge-join operator could expand the lengths of all previous resulting paths by two or drop them according to filtering conditions on the edge. It would first extract all the vertex IDs to be joined with the edge set in a graph instance. A join operator in Flink then would be applied to combine the previous selected vertices with filtered edges. Meanwhile all target vertices of filtered edges would also be kept in  in the paths for further selections.

3. Label matching operator:

4. Join operator:

5. Union operator:

The implementation of basic operators could be found [here](https://github.com/jiujieti/CypherImplementation/tree/master/src/main/java/operators).
###Filtering Conditions


##Query Execution Strategies

###Cost-based Optimizer
###Rule-based Optimizer

##Tools

###gMark
###LDBC

##How to Run a Test Example or a Query Plan Generator?
###Run a Test Example
###Run a Query Plan Generator