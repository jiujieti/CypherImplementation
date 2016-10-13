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

The `id` and the `properties` defined in an edge have similar meanings in those of a vertex. Note that an edge in a labeled property graph defined by Neo4j at most contains one label, thus the data type is defined as `String`. Besides, the `sourceId` and the `targetId` indicate the source vertex ID and the target vertex ID of this edge respectively. 

All these representations could be found [here](https://github.com/jiujieti/CypherImplementation/tree/master/src/main/java/operators/datastructures).

##Basic Operators
Basic operators are defined to perform queries on a labeled property graph. All the queries performed on a graph database are based on graph pattern matching. After the execution of a basic operator, paths, each of which consists of IDs of all vertices and edges on it, will be returned (except the projection operator). Note that a path here may only contain one vertex ID, which is returned by a scan operator.

1. `Scan operator`: a scan operator is used to find all vertices by various types of [filtering conditions](#filtering-conditions).

2. `Edge-join operator`: an edge-join operator could expand the lengths of all previous resulting paths by two or drop them according to filtering conditions on the edge. It first extracts all the vertex IDs to be joined with the edge set in a graph instance. A join operator in Flink then is applied to combine the previous selected vertices with filtered edges. Meanwhile all target vertices of filtered edges are also kept in the paths for further selections.

3. `Label matching operator`: a label matching operator selects paths of a variable number of edges with the specified labels in the data graph.

4. `Join operator`: a join operator functions the same as the one in relational algebra, which joins two lists of paths by common vertex IDs.

5. `Union operator`: a union operator also functions the same as the one in relational algebra, which returns the union set of two lists of paths.

6. `Projection operator`: a projection operator matches edge identifiers or vertex identifiers with corresponding edges or vertices in the graph instance and then collects all these components.

The implementation of basic operators could be found [here](https://github.com/jiujieti/CypherImplementation/tree/master/src/main/java/operators).

###Filtering Conditions

##Query Execution Strategies
Two types of query execution strategies have been implemented, which could be found [here](https://github.com/jiujieti/CypherImplementation/tree/master/src/main/java/queryplan).
###Cost-based optimizer
The cost-based query optimization algorithm is mainly based on pre-computed statistical information about the datasets. The general idea here is to first collect statistical information, that the number of vertices and edges with a specific label and the proportion of this type taking up in the total number
and then utilize these statistics to estimate the cardinality of query graph components in the query graph.
###Rule-based optimizer
The rule-based optimizer to generate a query plan is to use heuristic rules to estimate the cardinality of query graph components. Mainly, the idea here is that using the selectivity of a basic query graph pattern to estimate the cardinality of graph components. Besides, the join strategies offered by Flink are also utilized to facilitate the query optimization.
##Tools

###gMark
gMark is a dataset generator used in the project. Based on the design principles of gMark, gMark provides schema-driven generation of graphs and queries by utilizing a graph configuration. In term of generated graph instances, node types, edge types, both including proportions to the whole instance, and in- and out-degree distributions could all be defined by the users.

More information about [gMark](https://github.com/graphMark/gmark).
###LDBC-SNB
LDBC-SNB can simulates all activities of a user in a social network during a period of time and generate a synthetic social network. It is noticeable that the design principle of LDBC-SNB is not completely duplicating a real-life social network database, but concentrating on making the benchmark queries exhibit certain desired effects.

More information about [LDBC-SNB](http://ldbcouncil.org/developer/snb).

We use LDBC-SNB and gMark to generate our datasets. Since we have already defined our data model, namely the labeled property graph, then we need to transform generated raw datasets into our data structures. By setting the path of your files which store the raw datasets and running the class `GMarkDataGenerator` in package [gmark](https://github.com/jiujieti/CypherImplementation/tree/master/src/main/java/gmark) or the class `LDBCDataGenerator` in package [ldbc](https://github.com/jiujieti/CypherImplementation/tree/master/src/main/java/ldbc), the generated datasets will fit into our data model.
##How to Run a Test Example or a Query Plan Generator?
###Run a Test Example
###Run a Query Plan Generator
