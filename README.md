[![Build Status](https://travis-ci.org/jiujieti/CypherImplementation.svg?branch=master)](https://travis-ci.org/jiujieti/CypherImplementation)
# Cypher Implementation
This is the code for my master thesis in Eindhoven University of Technology. And in this project, a basic subset of Cypher (a graph query language, developed by [Neo4j](https://neo4j.com/)) clauses is implemented on Apach Flink. Besides,
two execution strategies for processing graph queries brought by us in the thesis are also implemented.

Please read the [thesis](thesis.pdf) for more details.

## Labeled Property Graph
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

## Basic Operators
Basic operators are defined to perform queries on a labeled property graph. All the queries performed on a graph database are based on graph pattern matching. After the execution of a basic operator, paths, each of which consists of IDs of all vertices and edges on it, will be returned (except the projection operator). Note that a path here may only contain one vertex ID, which is returned by a scan operator.

1. `Scan operator`: a scan operator is used to find all vertices by various types of [filtering conditions](#filtering-conditions).

2. `Edge-join operator`: an edge-join operator could expand the lengths of all previous resulting paths by two or drop them according to filtering conditions on the edge. It first extracts all the vertex IDs to be joined with the edge set in a graph instance. A join operator in Flink then is applied to combine the previous selected vertices with filtered edges. Meanwhile all target vertices of filtered edges are also kept in the paths for further selections.

3. `Label matching operator`: a label matching operator selects paths of a variable number of edges with the specified labels in the data graph.

4. `Join operator`: a join operator functions the same as the one in relational algebra, which joins two lists of paths by common vertex IDs.

5. `Union operator`: a union operator also functions the same as the one in relational algebra, which returns the union set of two lists of paths.

6. `Projection operator`: a projection operator matches edge identifiers or vertex identifiers with corresponding edges or vertices in the graph instance and then collects all these components.

The implementation of basic operators could be found [here](https://github.com/jiujieti/CypherImplementation/tree/master/src/main/java/operators). Note that an intersection operator is also implemented even though it is not mentioned in the thesis.

### Filtering Conditions

The filtering conditions consist of all [boolean expressions](https://github.com/jiujieti/CypherImplementation/tree/master/src/main/java/operators/booleanExpressions) and specific comparisons of properties.

Boolean expressions includes the following relations:

1. `AND`

2. `OR`

3. `XOR`

4. `NOT`

The following comparisons could be applied on property values:

1. `>`

2. `<`

3. `=`

4. `>=`

5. `<=`

6. `<>`

Note that so far the query optimizers only support conjunctive filtering conditions, namely `AND` boolean expressions.

## Query Execution Strategies
Two types of query execution strategies have been implemented, which could be found [here](https://github.com/jiujieti/CypherImplementation/tree/master/src/main/java/queryplan).
### Cost-based optimizer
The cost-based query optimization algorithm is mainly based on pre-computed statistical information about the datasets. The general idea here is to first collect statistical information, that the number of vertices and edges with a specific label and the proportion of this type taking up in the total number
and then utilize these statistics to estimate the cardinality of query graph components in the query graph.
### Rule-based optimizer
The rule-based optimizer to generate a query plan is to use heuristic rules to estimate the cardinality of query graph components. Mainly, the idea here is that using the selectivity of a basic query graph pattern to estimate the cardinality of graph components. Besides, the join strategies offered by Flink are also utilized to facilitate the query optimization.
## Tools
We use LDBC-SNB and gMark to generate our datasets. By setting the paths of files where the raw datasets are stored and running the class `GMarkDataGenerator` in package [gmark](https://github.com/jiujieti/CypherImplementation/tree/master/src/main/java/gmark) or the class `LDBCDataGenerator` in package [ldbc](https://github.com/jiujieti/CypherImplementation/tree/master/src/main/java/ldbc), the generated raw datasets will fit into our data model defined above.

Refer to [this link](https://github.com/s1ck/ldbc-flink-import) for a more decent way to import LDBC-SNB data into Gelly model provided by others.
### gMark
gMark is a dataset generator used in the project. Based on the design principles of gMark, gMark provides schema-driven generation of graphs and queries by utilizing a graph configuration. In term of generated graph instances, node types, edge types, both including proportions to the whole instance, and in- and out-degree distributions could all be defined by the users.

More information about [gMark](https://github.com/graphMark/gmark).
### LDBC-SNB
LDBC-SNB can simulates all activities of a user in a social network during a period of time and generate a synthetic social network. It is noticeable that the design principle of LDBC-SNB is not completely duplicating a real-life social network database, but concentrating on making the benchmark queries exhibit certain desired effects.

More information about [LDBC-SNB](http://ldbcouncil.org/developer/snb).

## How to Run a Test Example or a Query Plan Generator?
To run test examples provided in [test](https://github.com/jiujieti/CypherImplementation/tree/master/src/main/java/test) package or develop your own queries by utilizing a query plan generator, first you need to build Apache Flink from source and set up an IDE. Details of building Flink from source and setting up an IDE could be found by [this link](https://github.com/apache/flink). 

### Run a Test Example
After previous steps, you could use the following command to clone this project to your local directory. 

```
$ git clone https://github.com/jiujieti/CypherImplementation.git
```

Then open the project with an IDE and here we recommend IntelliJ IDEA. If you have already correctly followed previous steps, you could already run a provided test example. Note that some test examples have built the graph data model while others may need datasets input.

If you want to run a test example in a command prompt, refer to [this link](https://ci.apache.org/projects/flink/flink-docs-release-1.1/quickstart/setup_quickstart.html). 

### Run a Query Plan Generator
If you want to build your queries, you could use either the cost-based optimizer or the rule-based optimizer. To use these two optimizers, you need to specify the paths where the edge dataset and the vertex dataset are stored as well as input the query graph as shown in classes named `CostBasedOptimizerTest` and `RuleBasedOptimizerTest`.

A query graph requires to be constructed when inputting a query. The query graph is slightly different from the graph data model. A query vertex could be represented as:

```
QueryVertex(String: label, HashMap<String, String>: properties, boolean: isReturnedValue)
```
The member `label` and `properties` in a query vertex represent the specific label constraint and the property constraints on a vertex. The last member `isReturnedValue` is a boolean value, declaring whether this vertex should be returned or not.

A query edge is shown as

```
QueryEdge(QueryVertex: srcV, QueryVertex: tarV, String: label, HashMap<String, String>: properties)
```

`label` and `properties` here are the same as of the query vertices. A query edge also includes the source query vertex and the target query vertex. Then construct a query graph shown in the test examples, the query optimizers will take care of the optimization of the query.
