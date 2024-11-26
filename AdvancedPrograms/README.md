
# Breadth-First Search with Spark

This program(degrees-of-separation.py) demonstrates the Breadth-First Search (BFS) algorithm implemented in PySpark. BFS is used here to find the degree of separation between two characters in a graph of Marvel superheroes.

## Key Idea of BFS
- BFS explores all neighbors of a node at one distance before moving to the next level.
- Each node in the graph is represented as a tuple: `(heroID, (connections, distance, color))`:
  - **connections**: A list of connected nodes (neighbors).
  - **distance**: The shortest distance from the starting node (initialized to 0 for the start and 9999 for others).
  - **color**: Indicates the state of the node:
    - **WHITE**: Node not visited.
    - **GRAY**: Node is being visited (neighbors to be processed).
    - **BLACK**: Node fully processed (all neighbors processed).

## Steps of BFS Execution

### 1. Initialization
- `createStartingRdd` creates the initial RDD from the graph dataset.
- For the `startCharacterID` (Spider-Man), distance is set to `0` and color to `GRAY`.
- All other nodes are initialized with infinite distance (`9999`) and color `WHITE`.

### 2. Iterative BFS Logic
For each iteration (up to 10 or until the target is found):

1. **Mapping Phase** (`bfsMap` function):
    - Each `GRAY` node (node being visited) generates new nodes for its neighbors.
    - If the neighbor is the `targetCharacterID`, the `hitCounter` accumulator is incremented.
    - The original node is retained with its updated color (`BLACK`).

2. **Reducing Phase** (`bfsReduce` function):
    - Combines all emitted entries for the same `characterID`.
    - Merges connections, keeps the shortest distance, and assigns the "darkest" color.

### 3. Termination
- If `hitCounter` > 0 during any iteration, it means the target node has been reached.
- The program stops and outputs the number of paths leading to the target.

## Detailed Explanation of `map` and `reduce` Functions

### Mapping Phase (`bfsMap`)
This is the heart of BFS expansion. Each `GRAY` node generates new nodes for its neighbors and retains itself for further updates.

- **Inputs**: A single node `(characterID, (connections, distance, color))`.
- **Process**:
    - If the node's color is `GRAY`:
        - For each neighbor in `connections`:
            - Emit a new node with:
                - `connections` as an empty list (we only care about distances/colors for now).
                - `distance` incremented by 1.
                - `color` as `GRAY` (to process this node in the next iteration).
            - If the neighbor is the `targetCharacterID`, increment `hitCounter`.
        - Change the current node's color to `BLACK` (fully processed).
    - Append the current node (with updated color) to the output list.
- **Outputs**: A list of new and modified nodes.

### Reducing Phase (`bfsReduce`)
This merges all emitted nodes for the same `characterID` and ensures the correct state is preserved.

- **Inputs**: Two emitted nodes for the same `characterID`.
- **Process**:
    - Combine `connections` (union of all edges seen for this node).
    - Keep the smallest `distance` among the inputs (shortest path).
    - Retain the "darkest" color in this order: `BLACK > GRAY > WHITE`.
- **Output**: A single tuple representing the merged node.

## How BFS Works Iteratively

1. **Iteration 1**:
   - Starts with the `startCharacterID` as `GRAY`.
   - Its neighbors are emitted as new nodes with `distance = 1` and `color = GRAY`.
   - The starting node is updated to `BLACK`.

2. **Iteration 2**:
   - Newly added `GRAY` nodes (neighbors from the previous iteration) are processed.
   - Their neighbors are emitted as new nodes with `distance = 2`.
   - Current `GRAY` nodes are turned `BLACK`.

3. **Subsequent Iterations**:
   - Each iteration processes the next layer of neighbors.
   - Updates nodes to the shortest distance and darkest color.

4. **Termination**:
   - If the `hitCounter` detects the `targetCharacterID`, the loop exits early.
   - Otherwise, after 10 iterations, the search stops.

## Why Map and Reduce Are Effective
- **Parallelism**:
    - `bfsMap` can independently process each node in parallel, generating outputs for each neighbor.
    - `bfsReduce` combines emitted nodes for the same ID in parallel.
- **Scalability**:
    - Spark handles a large graph efficiently by distributing the processing across the cluster.

## Running the Program
1. Ensure the dataset (graph representation of Marvel superheroes) is available at the specified path.
2. Run the program using Spark. The result will indicate whether the `targetCharacterID` is reachable and how many paths exist to it.

