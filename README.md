# Database-Engine

## Overview

This project is a mini database engine implemented in Java. It provides basic database operations such as insertions, deletions, updates, and select operations. The project also uses B+Trees to optimize the performance of select queries.

- Supports 3 datatypes (Strings, Integers, Doubles).
- Stores tables, pages and indicies in `serialized` object files.
- Stores `page ranges` (min and max clustering key of each page) for each table.
- Supports `fast equality and range queries` by maintaining a balanced `B+Tree` on desired column.
- Saves metadata about tables in a CSV format, and uses `Singleton design pattern` to maintain a single instance of the `Metadata class` throughout its usage.

## Supported Operations

- Insertions
  - Runs in `O(N * log N)` to keep tuples sorted based on the table `clustering key`.
  - Finds appropriate insertion position by binary search on page ranges.
- Deletions
  - Runs in `O(log N)` when deleting by clustering key or an indexed column.
  - Runs in `O(N)` otherwise.
- Updates
  - Runs in `O(N)` to search for the tuple and update it.
- Select
  - Time complexity of `O(K + log N)` for range queries on indexed columns.
  - N represents the total number of data items stored in the B+Tree.
  - K represents the number of data items found within the specified range.
  - Supports operators (=, !=, >, >=, <, <=) for each condition, and logical operators (AND, OR, XOR) between multiple conditions.

## Technologies used

1. **Java**
2. **Maven**
