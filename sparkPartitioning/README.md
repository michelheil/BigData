# Content of sparkPartitioning
This little project should give an idea on how to do custom partitioning and what benefits it has.

## Steps
* Create an RDD with test data distributed over 3 partitions
* Assign a key out of 1, 2, or 3 to each value independent of their partition
* Test A: Process the values per key
* Test B: Repartition the RDD based on the key and perform identical process
* Measure time for Test A and Test B and compare the results 
  