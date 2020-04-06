### About
This repository contains the code for two concurrent deques written in Scala.\
One deque implementation is lock-based whereas the other one is lock-free. 
The lock-based implementation uses a doubly-linked list and achieves correctness by acquiring locks on all the nodes it wants to manipulate, before manipulating them. The lock-free implementation also uses a doubly-linked list. However, it achieves correctness through first logically deleting nodes before physically removing them and through helping mechanisms between threads.   
Both implementations have been extensively tested for correctness. \
A more extensive description of the underlying implementations and arguments for correctness can be found [here](https://github.com/arnegebert/deques/blob/master/documentation.pdf). These implementations were developed during my take home exam for the Concurrent Algorithms and Data Structures course and I wanted to share them here. 

### Dependencies and running the code
The source code contains a linearizability tester for the deque which makes use of the ox.cads linearizability testing library developed by Gavin Lowe. Documentation for the library can be found [here](http://www.cs.ox.ac.uk/people/gavin.lowe/LinearizabiltyTesting/). 
The implementation of the deques also makes use of Lock and AtomicPair classes as defined in the ox.cads library. (These could likely be replaced by classes from the [java.util.concurrent.atomic](https://docs.oracle.com/javase/7/docs/api/java/util/concurrent/atomic/package-summary.html) package if one only wants to run the deques, but not test them for linearizability).

The code was tested with Scala 2.13.0. The ox.cads library code found [here](http://www.cs.ox.ac.uk/people/gavin.lowe/LinearizabiltyTesting/) doesn't seem to be compatible with Scala 2.13.0, however, versions of the library compatible with Scala 2.12 and 2.13 are instead also available [here](https://www.cs.ox.ac.uk/teaching/materials19-20/cads/). 
To run the code, you need to download the source code for the ox.cads library, add it to your classpath and execute ```Deques.scala```. If all goes well, executing the code should result in one of the deque implementations being tested by the linearizability tester.
 
 
