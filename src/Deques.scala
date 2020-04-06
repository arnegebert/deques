import ox.cads.atomic.AtomicPair
import ox.cads.locks.Lock
import ox.cads.testing._
import ox.cads.util.ThreadID

// Trait implemented by the deques
trait Deque[T]{
  def addFirst(x: T) : Unit
  def addLast(x: T) : Unit
  def removeFirst : Option[T]
  def removeLast : Option[T]
}

/*
* A concurrent lock-based deque using a doubly-linked list.
* The locking is fine-grained, that is, for a single operation at most 3 nodes will be locked.
* However, in almost all cases, this variant is going to be strictly worse (slower) than the lockfree version below.
*  */
class LockbasedDeque[T] extends Deque[T]{
  // A node in the doubly linked-list
  class Node(val value: T) {
    @volatile var prev: Node = null
    @volatile var next: Node = null
    val l: Lock = Lock.FairLock
    def lock = { l.lock }
    def unlock = { l.unlock }
  }

  var head = new Node(null.asInstanceOf[T])
  var tail = new Node(null.asInstanceOf[T])
  head.next = tail
  tail.prev = head

  // Attempt to add a node to the start of the deque and return once it has been inserted.
  def addFirst(x: T) : Unit = {
    val node = new Node(x)
    while (true){
      val prev = head
      val next = head.next
      lockAll(prev, next)
      // the successor of head has changed inbetween locking - try again
      if (prev.next != next) unlockAll(prev, next)
      else { // actually insert node and release locks
        insertNewNode(prev, next, node)
        unlockAll(prev, next)
        return
      }
    }
  }

  // Attempt to add a node to the end of the deque and return once it has been inserted.
  def addLast(x: T) : Unit = {
    val node = new Node(x)
    while (true){
      val prev = tail.prev
      val next = tail
      lockAll(prev, next)
      // the predecessor of tail has changed inbetween locking - try again
      if (!(prev.next == next && next.prev == prev)) unlockAll(prev, next)
      else {// actually insert node and release locks
        insertNewNode(prev, next, node)
        unlockAll(prev, next)
        return
      }
    }
  }

  // Attempt to remove a node from the start of the deque and return its value once it has been removed.
  def removeFirst : Option[T] = {
    while (true){
      val next = head.next.next
      if (next == null) return None;
      val toRmv = next.prev
      val prev = toRmv.prev
      if (prev == head){
        lockAll(prev, toRmv, next)
        // locked nodes are no longer in succession - try again
        if (!(prev.next == toRmv && toRmv.next == next && next.prev == toRmv))
          unlockAll(prev, toRmv, next)
        else {// actually remove node and release locks
          val toReturn = rmvNode(prev, toRmv, next)
          unlockAll(prev, toRmv, next)
          return toReturn
        }
      }
    }
    throw new IllegalStateException("Can never occur but makes the compiler happy")
  }

  // Attempt to remove a node from the end of the deque and return its value once it has been removed.
  def removeLast : Option[T] = {
    while (true){
      val prev = tail.prev.prev
      if (prev == null) {return None;} // list only has 2 members
      val toRmv = prev.next
      val next = toRmv.next
      if (next == tail) {
        lockAll(prev, toRmv, next)
        // locked nodes are no longer in succession - try again
        if (!(prev.next == toRmv && toRmv.next == next && toRmv.prev == prev && next.prev == toRmv)) {
          unlockAll(prev, next, toRmv)
        } else {// actually remove node and release locks
          val toReturn = rmvNode(prev, toRmv, next)
          unlockAll(prev, toRmv, next)
          return toReturn
        }
      }
    }
    throw new IllegalStateException("Can never occur but makes the compiler happy")
  }

  def lockAll(prev: Node, toRmv: Node, next: Node): Unit = {
    prev.lock
    toRmv.lock
    next.lock
  }

  def lockAll(prev: Node, next: Node): Unit = {
    prev.lock
    next.lock
  }

  def unlockAll(prev: Node, toRmv: Node, next: Node): Unit = {
    next.unlock
    toRmv.unlock
    prev.unlock
  }

  def unlockAll(prev: Node, next: Node): Unit = {
    next.unlock
    prev.unlock
  }

  // Standard removal of a node within a doubly-linked list
  def rmvNode(prev: Node, toRmv: Node, next: Node): Option[T] = {
    prev.next = next
    next.prev = prev
    Some(toRmv.value)
  }

  // Standard insertion of a new node within a doubly-linked list
  def insertNewNode(prev: Node, next: Node, node: Node): Unit ={
    node.next = next
    node.prev = prev
    prev.next = node
    next.prev = node
  }
}

/*
* A concurrent lock-free deque which is based on a doubly-linked list and using single compare-and-set operations.
* In contrast to the lockbased version, each node now has a .mark-attribute, which is only set to true
* once a node has been logically deleted. Once a node is logically deleted, we know that unmarked nodes
* should no longer point to it. Only once no unmarked nodes no longer point to a marked node, it is physically deleted.
* This logical deletion, along with helping between threads, help us achieve the lock-free property.
 */
class LockFreeDeque[T] extends Deque[T]{

  // A node in the doubly linked-list
  class Node(val value: T){
    val link = new AtomicPair[(Node,Node), Boolean] ((null.asInstanceOf[Node], null.asInstanceOf[Node]), false)

    def prev = link.getFirst._1
    def next = link.getFirst._2
    def mark = link.getSecond
    def getPrevAndNext = link.getFirst
    def setPrevAndNext(prev: Node, next: Node): Unit = link.set((prev, next), mark)
  }

  var head = new Node(null.asInstanceOf[T])
  var tail = new Node(null.asInstanceOf[T])
  head.link.set((head.prev, tail), false)
  tail.link.set((head, tail.next), false)

  // Attempt to add a node to the start of the deque and return once it has been inserted.
  def addFirst(x: T) : Unit = {
    val node = new Node(x)
    val prev = head
    var next = prev.next
    while (true){
      node.setPrevAndNext(prev, next)
      // if CAS succeeds, we were able to insert the node
      if (CAS(prev, "next", next, node)) {
        updateAndGetPrev(next)
        return
      }// else: try again
      else next = prev.next
    }
    throw new IllegalStateException("Can never occur but makes the compiler happy")
  }

  // Attempt to add a node to the end of the deque and return once it has been inserted.
  def addLast(x: T) : Unit = {
    val node = new Node(x)
    val next = tail
    var prev = tail.prev
    while (true) {
      node.setPrevAndNext(prev, next)
      // if CAS succeeds, we were able to insert the node
      if (CAS(prev, "next", next, node)) {
        updateAndGetPrev(next)
        return
      } // else: try again
      else prev = updateAndGetPrev(next)
    }
    throw new IllegalStateException("Can never occur but makes the compiler happy")
  }

  // Attempt to remove a node from the start of the deque and return once it has been physically removed.
  def removeFirst : Option[T]= {
    while (true) {
      val toRmv = head.next
      // deque is empty
      if (toRmv == tail) return None
      // node after head is already marked, but not yet physically removed - help with that
      if (toRmv.mark) skipOverNode(toRmv)
      else {
        // attempt to mark the to-be-removed node
        if (CAS(toRmv, "mark", null, null)){
          // actually remove the to-be-removed node
          skipOverNode(toRmv)
          updateAndGetPrev(toRmv.next)
          return Some(toRmv.value)
        }
      }
    }
    throw new IllegalStateException("Can never occur but makes the compiler happy")
  }

  // Attempt to remove a node from the end of the deque and return once it has been physically removed.
  def removeLast : Option[T]= {
    while (true) {
      val toRmv = updateAndGetPrev(tail)
      // deque is empty
      if (toRmv == head) return None
      // attempt to mark the to-be-removed node
      if (CAS(toRmv, "mark", null, null)){
        // actually remove the to-be-removed node
        skipOverNode(toRmv)
        updateAndGetPrev(tail)
        return Some(toRmv.value)
      }
    }
    throw new IllegalStateException("Can never occur but makes the compiler happy")
  }

  // Takes a node and only returns once the node is no longe reachable from the .next-path starting at head
  def skipOverNode(node: Node): Unit = {
    // initial guess for the predecessor (might be wrong/outdated)
    var predGuess= node.prev
    var next= node.next
    while (true) {
      // predGuess was advanced so far that it points to next -- can terminate
      if (predGuess == next) return
      if (next.mark) next = next.next
      else {
        predGuess = updatePredGuess(predGuess, node)
        // attempt to actually update the pointer of the predecessor
        if (predGuess.next==node && CAS(predGuess, "next", node, next)) return
      }
    }
  }

  // A wrapper function around the compareAndSet-operation on AtomicPairs
  // Depending on the manner, attempt to update the .mark-attribute, .prev- or .next-pointer of node 'on'
  def CAS(on: Node, manner: String, before: Node, after: Node): Boolean = {
    if (manner=="mark") return on.link.compareAndSet((on.getPrevAndNext, false), (on.getPrevAndNext, true))
    else if (manner=="prev") return on.link.compareAndSet(((before, on.next), false), ((after, on.next), false))
    else if (manner=="next") return on.link.compareAndSet(((on.prev, before), false), ((on.prev, after), false))
    else throw new IllegalArgumentException("Not recognized 'manner' given as argument.")
  }

  // Find the predecessor for a node and update its .prev-pointer to it.
  // Return if update has been successful or if the original node was marked.
  def updateAndGetPrev(node: Node): Node = {
    var predGuess = node.prev
    while (true){
      // if the node is marked, one no longer need to update its pointers
      if (node.mark) return predGuess
      predGuess = updatePredGuess(predGuess, node)
      // actual attempt to update the .prev-pointer of node
      if (predGuess.next == node && CAS(node, "prev", node.prev, predGuess))
        return predGuess
    }
    throw new IllegalStateException("Can never occur but makes the compiler happy")
  }

  // Attempt to advance the guess for the predecessor of a node
  def updatePredGuess(predGuess: Node, node: Node): Node = {
    val next = predGuess.next;
    if (predGuess.mark) return predGuess.prev // backtracking
    if (next != node) return next // advancing
    else return predGuess // done
  }
}

// Code for linearization tester
object DequeTester {
  var ops = 20 // Number of operations done by each worker
  val MaxVal = 100 // Maximum value placed in the queue
  // probability to execute a certain operation on the queue
  val addFirstProb = 0.2
  val addLastProb = 0.2
  val rmvFirstProb = 0.3
  val rmvLastProb = 0.3
  assert(addFirstProb+addLastProb+rmvFirstProb+rmvLastProb == 1, "Probabilities have to add up to 1")

  val useLockfreeDeque = true

  // Scala immutable List used to simulate a deque;
  // rmvFirst: headOption + drop(1), rmvLast: lastOption + dropRight(1)
  // addLast(x): appended(x), addFirst(x): prepended(x)
  type seqDeque = scala.collection.immutable.List[Int]
  type concDeque = Deque[Int]

  // Define what effect a sequential operation on the concurrent deque would have
  // For each operation, define the return value and the state of the List after the operation
  def seqAddFirst(x: Int)(list: List[Int]): (Unit, List[Int]) = {
    ((), list.prepended(x))
  }

  def seqAddLast(x: Int)(list: List[Int]): (Unit, List[Int]) = {
    ((), list.appended(x))
  }

  def seqRmvFirst(list: List[Int]): (Option[Int], List[Int]) = {
    (list.headOption, list.drop(1))
  }

  def seqRmvLast(list: List[Int]): (Option[Int], List[Int]) = {
    (list.lastOption, list.dropRight(1))
  }

  // Worker for the testing framework based on an immutable sequential datatype
  def worker(me: Int, log: GenericThreadLog[seqDeque, concDeque]) = {
    val random = new scala.util.Random(scala.util.Random.nextInt)
    for (i <- 0 until ops) {
      val p = random.nextFloat
      val x = random.nextInt(MaxVal) + 1
      if (p <= addFirstProb) { // choose which operation to execute next
        log.log(_.addFirst(x), "addFirst(" + x + ")", seqAddFirst(x))
      } else if (p <= addFirstProb + addLastProb) {
        log.log(_.addLast(x), "addLast(" + x + ")", seqAddLast(x))
      } else if (p <= addFirstProb + addLastProb + rmvFirstProb) {
        log.log(_.removeFirst, "removeFirst", seqRmvFirst)
      } else {
        log.log(_.removeLast, "removeLast", seqRmvLast)
      }
    }
  }

  def resultValid(result: Int): Boolean = result > 0

  def main(args: Array[String]) = {
    val reps = 10000 // Number of repetitions
    val noWorkers = 4 // Number of workers
    val start = java.lang.System.nanoTime
    var repsDone = 0; var result = 1
    while (repsDone < reps && resultValid(result)) {
      val seqD = List[Int]()
      val conD: concDeque = if (useLockfreeDeque) new LockFreeDeque[Int] else new LockbasedDeque[Int]
      val tester = LinearizabilityTester.JITGraph[seqDeque, concDeque](seqD, conD, noWorkers, worker, ops)
      result = tester()
      repsDone += 1
      if (repsDone % 100 == 0) print(".")
    }
    println("\nTime taken: " + (java.lang.System.nanoTime - start) / 1000000 + "ms")
  }
}
