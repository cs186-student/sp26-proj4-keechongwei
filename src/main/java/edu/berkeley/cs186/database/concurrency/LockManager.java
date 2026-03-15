package edu.berkeley.cs186.database.concurrency;

import edu.berkeley.cs186.database.Transaction;
import edu.berkeley.cs186.database.TransactionContext;

import java.util.*;

/**
 * LockManager maintains the bookkeeping for what transactions have what locks
 * on what resources and handles queuing logic. The lock manager should generally
 * NOT be used directly: instead, code should call methods of LockContext to
 * acquire/release/promote/escalate locks.
 *
 * The LockManager is primarily concerned with the mappings between
 * transactions, resources, and locks, and does not concern itself with multiple
 * levels of granularity. Multigranularity is handled by LockContext instead.
 *
 * Each resource the lock manager manages has its own queue of LockRequest
 * objects representing a request to acquire (or promote/acquire-and-release) a
 * lock that could not be satisfied at the time. This queue should be processed
 * every time a lock on that resource gets released, starting from the first
 * request, and going in order until a request cannot be satisfied. Requests
 * taken off the queue should be treated as if that transaction had made the
 * request right after the resource was released in absence of a queue (i.e.
 * removing a request by T1 to acquire X(db) should be treated as if T1 had just
 * requested X(db) and there were no queue on db: T1 should be given the X lock
 * on db, and put in an unblocked state via Transaction#unblock).
 *
 * This does mean that in the case of:
 *    queue: S(A) X(A) S(A)
 * only the first request should be removed from the queue when the queue is
 * processed.
 **/

public class LockManager {
    // transactionLocks is a mapping from transaction number to a list of lock
    // objects held by that transaction.
    private Map<Long, List<Lock>> transactionLocks = new HashMap<>();

    // resourceEntries is a mapping from resource names to a ResourceEntry
    // object, which contains a list of Locks on the object, as well as a
    // queue for requests on that resource.
    private Map<ResourceName, ResourceEntry> resourceEntries = new HashMap<>();

    // A ResourceEntry contains the list of locks on a resource, as well as
    // the queue for requests for locks on the resource.
    private class ResourceEntry {
        // List of currently granted locks on the resource.
        List<Lock> locks = new ArrayList<>();
        // Queue for yet-to-be-satisfied lock requests on this resource.
        Deque<LockRequest> waitingQueue = new ArrayDeque<>();

        // Below are a list of helper methods we suggest you implement.
        // You're free to modify their type signatures, delete, or ignore them.

        /**
         * Check if `lockType` is compatible with preexisting locks. Allows
         * conflicts for locks held by transaction with id `except`, which is
         * useful when a transaction tries to replace a lock it already has on
         * the resource.
         */
        public boolean checkCompatible(LockType lockType, long except) {
            // TODO(proj4_part1): implement
            if (locks.isEmpty()){
                return true;
            }
            for (Lock lock : locks) {
                if (lock.transactionNum == except) continue;
                if (!LockType.compatible(lock.lockType, lockType)) {
                    return false;
                }
            }
            return true;
        }

        /**
         * Gives the transaction the lock `lock`. Assumes that the lock is
         * compatible. Updates lock on resource if the transaction already has a
         * lock.
         */
        public void grantOrUpdateLock(Lock lock) {
            // TODO(proj4_part1): implement
            //return;

            boolean updated = false;

            for (int i = 0; i < locks.size(); i++) {
                Lock curr = locks.get(i);

                if (curr.transactionNum == lock.transactionNum) {
                    locks.set(i, lock);
                    updated = true;
                    break;
                }
            }

            if (!updated) {
                locks.add(lock);
            }

            List<Lock> transLocks = transactionLocks.get(lock.transactionNum);
            if (transLocks == null) {
                transLocks = new ArrayList<>();
                transactionLocks.put(lock.transactionNum, transLocks);
            }

            boolean replaced = false;
            for (int i = 0; i < transLocks.size(); i++) {
                if (transLocks.get(i).name.equals(lock.name)) {
                    transLocks.set(i, lock);
                    replaced = true;
                    break;
                }
            }

            if (!replaced) {
                transLocks.add(lock);
            }

        }

        /**
         * Releases the lock `lock` and processes the queue. Assumes that the
         * lock has been granted before.
         */
        public void releaseLock(Lock lock) {
            // TODO(proj4_part1): implement
            // remove lock from transactionLocks
            List<Lock> transLock = transactionLocks.get(lock.transactionNum);
            transLock.remove(lock);
            transactionLocks.put(lock.transactionNum,transLock);
            // remove lock from ResourceEntry
            for (int i = 0; i < locks.size(); i++){
                if (locks.get(i).transactionNum == lock.transactionNum){
                    locks.remove(i);
                    break;
                }
            }
            // process queue
            processQueue();
        }

        /**
         * Adds `request` to the front of the queue if addFront is true, or to
         * the end otherwise.
         */
        public void addToQueue(LockRequest request, boolean addFront) {
            // TODO(proj4_part1): implement
            if (addFront){
                waitingQueue.addFirst(request);
            }
            else{
                waitingQueue.addLast(request);
            }
        }

        /**
         * Grant locks to requests from front to back of the queue, stopping
         * when the next lock cannot be granted. Once a request is completely
         * granted, the transaction that made the request can be unblocked.
         */
        private void processQueue() {
            // TODO(proj4_part1): implement
            Iterator<LockRequest> requests = waitingQueue.iterator();
            while (!waitingQueue.isEmpty()){
                LockRequest lr = waitingQueue.peekFirst();
                if (checkCompatible(lr.lock.lockType,lr.transaction.getTransNum())){
                    grantOrUpdateLock(lr.lock);
                    for (Lock lock:lr.releasedLocks){
                        releaseLock(lock);
                    }
                    lr.transaction.unblock();
                    waitingQueue.removeFirst();
                }
                else{
                    break;
                }
            }
        }

        /**
         * Gets the type of lock `transaction` has on this resource.
         */
        public LockType getTransactionLockType(long transaction) {
            // TODO(proj4_part1): implement
            LockType type = null;
            for (Lock lock: locks){
                if (lock.transactionNum == transaction){
                    type = lock.lockType;
                }
            }
            return type;
        }

        @Override
        public String toString() {
            return "Active Locks: " + Arrays.toString(this.locks.toArray()) +
                    ", Queue: " + Arrays.toString(this.waitingQueue.toArray());
        }
    }

        // You should not modify or use this directly.
        private Map<String, LockContext> contexts = new HashMap<>();

        /**
         * Helper method to fetch the resourceEntry corresponding to `name`.
         * Inserts a new (empty) resourceEntry into the map if no entry exists yet.
         */
        private ResourceEntry getResourceEntry(ResourceName name) {
            resourceEntries.putIfAbsent(name, new ResourceEntry());
            return resourceEntries.get(name);
        }

        /**
         * Acquire a `lockType` lock on `name`, for transaction `transaction`, and
         * releases all locks on `releaseNames` held by the transaction after
         * acquiring the lock in one atomic action.
         * <p>
         * Error checking must be done before any locks are acquired or released. If
         * the new lock is not compatible with another transaction's lock on the
         * resource, the transaction is blocked and the request is placed at the
         * FRONT of the resource's queue.
         * <p>
         * Locks on `releaseNames` should be released only after the requested lock
         * has been acquired. The corresponding queues should be processed.
         * <p>
         * An acquire-and-release that releases an old lock on `name` should NOT
         * change the acquisition time of the lock on `name`, i.e. if a transaction
         * acquired locks in the order: S(A), X(B), acquire X(A) and release S(A),
         * the lock on A is considered to have been acquired before the lock on B.
         *
         * @throws DuplicateLockRequestException if a lock on `name` is already held
         *                                       by `transaction` and isn't being released
         * @throws NoLockHeldException           if `transaction` doesn't hold a lock on one
         *                                       or more of the names in `releaseNames`
         */
        public void acquireAndRelease(TransactionContext transaction, ResourceName name,
                                      LockType lockType, List<ResourceName> releaseNames)
                throws DuplicateLockRequestException, NoLockHeldException {
            // TODO(proj4_part1): implement
            // You may modify any part of this method. You are not required to keep
            // all your code within the given synchronized block and are allowed to
            // move the synchronized block elsewhere if you wish.
            boolean shouldBlock = false;
            synchronized (this) {
                Lock lock = new Lock(name,lockType,transaction.getTransNum());
                ResourceEntry resource = getResourceEntry(name);
                List<Lock> releasedLocks = new ArrayList<>();

                // Validate releaseNames and build releasedLocks
                for (ResourceName rn : releaseNames) {
                    ResourceEntry re = getResourceEntry(rn);
                    LockType lt = re.getTransactionLockType(transaction.getTransNum());

                    if (lt == null) {
                        throw new NoLockHeldException("Transaction holds no lock on " + rn);
                    }

                    releasedLocks.add(new Lock(rn, lt, transaction.getTransNum()));
                }

                // check if a lock on `name` is already held by `transaction`
                // and isn't being released
                if (!releaseNames.contains(name) &&
                        resource.getTransactionLockType(transaction.getTransNum()) != null) {
                    throw new DuplicateLockRequestException("Duplicate lock request");
                }
                if (resource.checkCompatible(lockType,transaction.getTransNum())){
                    resource.grantOrUpdateLock(lock);
                    // skip name that lock is acquired on as if the name is in releaseNames
                    // then grantOrUpdate would have promoted old lock already
                    for (Lock l : releasedLocks) {
                        if (!l.name.equals(name)) {
                            getResourceEntry(l.name).releaseLock(l);
                        }
                    }
                }
                else {
                    LockRequest lr = new LockRequest(transaction,lock,releasedLocks);
                    resource.addToQueue(lr,true);
                    transaction.prepareBlock();
                    shouldBlock = true;
                }

            }
            if (shouldBlock) {
                transaction.block();
            }
        }

        /**
         * Acquire a `lockType` lock on `name`, for transaction `transaction`.
         * <p>
         * Error checking must be done before the lock is acquired. If the new lock
         * is not compatible with another transaction's lock on the resource, or if there are
         * other transaction in queue for the resource, the transaction is
         * blocked and the request is placed at the **back** of NAME's queue.
         *
         * @throws DuplicateLockRequestException if a lock on `name` is held by
         *                                       `transaction`
         */
        public void acquire(TransactionContext transaction, ResourceName name,
                            LockType lockType) throws DuplicateLockRequestException {
            // TODO(proj4_part1): implement
            // You may modify any part of this method. You are not required to keep all your
            // code within the given synchronized block and are allowed to move the
            // synchronized block elsewhere if you wish.
            boolean shouldBlock = false;
            synchronized (this) {
                Lock lock = new Lock(name,lockType,transaction.getTransNum());
                ResourceEntry resource = getResourceEntry(name);
                if (resource.getTransactionLockType(transaction.getTransNum()) != null) {
                    throw new DuplicateLockRequestException("Duplicate lock request");
                }
                if (resource.checkCompatible(lockType,transaction.getTransNum()) && resource.waitingQueue.isEmpty()){
                    resource.grantOrUpdateLock(lock);
                }
                else{
                    LockRequest lr = new LockRequest(transaction,lock);
                    resource.addToQueue(lr,false);
                    transaction.prepareBlock();
                    shouldBlock = true;
                }
            }
            if (shouldBlock) {
                transaction.block();
            }
        }

        /**
         * Release `transaction`'s lock on `name`. Error checking must be done
         * before the lock is released.
         * <p>
         * The resource name's queue should be processed after this call. If any
         * requests in the queue have locks to be released, those should be
         * released, and the corresponding queues also processed.
         *
         * @throws NoLockHeldException if no lock on `name` is held by `transaction`
         */
        public void release(TransactionContext transaction, ResourceName name)
                throws NoLockHeldException {
            // TODO(proj4_part1): implement
            // You may modify any part of this method.
            synchronized (this) {
                ResourceEntry re = getResourceEntry(name);
                LockType lt = re.getTransactionLockType(transaction.getTransNum());

                if (lt == null) {
                    throw new NoLockHeldException("Transaction holds no lock on " + name);
                }
                Lock lock = new Lock(name,lt,transaction.getTransNum());
                re.releaseLock(lock);
            }
        }

        /**
         * Promote a transaction's lock on `name` to `newLockType` (i.e. change
         * the transaction's lock on `name` from the current lock type to
         * `newLockType`, if its a valid substitution).
         * <p>
         * Error checking must be done before any locks are changed. If the new lock
         * is not compatible with another transaction's lock on the resource, the
         * transaction is blocked and the request is placed at the FRONT of the
         * resource's queue.
         * <p>
         * A lock promotion should NOT change the acquisition time of the lock, i.e.
         * if a transaction acquired locks in the order: S(A), X(B), promote X(A),
         * the lock on A is considered to have been acquired before the lock on B.
         *
         * @throws DuplicateLockRequestException if `transaction` already has a
         *                                       `newLockType` lock on `name`
         * @throws NoLockHeldException           if `transaction` has no lock on `name`
         * @throws InvalidLockException          if the requested lock type is not a
         *                                       promotion. A promotion from lock type A to lock type B is valid if and
         *                                       only if B is substitutable for A, and B is not equal to A.
         */
        public void promote(TransactionContext transaction, ResourceName name,
                            LockType newLockType)
                throws DuplicateLockRequestException, NoLockHeldException, InvalidLockException {
            // TODO(proj4_part1): implement
            // You may modify any part of this method.
            boolean shouldBlock = false;
            synchronized (this) {
                Lock newLock = new Lock(name,newLockType,transaction.getTransNum());
                ResourceEntry resource = getResourceEntry(name);
                LockType curr = resource.getTransactionLockType(transaction.getTransNum());
                if (curr == newLockType){
                    throw new DuplicateLockRequestException("Duplicate Lock Request");
                }
                else if (curr == null){
                    throw new NoLockHeldException("No Lock Held on " + name);
                }
                else if (!LockType.substitutable(newLockType,curr)) {
                    throw new InvalidLockException("NewLockType is not substitutable for currently held lock");
                }
                if (resource.checkCompatible(newLockType,transaction.getTransNum())){
                    resource.grantOrUpdateLock(newLock);
                }
                else{
                    transaction.prepareBlock();
                    LockRequest lr = new LockRequest(transaction,newLock);
                    resource.addToQueue(lr,true);
                    shouldBlock = true;
                }

            }
            if (shouldBlock) {
                transaction.block();
            }
        }

        /**
         * Return the type of lock `transaction` has on `name` or NL if no lock is
         * held.
         */
        public synchronized LockType getLockType(TransactionContext transaction, ResourceName name) {
            // TODO(proj4_part1): implement
            ResourceEntry resourceEntry = getResourceEntry(name);
            LockType lt = resourceEntry.getTransactionLockType(transaction.getTransNum());
            if (lt == null){
                return LockType.NL;
            }
            else{
                return lt;
            }
        }

        /**
         * Returns the list of locks held on `name`, in order of acquisition.
         */
        public synchronized List<Lock> getLocks(ResourceName name) {
            return new ArrayList<>(resourceEntries.getOrDefault(name, new ResourceEntry()).locks);
        }

        /**
         * Returns the list of locks held by `transaction`, in order of acquisition.
         */
        public synchronized List<Lock> getLocks(TransactionContext transaction) {
            return new ArrayList<>(transactionLocks.getOrDefault(transaction.getTransNum(),
                    Collections.emptyList()));
        }

        /**
         * Creates a lock context. See comments at the top of this file and the top
         * of LockContext.java for more information.
         */
        public synchronized LockContext context(String name) {
            if (!contexts.containsKey(name)) {
                contexts.put(name, new LockContext(this, null, name));
            }
            return contexts.get(name);
        }

        /**
         * Create a lock context for the database. See comments at the top of this
         * file and the top of LockContext.java for more information.
         */
        public synchronized LockContext databaseContext() {
            return context("database");
        }
}
