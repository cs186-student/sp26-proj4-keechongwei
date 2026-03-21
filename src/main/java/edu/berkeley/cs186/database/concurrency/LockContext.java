package edu.berkeley.cs186.database.concurrency;

import edu.berkeley.cs186.database.TransactionContext;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * LockContext wraps around LockManager to provide the hierarchical structure
 * of multigranularity locking. Calls to acquire/release/etc. locks should
 * be mostly done through a LockContext, which provides access to locking
 * methods at a certain point in the hierarchy (database, table X, etc.)
 */
public class LockContext {
    // You should not remove any of these fields. You may add additional
    // fields/methods as you see fit.

    // The underlying lock manager.
    protected final LockManager lockman;

    // The parent LockContext object, or null if this LockContext is at the top of the hierarchy.
    protected final LockContext parent;

    // The name of the resource this LockContext represents.
    protected ResourceName name;

    // Whether this LockContext is readonly. If a LockContext is readonly, acquire/release/promote/escalate should
    // throw an UnsupportedOperationException.
    protected boolean readonly;

    // A mapping between transaction numbers, and the number of locks on children of this LockContext
    // that the transaction holds.
    protected final Map<Long, Integer> numChildLocks;

    // You should not modify or use this directly.
    protected final Map<String, LockContext> children;

    // Whether or not any new child LockContexts should be marked readonly.
    protected boolean childLocksDisabled;

    public LockContext(LockManager lockman, LockContext parent, String name) {
        this(lockman, parent, name, false);
    }

    protected LockContext(LockManager lockman, LockContext parent, String name,
                          boolean readonly) {
        this.lockman = lockman;
        this.parent = parent;
        if (parent == null) {
            this.name = new ResourceName(name);
        } else {
            this.name = new ResourceName(parent.getResourceName(), name);
        }
        this.readonly = readonly;
        this.numChildLocks = new ConcurrentHashMap<>();
        this.children = new ConcurrentHashMap<>();
        this.childLocksDisabled = readonly;
    }

    /**
     * Gets a lock context corresponding to `name` from a lock manager.
     */
    public static LockContext fromResourceName(LockManager lockman, ResourceName name) {
        Iterator<String> names = name.getNames().iterator();
        LockContext ctx;
        String n1 = names.next();
        ctx = lockman.context(n1);
        while (names.hasNext()) {
            String n = names.next();
            ctx = ctx.childContext(n);
        }
        return ctx;
    }

    /**
     * Get the name of the resource that this lock context pertains to.
     */
    public ResourceName getResourceName() {
        return name;
    }

    /**
     * Acquire a `lockType` lock, for transaction `transaction`.
     *
     * Note: you must make any necessary updates to numChildLocks, or else calls
     * to LockContext#getNumChildren will not work properly.
     *
     * @throws InvalidLockException if the request is invalid
     * @throws DuplicateLockRequestException if a lock is already held by the
     * transaction.
     * @throws UnsupportedOperationException if context is readonly
     */
    public void acquire(TransactionContext transaction, LockType lockType)
            throws InvalidLockException, DuplicateLockRequestException {
        // TODO(proj4_part2): implement
        // check if trying to acquire NL lock
        if (lockType == LockType.NL){
            throw new InvalidLockException("Cannot request NL lock on resource");
        }
        // check if LockContext is readonly
        if (this.readonly){
            throw new UnsupportedOperationException("Lock Context is Read Only");
        }
        // check existing lock that transaction holds
        LockType currLockTypeHeld = this.lockman.getLockType(transaction,this.name);
        // check if duplicate lock
        if (currLockTypeHeld == lockType){
            throw new DuplicateLockRequestException("Transaction already holds requested lock");
        }
        // prohibit owning S/IS if owned SIX
        else if(currLockTypeHeld == LockType.SIX){
            if (lockType == LockType.S || lockType == LockType.IS){
                throw new InvalidLockException("Transaction already holds an SIX Lock");
            }
        }
        else if(!LockType.compatible(currLockTypeHeld,lockType)){
            throw new InvalidLockException("Requested Lock is not compatible with transaction's existing held locks");
        }

        // check lock on parent
        if (this.parent != null) {
            LockType transLockOnParent = this.lockman.getLockType(transaction, this.parent.getResourceName());
            if (transLockOnParent == LockType.IS && lockType == LockType.X) {
                throw new InvalidLockException("Cannot Request 'X' on resource with 'IS' on parent");
            }
        }

        // grant lock
        this.lockman.acquire(transaction,this.name,lockType);
        // update locks that transaction holds
        if (parent != null) {
            this.parent.numChildLocks.put(transaction.getTransNum(),
                    this.parent.numChildLocks.getOrDefault(transaction.getTransNum(), 0) + 1);
        }
        else{
            this.numChildLocks.put(transaction.getTransNum(), 0);
        }
    }

    /**
     * Release `transaction`'s lock on `name`.
     *
     * Note: you *must* make any necessary updates to numChildLocks, or
     * else calls to LockContext#getNumChildren will not work properly.
     *
     * @throws NoLockHeldException if no lock on `name` is held by `transaction`
     * @throws InvalidLockException if the lock cannot be released because
     * doing so would violate multigranularity locking constraints
     * @throws UnsupportedOperationException if context is readonly
     */
    public void release(TransactionContext transaction)
            throws NoLockHeldException, InvalidLockException {
        // TODO(proj4_part2): implement
        // check if LockContext is readonly
        if (this.readonly){
            throw new UnsupportedOperationException("Lock Context is Read Only");
        }
        // check if any lock is on name
        LockType currLockType = this.lockman.getLockType(transaction,this.name);
        if (currLockType == LockType.NL){
            throw new NoLockHeldException("Transaction does not hold a lock on the resource");
        }
        // check if lock cannot be released due to violating multigranularity locking constraints
        // if transaction has X on a child, cannot release IX/SIX on this resource
        // if transaction has S on a child, cannot release IS on this resource
        for (LockContext childLockContext: this.children.values()){
            LockType transLockOnChild = this.lockman.getLockType(transaction,childLockContext.getResourceName());
            if (currLockType == LockType.IX || currLockType == LockType.SIX || currLockType == LockType.IS) {
                if (transLockOnChild != LockType.NL) {
                    throw new InvalidLockException("Releasing Lock will violate multigranularity locking constraints");
                }
            }
        }
        this.lockman.release(transaction,this.name);
        // update locks that transaction holds
        if (parent != null) {
            this.parent.numChildLocks.put(transaction.getTransNum(), this.parent.numChildLocks.get(transaction.getTransNum()) - 1);
            return;
        }
    }

    /**
     * Promote `transaction`'s lock to `newLockType`. For promotion to SIX from
     * IS/IX, all S and IS locks on descendants must be simultaneously
     * released. The helper function sisDescendants may be helpful here.
     *
     * Note: you *must* make any necessary updates to numChildLocks, or else
     * calls to LockContext#getNumChildren will not work properly.
     *
     * @throws DuplicateLockRequestException if `transaction` already has a
     * `newLockType` lock
     * @throws NoLockHeldException if `transaction` has no lock
     * @throws InvalidLockException if the requested lock type is not a
     * promotion or promoting would cause the lock manager to enter an invalid
     * state (e.g. IS(parent), X(child)). A promotion from lock type A to lock
     * type B is valid if B is substitutable for A and B is not equal to A, or
     * if B is SIX and A is IS/IX/S, and invalid otherwise. hasSIXAncestor may
     * be helpful here.
     * @throws UnsupportedOperationException if context is readonly
     */
    public void promote(TransactionContext transaction, LockType newLockType)
            throws DuplicateLockRequestException, NoLockHeldException, InvalidLockException {
        // TODO(proj4_part2): implement
        // check if LockContext is readonly
        if (this.readonly){
            throw new UnsupportedOperationException("Lock Context is read only");
        }
        // check if newLockType is NL
        if (newLockType == LockType.NL){
            throw new InvalidLockException("New Lock Type is not a promotion");
        }
        // check if transaction has lock or if lock is duplicate
        LockType currLockHeld = this.lockman.getLockType(transaction,this.name);
        if (currLockHeld == LockType.NL){
            throw new NoLockHeldException("Transaction does not hold a lock on resource");
        }
        else if (currLockHeld == newLockType){
            throw new DuplicateLockRequestException("Transaction already has requested lock on resource");
        }

        // if promotion will lead to invalid state
        if (parent != null){
            LockType transLockOnParent = this.lockman.getLockType(transaction,parent.getResourceName());
            if (transLockOnParent == LockType.IS && newLockType == LockType.X){
                throw new InvalidLockException("Promotion will lead to Invalid State");
            }
        }

        // check validity of promotion
        if (LockType.substitutable(newLockType,currLockHeld)){
            // promotion to SIX from IS/IX
            if (newLockType == LockType.SIX){
                if (hasSIXAncestor(transaction)){
                    throw new InvalidLockException("Ancestor already has SIX lock");
                }
                if (currLockHeld == LockType.S || currLockHeld == LockType.IX || currLockHeld == LockType.IS){
                    List<ResourceName> releaseNames = sisDescendants(transaction);
                    this.lockman.acquireAndRelease(transaction,name,newLockType,releaseNames);
                    // reduce numChildLocks by num of S/IS locks released
                    this.numChildLocks.put(transaction.getTransNum(),this.numChildLocks.get(transaction.getTransNum()) - releaseNames.size());
                }
            }
            this.release(transaction);
            this.acquire(transaction,newLockType);
        }

        return;
    }

    /**
     * Escalate `transaction`'s lock from descendants of this context to this
     * level, using either an S or X lock. There should be no descendant locks
     * after this call, and every operation valid on descendants of this context
     * before this call must still be valid. You should only make *one* mutating
     * call to the lock manager, and should only request information about
     * TRANSACTION from the lock manager.
     *
     * For example, if a transaction has the following locks:
     *
     *                    IX(database)
     *                    /         \
     *               IX(table1)    S(table2)
     *                /      \
     *    S(table1 page3)  X(table1 page5)
     *
     * then after table1Context.escalate(transaction) is called, we should have:
     *
     *                    IX(database)
     *                    /         \
     *               X(table1)     S(table2)
     *
     * You should not make any mutating calls if the locks held by the
     * transaction do not change (such as when you call escalate multiple times
     * in a row).
     *
     * Note: you *must* make any necessary updates to numChildLocks of all
     * relevant contexts, or else calls to LockContext#getNumChildren will not
     * work properly.
     *
     * @throws NoLockHeldException if `transaction` has no lock at this level
     * @throws UnsupportedOperationException if context is readonly
     */
    public void escalate(TransactionContext transaction) throws NoLockHeldException {
        // TODO(proj4_part2): implement
        if (this.readonly){
            throw new UnsupportedOperationException("Lock Context is read only");
        }
        if (this.lockman.getLockType(transaction,name) == LockType.NL){
            throw new NoLockHeldException("Transaction Holds No Lock on Resource");
        }
        // hierarchy db -> table -> page

        ArrayList<ResourceName> releaseNames = new ArrayList<>();
        boolean escalateToX = false;
        // escalate to least permissible , if no IX, then escalate to S
        LockType currLockHeld = this.lockman.getLockType(transaction,name);

        // gather all children names and if any have IX/X, must escalate to X
        for (LockContext childLockContext: this.children.values()){
            if (!childLockContext.children.isEmpty()){
                for (LockContext descendantContext: childLockContext.children.values()){
                    LockType descendantLockType = this.lockman.getLockType(transaction,descendantContext.getResourceName());
                    if (descendantLockType == LockType.IX || descendantLockType == LockType.X){
                        escalateToX = true;
                    }
                    if (descendantLockType != LockType.NL){
                        releaseNames.add(descendantContext.getResourceName());
                    }
                }
            }
            childLockContext.numChildLocks.put(transaction.getTransNum(),0);
            LockType childLockType = this.lockman.getLockType(transaction,childLockContext.getResourceName());
            if (childLockType == LockType.IX || childLockType == LockType.X){
                escalateToX = true;
            }
            if (childLockType != LockType.NL){
                releaseNames.add(childLockContext.getResourceName());
            }
        }
        if (currLockHeld == LockType.IX || currLockHeld == LockType.X){
            escalateToX = true;
        }
        LockType newLockType = escalateToX ? LockType.X : LockType.S;
        // If the current lock is different from the desired lock, perform the escalation
        if (currLockHeld != newLockType) {
            releaseNames.add(name);
            this.lockman.acquireAndRelease(transaction,name,newLockType,releaseNames);
        }
        // Update the numChildLocks for this context to reflect any changes
        this.numChildLocks.put(transaction.getTransNum(),this.numChildLocks.get(transaction.getTransNum()) - releaseNames.size() + 1);
        return;
    }

    /**
     * Get the type of lock that `transaction` holds at this level, or NL if no
     * lock is held at this level.
     */
    public LockType getExplicitLockType(TransactionContext transaction) {
        if (transaction == null) return LockType.NL;
        // TODO(proj4_part2): implement
        else{
            return this.lockman.getLockType(transaction,name);
        }
    }

    /**
     * Gets the type of lock that the transaction has at this level, either
     * implicitly (e.g. explicit S lock at higher level implies S lock at this
     * level) or explicitly. Returns NL if there is no explicit nor implicit
     * lock.
     */
    public LockType getEffectiveLockType(TransactionContext transaction) {
        if (transaction == null) return LockType.NL;
        // TODO(proj4_part2): implement
        LockContext curParent = this.parent;
        LockType out = getExplicitLockType(transaction);
        while (curParent != null){
            if (this.lockman.getLockType(transaction,curParent.getResourceName()) == LockType.X){
                out =  LockType.X;
            } else{
                if (out != LockType.X){
                    if(this.lockman.getLockType(transaction,curParent.getResourceName()) == LockType.S || this.lockman.getLockType(transaction,curParent.getResourceName()) == LockType.SIX){
                        out = LockType.S;
                    }
                }
            }
            curParent = curParent.parent;
        }
        return out;
    }

    /**
     * Helper method to see if the transaction holds a SIX lock at an ancestor
     * of this context
     * @param transaction the transaction
     * @return true if holds a SIX at an ancestor, false if not
     */
    private boolean hasSIXAncestor(TransactionContext transaction) {
        // TODO(proj4_part2): implement
        LockContext curParent = parent;
        while (curParent != null){
            LockType curParentLock = this.lockman.getLockType(transaction,curParent.getResourceName());
            if (curParentLock == LockType.SIX){
                return true;
            }
            curParent = curParent.parent;
        }
        return false;
    }

    /**
     * Helper method to get a list of resourceNames of all locks that are S or
     * IS and are descendants of current context for the given transaction.
     * @param transaction the given transaction
     * @return a list of ResourceNames of descendants which the transaction
     * holds an S or IS lock.
     */
    private List<ResourceName> sisDescendants(TransactionContext transaction) {
        // TODO(proj4_part2): implement
        ArrayList<ResourceName> out = new ArrayList<>();
        for (LockContext childLockContext: this.children.values()){
            LockType transLockOnCurChild = this.lockman.getLockType(transaction,childLockContext.getResourceName());
            if (transLockOnCurChild == LockType.S || transLockOnCurChild == LockType.IS){
                out.add(childLockContext.getResourceName());
            }
        }
        return out;
    }

    /**
     * Disables locking descendants. This causes all new child contexts of this
     * context to be readonly. This is used for indices and temporary tables
     * (where we disallow finer-grain locks), the former due to complexity
     * locking B+ trees, and the latter due to the fact that temporary tables
     * are only accessible to one transaction, so finer-grain locks make no
     * sense.
     */
    public void disableChildLocks() {
        this.childLocksDisabled = true;
    }

    /**
     * Gets the parent context.
     */
    public LockContext parentContext() {
        return parent;
    }

    /**
     * Gets the context for the child with name `name` and readable name
     * `readable`
     */
    public synchronized LockContext childContext(String name) {
        LockContext temp = new LockContext(lockman, this, name,
                this.childLocksDisabled || this.readonly);
        LockContext child = this.children.putIfAbsent(name, temp);
        if (child == null) child = temp;
        return child;
    }

    /**
     * Gets the context for the child with name `name`.
     */
    public synchronized LockContext childContext(long name) {
        return childContext(Long.toString(name));
    }

    /**
     * Gets the number of locks held on children a single transaction.
     */
    public int getNumChildren(TransactionContext transaction) {
        return numChildLocks.getOrDefault(transaction.getTransNum(), 0);
    }

    @Override
    public String toString() {
        return "LockContext(" + name.toString() + ")";
    }
}

