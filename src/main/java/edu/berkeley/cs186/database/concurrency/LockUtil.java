package edu.berkeley.cs186.database.concurrency;

import edu.berkeley.cs186.database.TransactionContext;

/**
 * LockUtil is a declarative layer which simplifies multigranularity lock
 * acquisition for the user (you, in the last task of Part 2). Generally
 * speaking, you should use LockUtil for lock acquisition instead of calling
 * LockContext methods directly.
 */
public class LockUtil {
    /**
     * Ensure that the current transaction can perform actions requiring
     * `requestType` on `lockContext`.
     *
     * `requestType` is guaranteed to be one of: S, X, NL.
     *
     * This method should promote/escalate/acquire as needed, but should only
     * grant the least permissive set of locks needed. We recommend that you
     * think about what to do in each of the following cases:
     * - The current lock type can effectively substitute the requested type
     * - The current lock type is IX and the requested lock is S
     * - The current lock type is an intent lock
     * - None of the above: In this case, consider what values the explicit
     *   lock type can be, and think about how ancestor looks will need to be
     *   acquired or changed.
     *
     * You may find it useful to create a helper method that ensures you have
     * the appropriate locks on all ancestors.
     */
    public static void ensureSufficientLockHeld(LockContext lockContext, LockType requestType) {
        // requestType must be S, X, or NL
        assert (requestType == LockType.S || requestType == LockType.X || requestType == LockType.NL);

        // Do nothing if the transaction or lockContext is null
        TransactionContext transaction = TransactionContext.getTransaction();
        if (transaction == null || lockContext == null) return;

        // You may find these variables useful
        LockContext parentContext = lockContext.parentContext();
        LockType effectiveLockType = lockContext.getEffectiveLockType(transaction);
        LockType explicitLockType = lockContext.getExplicitLockType(transaction);

        if (requestType == LockType.NL){
            return;
        }
        // if current lock type can already substitute requested lock type, leave it alone
        else if (LockType.substitutable(effectiveLockType,requestType)){
            return;
        }
        LockType intentType = (requestType == LockType.S) ? LockType.IS : LockType.IX;
        ensureAncestors(lockContext, transaction, intentType);

        if (explicitLockType == LockType.NL) {
            // No lock held yet — just acquire
            lockContext.acquire(transaction, requestType);
        } else if (explicitLockType == LockType.IS && requestType == LockType.S) {
            lockContext.escalate(transaction);
        } else if (explicitLockType == LockType.IX && requestType == LockType.S) {
            lockContext.promote(transaction, LockType.SIX);
        } else if (explicitLockType.isIntent()) {
            lockContext.escalate(transaction); // escalates to S or X
            if (!LockType.substitutable(lockContext.getExplicitLockType(transaction), requestType)) {
                lockContext.promote(transaction, requestType);
            }
        } else {
            // Explicit non-intent lock (S) but need X: promote
            lockContext.promote(transaction, requestType);
        }
        // TODO(proj4_part2): implement
        return;
    }

    private static void ensureAncestors(LockContext lockContext, TransactionContext transaction, LockType intentType) {
        LockContext parent = lockContext.parentContext();
        if (parent == null) return;

        ensureAncestors(parent, transaction, intentType);

        LockType parentExplicitLockType = parent.getExplicitLockType(transaction);

        if (LockType.substitutable(parentExplicitLockType, intentType)) {
            // Already sufficient (e.g. X or SIX satisfies IX; S or X satisfies IS)
            return;
        }

        if (parentExplicitLockType == LockType.NL) {
            parent.acquire(transaction, intentType);
        } else if (parentExplicitLockType == LockType.IS && intentType == LockType.IX) {
            // Need to upgrade IS -> IX on ancestor
            parent.promote(transaction, LockType.IX);
        }
        // S/X/SIX already cover everything, handled by substitutable check above
    }
    // TODO(proj4_part2) add any helper methods you want
}
