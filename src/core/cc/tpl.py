try:
    from typing import List
except:
    from typing_extensions import List
from core.cc.strategy import CCStrategy, Schedule
from structs.schedule import Schedule
from core.lock import LockType, LockManager
from structs.transaction import Operation, OperationType

class TwoPhaseLockingCC(CCStrategy):
    def accept(self, schedule: Schedule) -> None:
        lm = LockManager()
        queue: List[Operation] = list()

        print('Beginning 2PL protocol (using wound-wait DP strategy)...')
        i = 0
        sched_len = len(schedule.operations)
        while i < sched_len:
            op = schedule.operations[i]

            # Check the type of this operations
            if op.op_type == OperationType.COMMIT:
                # Release all locks of this transaction
                print(f'  {op}: Commit transaction T{op.transaction_id} and release all locks')
                lm.release_locks(op.transaction_id)

                # Retry queued operations
                j = i + 1
                while len(queue) > 0:
                    schedule.operations.insert(j, queue.pop(0))
                    j += 1
            else:
                # Do automatic lock acquisition:
                # Select the appropriate lock type for this operation
                if op.op_type == OperationType.READ:
                    req_lock = LockType.SHARED
                elif op.op_type == OperationType.WRITE:
                    req_lock = LockType.EXCLUSIVE
                
                # Try to acquire lock for this operation
                # If lock acquisition fails, use the wound-wait strategy to prevent deadlocks
                do_req = True
                while do_req:
                    # Request a lock on the data item to be accessed
                    if lm.grant_lock(op.data_item, req_lock, op.transaction_id):
                        # The lock has been granted
                        print(f'  {op}: Acquired {req_lock.name} lock on {op.data_item}')
                        do_req = False
                    else:
                        # Compare this transaction's TS with the lock holder's
                        holder_id = lm.peek_lock_holder(op.data_item, op.transaction_id)
                        holder_ts = schedule.transactions[holder_id].timestamp
                        op_ts = schedule.transactions[op.transaction_id].timestamp
                        
                        if op_ts < holder_ts:
                            # Wound: Abort the lock holder and request lock again
                            print(f'  {op}: Wound transaction T{holder_id} (incompatible lock on {op.data_item})')
                            lm.release_locks(holder_id)

                            # Gather executed operations
                            pre_queue: List[Operation] = list()
                            for j in range(i - 1, -1, -1):
                                if schedule.operations[j].transaction_id == holder_id:
                                    pre_queue.insert(0, schedule.operations.pop(j))
                                    i -= 1
                            
                            # Gather future operations (will be empty if waiting)
                            post_queue: List[Operation] = list()
                            for j in range(len(schedule.operations) - 1, i, -1):
                                if schedule.operations[j].transaction_id == holder_id:
                                    post_queue.insert(0, schedule.operations.pop(j))
                            
                            queue = pre_queue + queue + post_queue
                        else:
                            # Wait: Enqueue this and subsequent operations
                            print(f'  {op}: Wait for transaction T{holder_id} (incompatible lock on {op.data_item})')
                            pre_queue: List[Operation] = list()
                            for j in range(len(schedule.operations) - 1, i - 1, -1):
                                if schedule.operations[j].transaction_id == op.transaction_id:
                                    pre_queue.insert(0, schedule.operations.pop(j))
                            i -= 1
                            queue.extend(pre_queue)
                            do_req = False
            # Move to the next operation
            i += 1
        print('2PL protocol finished')

# class TwoPhaseLockingCC(CCStrategy):
#     def accept(self, schedule: Schedule) -> None:
#         lm = LockManager()
#         commit_set: Set[int] = set()
#         wait_by: Dict[int, List[int]] = dict()
#         wait_ops: Dict[int, List[Operation]] = dict()
#         abort_ops: Dict[int, List[Operation]] = dict()

#         print('Beginning 2PL protocol (using wound-wait DP strategy)...')
#         # while True:
#         #     print('  Beginning next pass...')
#         print(f'  {schedule.operations}')
#         i = 0
#         while i < len(schedule.operations):
#             op = schedule.operations[i]

#             # Ignore operations of committed transactions
#             if op.transaction_id in commit_set:
#                 i += 1
#                 continue

#             # Enqueue operations of aborted transactions
#             if op.transaction_id in abort_ops or op.transaction_id in commit_set:
#                 print(f'      Enqueued {op} (abort)')
#                 abort_ops[op.transaction_id].append(op)
#                 schedule.operations.pop(i)
#                 continue

#             # If this transaction is waiting, push this transaction to wait-ops
#             if op.transaction_id in wait_ops:
#                 print(f'      Enqueued {op} (wait)')
#                 wait_ops[op.transaction_id].append(op)
#                 schedule.operations.pop(i)
#                 continue

#             # Commit this transaction
#             if op.op_type == OperationType.COMMIT:
#                 commit_set.add(op.transaction_id)
#                 lm.release_locks(op.transaction_id)
#                 print(f'    {op}: Commit transaction T{op.transaction_id} and release all locks')

#                 # Retry all aborted transactions
#                 j = i + 1
#                 for abort_id in list(abort_ops.keys()):
#                     aborted = abort_ops.pop(abort_id)
#                     for abort_op in aborted:
#                         print(f'      Inserted {abort_op} (abort)')
#                         schedule.operations.insert(j, abort_op)
#                         j += 1
#                 # Insert operations that is waiting for this transaction
#                 if op.transaction_id in wait_by:
#                     for wait_id in wait_by.pop(op.transaction_id):
#                         for wait_op in wait_ops.pop(wait_id):
#                             print(f'      Inserted {wait_op} (wait)')
#                             schedule.operations.insert(j, wait_op)
#                             j += 1
#                 i += 1
#                 continue

#             else:
#                 # Do automatic lock acquisition:
#                 # Select the appropriate lock type for this transaction
#                 if op.op_type == OperationType.READ:
#                     req_lock = LockType.SHARED
#                 elif op.op_type == OperationType.WRITE:
#                     req_lock = LockType.EXCLUSIVE
                
#                 # Try to acquire lock for this operation
#                 # If lock acquisition fails, use the wound-wait strategy to prevent deadlocks
#                 stop_lock = False
#                 waiting = False
#                 while not stop_lock:
#                     if lm.grant_lock(op.data_item, req_lock, op.transaction_id):
#                         stop_lock = True
#                         print(f'    {op}: Acquired lock {req_lock.name} on {op.data_item}')
#                     else:
#                         # Compare this transaction's TS with the lock holder's
#                         hold_id = lm.peek_lock_holder(op.data_item)
#                         curr_ts, hold_ts = \
#                             schedule.transactions[op.transaction_id].timestamp, \
#                             schedule.transactions[hold_id].timestamp
#                         if curr_ts < hold_ts:
#                             # Wound: Abort the lock holder and try to acquire lock again
#                             aborted = [
#                                 schedule.operations[j]
#                                 for j in range(i - 1, -1, -1)
#                                 if schedule.operations[j].transaction_id == hold_id
#                             ]
#                             aborted.reverse()
#                             for abort_op in aborted:
#                                 schedule.operations.remove(abort_op)
#                                 i -= 1
#                             abort_ops[hold_id] = aborted
#                             print(f'      Enqueued {aborted} (abort)')
#                             lm.release_locks(hold_id)

#                             # Retry all aborted transactions
#                             j = i + 1
#                             for abort_id in list(abort_ops.keys()):
#                                 if abort_id == hold_id: continue
#                                 aborted = abort_ops.pop(abort_id)
#                                 for abort_op in aborted:
#                                     print(f'      Inserted {abort_op} (abort)')
#                                     schedule.operations.insert(j, abort_op)
#                                     j += 1
#                             # Insert operations that is waiting for this transaction
#                             if op.transaction_id in wait_by:
#                                 for wait_id in wait_by.pop(op.transaction_id):
#                                     if wait_id == hold_id: continue
#                                     for wait_op in wait_ops.pop(wait_id):
#                                         print(f'      Inserted {wait_op} (wait)')
#                                         schedule.operations.insert(j, wait_op)
#                                         j += 1
#                             print(f'    {op}: Wound transaction {hold_id} (incompatible lock on {op.data_item})')
#                         elif curr_ts > hold_ts:
#                             # Wait: Enqueue this and all subsequent operations up until the lock holder's commit
#                             if hold_id not in wait_by:
#                                 wait_by[hold_id] = list()
#                             wait_by[hold_id].append(op.transaction_id)

#                             if op.transaction_id not in wait_ops:
#                                 wait_ops[op.transaction_id] = list()
#                             wait_ops[op.transaction_id].append(op)

#                             schedule.operations.pop(i)
#                             stop_lock = True
#                             waiting = True
#                             print(f'    {op}: Wait for transaction {hold_id} (incompatible lock on {op.data_item})')
                
#                 # This transaction is waiting
#                 if waiting: continue
            
#             # Move to the next operation
#             i += 1

#             # Break if no transactions were aborted
#             # if len(abort_set) == 0: break

#             # # Collect operations of aborted transactions and reschedule
#             # for id in abort_set:
#             #     # Dump all queued operations first
#             #     if id in wait_ops:
#             #         for k, v in list(wait_by.items()):
#             #             v.remove(id)
#             #             if len(v) == 0:
#             #                 wait_by.pop(k)
#             #         schedule.operations += wait_ops.pop(id)

#             #     # Filter out aborted operations
#             #     ops = list(filter(lambda op: op.transaction_id == id, schedule.operations))
#             #     for op in ops: schedule.operations.remove(op)

#             #     # Append the operations at the end of the schedule
#             #     schedule.operations += ops

#             #     # Bump the transaction timestamp
#             #     new_ts = max(list(map(lambda t: t.timestamp, schedule.transactions.values()))) + 1
#             #     schedule.transactions[id].timestamp = new_ts

#             #     print(f'    T{id}: Bumped timestamp to {new_ts}, rescheduled {len(ops)} operations')
#             # abort_set.clear()
#         print('2PL protocol finished')

# class TwoPhaseLockingCC(CCStrategy):
#     def accept(self, schedule: Schedule) -> None:
#         lock_manager = LockManager()

#         # Phase 1: Growing Phase
#         self.growing_phase(schedule, lock_manager)

#         # Print the state of the lock manager after the Growing Phase
#         print("State of the lock manager after Growing Phase:")
#         for data_item, lock_list in lock_manager.locks.items():
#             print(f"{data_item}: {[(tid, lock) for tid, lock in lock_list._locks.items()]}")

#         # Phase 2: Shrinking Phase with Deadlock Prevention
#         self.shrinking_phase(schedule, lock_manager)

#         # Print the final state of the lock manager after the Shrinking Phase
#         print("Final state of the lock manager after Shrinking Phase:")
#         for data_item, lock_list in lock_manager.locks.items():
#             print(f"{data_item}: {[(tid, lock) for tid, lock in lock_list._locks.items()]}")

#     def growing_phase(self, schedule: Schedule, lock_manager: LockManager):
#         print("Starting Growing Phase")
#         for transaction in schedule.transactions:
#             for operation in transaction.operations:
#                 if operation.op_type == OperationType.READ:
#                     print(f"Transaction {transaction.id} requesting READ lock on {operation.data_item}")
#                     lock_manager.grant_lock(operation.data_item, LockType.SHARED, transaction.id)
#                 elif operation.op_type == OperationType.WRITE:
#                     print(f"Transaction {transaction.id} requesting WRITE lock on {operation.data_item}")
#                     lock_manager.grant_lock(operation.data_item, LockType.EXCLUSIVE, transaction.id)

#     def shrinking_phase(self, schedule : Schedule, lock_manager: LockManager):
#         print("Starting Shrinking Phase with Deadlock Prevention")
#         for transaction in reversed(schedule.transactions):
#             for operation in reversed(transaction.operations):
#                 if operation.op_type == OperationType.COMMIT:
#                     print(f"Transaction {transaction.id} committing and releasing locks")
#                     lock_manager.release_locks(transaction.id)
#                 elif operation.op_type == OperationType.WRITE:
#                     print(f"Transaction {transaction.id} releasing WRITE lock on {operation.data_item}")
#                     lock_manager.release_lock(operation.data_item, transaction.id)
                    
#                     # Check for deadlock prevention (based on priorities)
#                     self.deadlock_prevention(transaction, lock_manager)

#     def deadlock_prevention(self, transaction: Transaction, lock_manager: LockManager):
#         # Assume each transaction has a priority attribute
#         transaction_priority = transaction.priority if hasattr(transaction, 'priority') else 0

#         # Check if the released lock can be granted to waiting transactions
#         for data_item, lock_list in lock_manager.locks.items():
#             for waiting_transaction_id in lock_list._locks.keys():
#                 waiting_transaction = lock_manager.get_transaction(waiting_transaction_id)
#                 print(waiting_transaction)

#                 if waiting_transaction:
#                     waiting_transaction_priority = (
#                         waiting_transaction.priority
#                         if hasattr(waiting_transaction, 'priority')
#                         else 0
#                     )
#                     print("masuk")

#                     if self.is_deadlocked(transaction, waiting_transaction, lock_manager):
#                         print(
#                             f"Transaction {waiting_transaction_id} rolled back to prevent deadlock."
#                         )
#                         lock_manager.abort_transaction(waiting_transaction_id)
#                     else :
#                         print("no deadlock")

#     def is_deadlocked(self, releasing_transaction, waiting_transaction, lock_manager: LockManager):
#         visited = set()

#         def dfs(transaction_id):
#             print(f"Checking transaction {transaction_id}")
#             visited.add(transaction_id)

#             for next_transaction_id in lock_manager.get_waiting_transactions(transaction_id):
#                 print(f"Next transaction: {next_transaction_id.id}")

#                 if next_transaction_id not in visited:
#                     next_transaction = lock_manager.get_transaction(next_transaction_id.id)

#                     if next_transaction:
#                         # disini anehnya
#                         print(f"Next transaction operations: {next_transaction.operations}")
#                         print(f"Next transaction: {next_transaction.id}")
#                         print(f"data_item {next_transaction.get_data_items()}")
                        
#                         for data_item in next_transaction.get_data_items():
#                             print(f"ini data item: {data_item}")
#                             print(f"Checking data item: {data_item}")
                            
#                             print(f"data item = {lock_manager.locks[data_item].peek_id}")

#                             if lock_manager.locks[data_item].peek_id == waiting_transaction.id:
#                                 print("Deadlock detected!")
#                                 return True  # Deadlock detected

#                         if dfs(next_transaction_id):
#                             return True  # Deadlock detected
#             return False

#         return dfs(waiting_transaction.id)

