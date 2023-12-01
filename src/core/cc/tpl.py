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
