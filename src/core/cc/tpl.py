from core.cc.strategy import CCStrategy
from structs.schedule import Schedule
from core.lock import LockType, LockManager
from structs.transaction import OperationType, Transaction

class TwoPhaseLockingCC(CCStrategy):
    def accept(self, schedule: Schedule) -> None:
        lock_manager = LockManager()

        # Phase 1: Growing Phase
        self.growing_phase(schedule, lock_manager)

        # Print the state of the lock manager after the Growing Phase
        print("State of the lock manager after Growing Phase:")
        for data_item, lock_list in lock_manager.locks.items():
            print(f"{data_item}: {[(tid, lock) for tid, lock in lock_list._locks.items()]}")

        # Phase 2: Shrinking Phase with Deadlock Prevention
        self.shrinking_phase(schedule, lock_manager)

        # Print the final state of the lock manager after the Shrinking Phase
        print("Final state of the lock manager after Shrinking Phase:")
        for data_item, lock_list in lock_manager.locks.items():
            print(f"{data_item}: {[(tid, lock) for tid, lock in lock_list._locks.items()]}")

    def growing_phase(self, schedule: Schedule, lock_manager: LockManager):
        print("Starting Growing Phase")
        for transaction in schedule.transactions:
            for operation in transaction.operations:
                if operation.op_type == OperationType.READ:
                    print(f"Transaction {transaction.id} requesting READ lock on {operation.data_item}")
                    lock_manager.grant_lock(operation.data_item, LockType.SHARED, transaction.id)
                elif operation.op_type == OperationType.WRITE:
                    print(f"Transaction {transaction.id} requesting WRITE lock on {operation.data_item}")
                    lock_manager.grant_lock(operation.data_item, LockType.EXCLUSIVE, transaction.id)

    def shrinking_phase(self, schedule : Schedule, lock_manager: LockManager):
        print("Starting Shrinking Phase with Deadlock Prevention")
        for transaction in reversed(schedule.transactions):
            for operation in reversed(transaction.operations):
                if operation.op_type == OperationType.COMMIT:
                    print(f"Transaction {transaction.id} committing and releasing locks")
                    lock_manager.release_locks(transaction.id)
                elif operation.op_type == OperationType.WRITE:
                    print(f"Transaction {transaction.id} releasing WRITE lock on {operation.data_item}")
                    lock_manager.release_lock(operation.data_item, transaction.id)
                    
                    # Check for deadlock prevention (based on priorities)
                    self.deadlock_prevention(transaction, lock_manager)

    def deadlock_prevention(self, transaction: Transaction, lock_manager: LockManager):
        # Assume each transaction has a priority attribute
        transaction_priority = transaction.priority if hasattr(transaction, 'priority') else 0

        # Check if the released lock can be granted to waiting transactions
        for data_item, lock_list in lock_manager.locks.items():
            for waiting_transaction_id in lock_list._locks.keys():
                waiting_transaction = lock_manager.get_transaction(waiting_transaction_id)
                print(waiting_transaction)

                if waiting_transaction:
                    waiting_transaction_priority = (
                        waiting_transaction.priority
                        if hasattr(waiting_transaction, 'priority')
                        else 0
                    )
                    print("masuk")

                    if self.is_deadlocked(transaction, waiting_transaction, lock_manager):
                        print(
                            f"Transaction {waiting_transaction_id} rolled back to prevent deadlock."
                        )
                        lock_manager.abort_transaction(waiting_transaction_id)
                    else :
                        print("no deadlock")

    def is_deadlocked(self, releasing_transaction, waiting_transaction, lock_manager: LockManager):
        visited = set()

        def dfs(transaction_id):
            print(f"Checking transaction {transaction_id}")
            visited.add(transaction_id)

            for next_transaction_id in lock_manager.get_waiting_transactions(transaction_id):
                print(f"Next transaction: {next_transaction_id.id}")

                if next_transaction_id not in visited:
                    next_transaction = lock_manager.get_transaction(next_transaction_id.id)

                    if next_transaction:
                        # disini anehnya
                        print(f"Next transaction operations: {next_transaction.operations}")
                        print(f"Next transaction: {next_transaction.id}")
                        print(f"data_item {next_transaction.get_data_items()}")
                        
                        for data_item in next_transaction.get_data_items():
                            print(f"ini data item: {data_item}")
                            print(f"Checking data item: {data_item}")
                            
                            print(f"data item = {lock_manager.locks[data_item].peek_id}")

                            if lock_manager.locks[data_item].peek_id == waiting_transaction.id:
                                print("Deadlock detected!")
                                return True  # Deadlock detected

                        if dfs(next_transaction_id):
                            return True  # Deadlock detected
            return False

        return dfs(waiting_transaction.id)

