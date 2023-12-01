try:
    from typing import Dict, Tuple, Optional, List
except ImportError:
    from typing_extensions import Dict, Tuple
from enum import Enum
from structs.transaction import Transaction

class LockType(Enum):
    SHARED = 1
    EXCLUSIVE = 2

class _LockList:
    def __init__(self):
        self._first: Tuple[int, LockType] = None
        self._locks: Dict[int, LockType] = {}
    
    def add(self, transaction_id: int, lock: LockType) -> bool:
        if self._first is None: self._first = (transaction_id, lock)
        self._locks[transaction_id] = lock
        return True
    
    def remove(self, transaction_id: int):
        del self._locks[transaction_id]
        if len(self._locks) == 0: self._first = None

    @property
    def peek_id(self) -> int:
        return self._first[0] if self._first is not None else None

    @property
    def peek_lock(self) -> LockType:
        return self._first[1]
    
    def __len__(self) -> int:
        return len(self._locks)

class LockManager:
    def __init__(self):
        self.locks: Dict[str, _LockList] = {}
        self.transactions: Dict[int, Transaction] = {}
    
    def grant_lock(self, data_item: str, lock_type: LockType, transaction_id: int):
        # If there is no lock list on this data item yet, create a new lock list
        if data_item not in self.locks.keys():
            self.locks[data_item] = _LockList()
            
        if transaction_id not in self.transactions:
            self.transactions[transaction_id] = Transaction(transaction_id, 0, [])
        
        # If this data item has no locks, grant lock
        if len(self.locks[data_item]) == 0:
            return self.locks[data_item].add(transaction_id, lock_type)
        
        # If this data item already has locks...
        if lock_type == LockType.SHARED:
            # If an S-lock is requested, grant only if this data item only has S-locks
            return \
                self.locks[data_item].peek_lock == LockType.SHARED and \
                self.locks[data_item].add(transaction_id, lock_type)
        elif lock_type == LockType.EXCLUSIVE:
            # If an X-lock is requested, grant only if the requester already has an S-lock
            return \
                self.locks[data_item].peek_id == transaction_id and \
                self.locks[data_item].peek_lock == LockType.SHARED and \
                self.locks[data_item].add(transaction_id, lock_type)
        else:
            # How the hell did you get here?????
            raise RuntimeError('What?')

    def release_lock(self, data_item: str, transaction_id: int):
        if data_item in self.locks and self.locks[data_item].peek_id == transaction_id:
            self.locks[data_item].remove(transaction_id)

    def release_locks(self, transaction_id: int):
        for data_item in list(self.locks):
            self.release_lock(data_item, transaction_id)
            
    def get_transaction(self, transaction_id: int) -> Optional[Transaction]:
        return self.transactions.get(transaction_id)

    def abort_transaction(self, transaction_id: int):
        if transaction_id in self.transactions:
            del self.transactions[transaction_id]
            self.release_locks(transaction_id)
            print(f"Transaction {transaction_id} aborted.")

    def get_waiting_transactions(self, current_transaction_id: int) -> List[Transaction]:
        waiting_transactions = []
        for data_item, lock_list in self.locks.items():
            for waiting_transaction_id in lock_list._locks.keys():
                if waiting_transaction_id != current_transaction_id:
                    waiting_transaction = self.get_transaction(waiting_transaction_id)
                    if waiting_transaction:
                        waiting_transactions.append(waiting_transaction)
        return waiting_transactions