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
        if len(self._locks) == 0:
            self._first = None
        elif self._first[0] == transaction_id:
            self._first = list(self._locks.items())[0]

    @property
    def peek_id(self) -> int:
        return self._first[0] if self._first is not None else None

    @property
    def peek_lock(self) -> LockType:
        return self._first[1]
    
    def __getitem__(self, key: int) -> LockType:
        return self._locks[key]

    def __contains__(self, key: int) -> bool:
        return key in self._locks
    
    def __len__(self) -> int:
        return len(self._locks)

class LockManager:
    def __init__(self):
        self.locks: Dict[str, _LockList] = {}
    
    def grant_lock(self, data_item: str, lock_type: LockType, transaction_id: int):
        # If there is no lock list on this data item yet, create a new lock list
        if data_item not in self.locks:
            self.locks[data_item] = _LockList()
        
        # If the transaction already has a sufficient lock, do nothing
        if self.has_lock(data_item, lock_type, transaction_id):
            return True
        
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
    
    def has_lock(self, data_item: str, lock_type: LockType, transaction_id: int) -> bool:
        if data_item not in self.locks: return False
        if transaction_id not in self.locks[data_item]: return False
        lock = self.locks[data_item][transaction_id]
        return lock == LockType.EXCLUSIVE or lock_type == LockType.SHARED

    def release_lock(self, data_item: str, transaction_id: int):
        if data_item in self.locks and transaction_id in self.locks[data_item]:
            self.locks[data_item].remove(transaction_id)
    
    def peek_lock_holder(self, data_item: str) -> int:
        return self.locks[data_item].peek_id if data_item in self.locks else None

    def release_locks(self, transaction_id: int):
        for data_item in self.locks:
            self.release_lock(data_item, transaction_id)
