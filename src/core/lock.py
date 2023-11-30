try:
    from typing import Dict, Tuple
except ImportError:
    from typing_extensions import Dict, Tuple
from enum import Enum

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
        return self._first[0]

    @property
    def peek_lock(self) -> LockType:
        return self._first[1]
    
    def __len__(self) -> int:
        return len(self._locks)

class LockManager:
    def __init__(self):
        self.locks: Dict[str, _LockList] = {}
    
    def grant_lock(self, data_item: str, lock_type: LockType, transaction_id: int):
        # If there is no lock list on this data item yet, create a new lock list
        if data_item not in self.locks.keys():
            self.locks[data_item] = _LockList()
        
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
