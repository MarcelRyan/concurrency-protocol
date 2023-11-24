from typing import List
from structs.transaction import Action, Transaction
from core.lock import LockManager

class Schedule:
    def __init__(self, actions: List[Action]) -> None:
        self._actions = actions
    
    @property
    def actions(self) -> List[Action]:
        return self._actions

    @property
    def transactions(self) -> List[Transaction]:
        return self._transactions

    @actions.setter
    def actions(self, value: List[Action]) -> None:
        self._actions = value

    @actions.setter
    def transactions(self, value: List[Transaction]) -> None:
        self._transactions = value

    def isTransactionExist(self, id: int) -> bool:
        for transaction in self._transactions:
            if transaction.id == id:
                return True
        return False
    
    
    

        