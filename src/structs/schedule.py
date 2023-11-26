from typing import List
from structs.transaction import Action, Transaction

class Schedule:
    def __init__(self, actions: List[Action]) -> None:
        self.actions = actions
        transaction_dict = {}
        for action in actions:
            if action.transaction_id not in transaction_dict.keys():
                transaction_dict[action.transaction_id] = []
            transaction_dict[action.transaction_id].append(action)
        
        transactions = []
        for key in transaction_dict.keys():
            transactions.append(Transaction(key, key, transaction_dict[key]))      
        self._transactions = transactions
    
    @property
    def actions(self) -> List[Action]:
        return self._actions
    
    @actions.setter
    def actions(self, value: List[Action]) -> None:
        self._actions = value

    @property
    def transactions(self) -> List[Transaction]:
        return self._transactions
    
    @transactions.setter
    def transactions(self, value: List[Transaction]) -> None:
        self._transactions = value


    
    
    

        