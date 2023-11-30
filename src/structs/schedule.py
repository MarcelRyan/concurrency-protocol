from typing import List, Dict
from structs.transaction import Operation, Transaction

class Schedule:
    def __init__(self, operations: List[Operation]) -> None:
        self.operations = operations.copy()
        transaction_dict: Dict[int, List[Operation]] = dict()
        for op in operations:
            if op.transaction_id not in transaction_dict.keys():
                transaction_dict[op.transaction_id] = list()
            transaction_dict[op.transaction_id].append(op)
        
        transactions: List[Transaction] = list()
        for key in transaction_dict.keys():
            transactions.append(Transaction(key, key, transaction_dict[key]))      
        self._transactions = transactions
    
    @property
    def operations(self) -> List[Operation]:
        return self._operations
    
    @operations.setter
    def operations(self, value: List[Operation]) -> None:
        self._operations = value

    @property
    def transactions(self) -> List[Transaction]:
        return self._transactions
    
    @transactions.setter
    def transactions(self, value: List[Transaction]) -> None:
        self._transactions = value

    def __repr__(self) -> str:
        col_width = 12
        rep = '|'.join([f'{f"T{t.id}":^{col_width}}' for t in self.transactions])
        rep += '\n' + '|'.join(['-' * col_width for _ in self.transactions])

        row: list[str] = list()
        for a in self.operations:
            row.clear()
            for t in self.transactions:
                row.append(f'{str(a):^{col_width}}' if t.id == a.transaction_id else ' ' * col_width)
            rep += '\n' + '|'.join(row)
        
        return rep
