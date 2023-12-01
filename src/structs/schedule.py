try:
    from typing import List, Dict
except ImportError:
    from typing_extensions import List, Dict
from structs.transaction import Operation, Transaction
from core.cc.strategy import CCStrategy

class Schedule:
    def __init__(self, operations: List[Operation]) -> None:
        self.operations = operations
        transaction_dict: Dict[int, List[Operation]] = dict()
        for op in operations:
            if op.transaction_id not in transaction_dict.keys():
                transaction_dict[op.transaction_id] = list()
            transaction_dict[op.transaction_id].append(op)
        
        transactions: Dict[int, Transaction] = dict()
        for key in transaction_dict.keys():
            transactions[key] = Transaction(key, key, transaction_dict[key])
        self._transactions = transactions
    
    @property
    def operations(self) -> List[Operation]:
        return self._operations
    
    @operations.setter
    def operations(self, value: List[Operation]) -> None:
        self._operations = value

    @property
    def transactions(self) -> Dict[int, Transaction]:
        return self._transactions
    
    @transactions.setter
    def transactions(self, value: Dict[int, Transaction]) -> None:
        self._transactions = value

    def apply_cc(self, strategy: CCStrategy) -> None:
        strategy.accept(self)

    def __repr__(self) -> str:
        col_width = 12
        rep = '|'.join([f'{f"T{t.id}":^{col_width}}' for t in self.transactions.values()])
        rep += '\n' + '|'.join(['-' * col_width for _ in self.transactions.values()])

        row: List[str] = list()
        for a in self.operations:
            row.clear()
            for t in self.transactions.values():
                row.append(f'{str(a):^{col_width}}' if t.id == a.transaction_id else ' ' * col_width)
            rep += '\n' + '|'.join(row)
        
        return rep
