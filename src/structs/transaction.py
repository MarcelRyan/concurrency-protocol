try:
    from typing import List
except ImportError:
    from typing_extensions import List
from enum import Enum

class OperationType(Enum):
    READ = 1
    WRITE = 2
    COMMIT = 3

    @classmethod
    def from_str(cls, name: str):
        match = list(filter(lambda key: key[0] == name, cls._member_names_))
        if len(match) == 0: raise ValueError(f'Invalid operation name "{name}" ')
        return cls._member_map_[match[0]]

    def __str__(self) -> str:
        return self.name[0]
    
    def __eq__(self, __value: object) -> bool:
        return isinstance(__value, OperationType) and \
            self.value == __value.value

    def __repr__(self) -> str:
        return str(self)

class Operation:
    def __init__(self, transaction_id: int, op_type: OperationType, data_item: str = None) -> None:
        if op_type != OperationType.COMMIT and data_item == None:
            raise ValueError('Read and write operations must have an associated data item')
        if op_type == OperationType.COMMIT and data_item != None:
            raise ValueError('Commit operations cannot have an associated data item')

        self._transaction_id = transaction_id
        self._op_type = op_type
        self._data_item = data_item
    
    @property
    def transaction_id(self) -> int:
        return self._transaction_id

    @property
    def op_type(self) -> OperationType:
        return self._op_type

    @property
    def data_item(self) -> str:
        return self._data_item
    
    def __eq__(self, __value: object) -> bool:
        return isinstance(__value, Operation) and \
            self._transaction_id == __value.transaction_id and \
            self._op_type == __value._op_type and \
            self._data_item == __value._data_item
    
    def __str__(self) -> str:
        rep = f'{self.op_type}{self.transaction_id}'
        if self.data_item != None:
            rep += f'({self.data_item})'
        return rep

    def __repr__(self) -> str:
        return str(self)

class Transaction:
    def __init__(self, id: int, timestamp: int, operations: List[Operation]) -> None:
        self._id = id
        self.timestamp = timestamp
        self.operations = operations
    
    @property
    def id(self) -> int:
        return self._id

    @property
    def timestamp(self) -> int:
        return self._timestamp
    
    @timestamp.setter
    def timestamp(self, value: int) -> None:
        self._timestamp = value
    
    @property
    def operations(self) -> List[Operation]:
        return self._operations
    
    @operations.setter
    def operations(self, value: List[Operation]) -> None:
        self._operations = value
