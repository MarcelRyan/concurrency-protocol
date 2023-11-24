from typing import List
from enum import Enum

class ActionType(Enum):
    READ = 1
    WRITE = 2
    COMMIT = 3

    @classmethod
    def from_str(cls, name: str):
        match = list(filter(lambda key: key[0] == name, cls._member_names_))
        if len(match) == 0: raise ValueError(f'Invalid action name "{name}" ')
        return cls._member_map_[match[0]]

    def __str__(self) -> str:
        return self.name[0]
    
    def __eq__(self, __value: object) -> bool:
        return isinstance(__value, ActionType) and \
            self.value == __value.value

    def __repr__(self) -> str:
        return str(self)

class Action:
    def __init__(self, transaction_id: int, action_type: ActionType, data_item: str = '') -> None:
        if action_type != ActionType.COMMIT and data_item == '':
            raise ValueError('Read and write operations must have an associated data item')

        self._transaction_id = transaction_id
        self._action_type = action_type
        self._data_item = data_item
    
    @property
    def transaction_id(self) -> int:
        return self._transaction_id

    @property
    def action_type(self) -> ActionType:
        return self._action_type

    @property
    def data_item(self) -> str:
        return self._data_item
    
    def __eq__(self, __value: object) -> bool:
        return isinstance(__value, Action) and \
            self._transaction_id == __value.transaction_id and \
            self._action_type == __value._action_type and \
            self._data_item == __value._data_item

    def __repr__(self) -> str:
        return f'{self.action_type}{self.transaction_id}({self.data_item})'

class Transaction:
    def __init__(self, id: int, timestamp: int, actions: List[Action]) -> None:
        self._id = id
        self.timestamp = timestamp
        self.actions = actions
    
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
    def actions(self) -> List[Action]:
        return self._actions
    
    @actions.setter
    def actions(self, value: List[Action]) -> None:
        self._actions = value
