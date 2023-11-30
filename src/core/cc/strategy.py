from abc import ABC, abstractmethod
from structs.schedule import Schedule

class CCStrategy(ABC):
    @abstractmethod
    def accept(self, schedule: Schedule) -> None:
        pass
