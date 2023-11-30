from abc import ABC, abstractmethod

# Import from structs.schedule
class Schedule: ...

class CCStrategy(ABC):
    @abstractmethod
    def accept(self, schedule: Schedule) -> None:
        pass
