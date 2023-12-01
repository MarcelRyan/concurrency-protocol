from argparse import ArgumentParser
from structs.schedule import Schedule
from core.cc.tpl import TwoPhaseLockingCC
from core.cc.occ import OptimisticCC
from core.cc.mvcc import MultiversionTimestampCC
import utils.parser as parse
from core.cc.strategy import CCStrategy
from structs.schedule import Schedule
from core.lock import LockType, LockManager
from structs.transaction import OperationType, Operation, Transaction

_STRATEGY_SELECT = {
    'tpl': TwoPhaseLockingCC,
    'occ': OptimisticCC,
    'mvcc': MultiversionTimestampCC
}
_STRATEGY_DEFAULT = 'tpl'

parser = ArgumentParser(description='Memprotokolkan sebuah konkurensi')
parser.add_argument('schedule')
parser.add_argument('--strategy', '-s', choices=list(_STRATEGY_SELECT.keys()), default=_STRATEGY_DEFAULT)

if __name__ == '__main__':
    args = parser.parse_args()
    schedule_str: str = args.schedule
    operations = parse.parseInput(schedule_str)
    sched = Schedule(operations)
    print(f'Input schedule:\n{sched}')

    strategy = _STRATEGY_SELECT[args.strategy]()
    sched.apply_cc(strategy)
    print(f'\nNew schedule (using CC strategy "{strategy.__class__.__name__}"):\n{sched}')

# def create_complex_deadlock_schedule():
#     operations = [
#         Operation(1, OperationType.READ, "A"),
#         Operation(2, OperationType.WRITE, "B"),
#         Operation(1, OperationType.WRITE, "A"),
#         Operation(3, OperationType.READ, "B"),
#         Operation(2, OperationType.COMMIT),
#         Operation(4, OperationType.READ, "C"),
#         Operation(3, OperationType.WRITE, "B"),
#         Operation(4, OperationType.WRITE, "C"),
#         Operation(1, OperationType.COMMIT),
#         Operation(3, OperationType.COMMIT),
#         Operation(4, OperationType.COMMIT),
#     ]
#     transactions = [
#         Transaction(1, 1, operations[:4]),
#         Transaction(2, 2, operations[4:5]),
#         Transaction(3, 3, operations[5:8]),
#         Transaction(4, 4, operations[8:11]),
#     ]
#     return Schedule(operations), transactions


# # Function to create a test schedule with a deadlock
# def create_test_schedule_with_deadlock():
#     operations = [
#         Operation(1, OperationType.READ, "A"),
#         Operation(2, OperationType.WRITE, "B"),
#         Operation(1, OperationType.WRITE, "A"),
#         Operation(3, OperationType.READ, "B"),
#         Operation(2, OperationType.COMMIT),
#         Operation(3, OperationType.WRITE, "B"),
#         Operation(1, OperationType.COMMIT),
#         Operation(3, OperationType.COMMIT),
#     ]
#     transactions = [
#         Transaction(1, 1, operations[:3]),
#         Transaction(2, 2, operations[3:5]),
#         Transaction(3, 3, operations[5:8]),
#     ]
#     return Schedule(operations), transactions

# def main():
#     # Create a test schedule with a deadlock
#     test_schedule, transactions = create_test_schedule_with_deadlock()

#     for transaction in transactions:
#         print(f"Transaction {transaction.id} operations: {transaction.operations}")
    
    
#     # Initialize a lock manager
#     lock_manager = LockManager()

#     # Create an instance of TwoPhaseLockingCC
#     two_phase_locking_strategy = TwoPhaseLockingCC()

#     # Apply the strategy to the schedule
#     test_schedule.lock_manager = lock_manager
#     two_phase_locking_strategy.accept(test_schedule)

#     # Print the final state of the schedule
#     print("Final Schedule:")
#     print(test_schedule)

# if __name__ == "__main__":
#     main()
