try:
    from typing import Dict, List, Set
except:
    from typing_extensions import Dict, List, Set
from core.cc.strategy import CCStrategy
from structs.transaction import Operation, Transaction, OperationType
from structs.schedule import Schedule
from time import sleep

class _State:
    def __init__(self, startTS: int, validationTS: int, finishTS: int, isFirstTr: bool = False) -> None:
        self.isFirstTr = isFirstTr
        self.startTS = startTS
        self.validationTS = validationTS
        self.finishTS = finishTS
        self.read_set = []
        self.write_set: List[Operation] = []
    
class OptimisticCC(CCStrategy):
    def accept(self, schedule: Schedule) -> None:
        transaction_ts: Dict[int, _State] = dict()
        preExecutedOp: List[Operation] = schedule.operations
        executedTr: List[Transaction] = []

        # Setting initial/default timestamps
        for tr in schedule.transactions.keys():
            transaction_ts[tr] = _State(schedule.operations.index(schedule.transactions[tr].operations[0]), 0, 0)
        
        # Marking first transaction
        transaction_ts[schedule.operations[0].transaction_id].isFirstTr = True

        # Read Phase
        for op in schedule.operations:
            if op.op_type == OperationType.READ:
                transaction_ts[op.transaction_id].read_set.append(op)
            elif op.op_type == OperationType.WRITE:
                transaction_ts[op.transaction_id].write_set.append(op)

        # Validation phase
        for tr in schedule.transactions.keys():
            print(f"Validating transaction {tr}...")

            # Set validationTS in the time commit
            transaction_ts[tr].validationTS = schedule.operations.index(schedule.transactions[tr].operations[-1])

            # If first transaction, automatically validated
            if transaction_ts[tr].isFirstTr:
                transaction_ts[tr].finishTS = transaction_ts[tr].validationTS
                executedTr.append(schedule.transactions[tr])
                        
            else:
                success = False

                # Keep rollingback everytime aborted
                while not success:

                    # Check if data used was updated in previous transaction
                    for executed in executedTr:
                        if transaction_ts[executed.id].finishTS < transaction_ts[tr].startTS:
                            success = True

                        elif transaction_ts[tr].startTS < transaction_ts[executed.id].finishTS < transaction_ts[tr].validationTS:
                            for execWrite in transaction_ts[executed.id].write_set:
                                if execWrite.data_item in [op.data_item for op in schedule.transactions[tr].operations]:
                                    print(f"Operation intersect detected with {execWrite} from transaction {executed.id}")
                                    success = False
                                    break
                                success = True
                        else:
                            success = False

                        if not success:
                            break
                    
                    # Rollback
                    if not success:
                        print(f"Abort, restarting transaction {tr} at timestamp {transaction_ts[executed.id].finishTS + 1}")
                        transaction_ts[tr].startTS = transaction_ts[executed.id].finishTS + 1
                        transaction_ts[tr].validationTS += (transaction_ts[executed.id].finishTS + 1 - transaction_ts[tr].startTS)

                        # Move operations to the front
                        for op in schedule.transactions[tr].operations:
                            restart: Operation = preExecutedOp.pop(preExecutedOp.index(op))
                            preExecutedOp.append(restart)
                    # Write Phase
                    else:
                        transaction_ts[tr].finishTS = transaction_ts[tr].validationTS
                        executedTr.append(schedule.transactions[tr])
            print(f"Transaction {tr} has been validated, adding to schedule\n")
        schedule.operations = preExecutedOp