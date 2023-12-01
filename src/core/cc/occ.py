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
    
    def __repr__(self) -> str:
        return f'<{self.data_item}{self.version}, RTS: {self.read_t}, WTS: {self.write_t}>'
    
class OptimisticCC(CCStrategy):
    def accept(self, schedule: Schedule) -> None:
        transaction_ts: Dict[int, _State] = dict()

        for tr in schedule.transactions:
            transaction_ts[tr.id] = _State(schedule.operations.index(tr.operations[0]), 0, 0)
        
        transaction_ts[schedule.operations[0].transaction_id].isFirstTr = True
        preExecutedOp: List[Operation] = schedule.operations

        for op in schedule.operations:
            if op.op_type == OperationType.READ:
                transaction_ts[op.transaction_id].read_set.append(op)
            elif op.op_type == OperationType.WRITE:
                transaction_ts[op.transaction_id].write_set.append(op)
            # else:
            #     transaction_ts[op.transaction_id].validationTS = schedule.operations.index(schedule.transactions)

        executedOperations = []
        executedTr: List[Transaction] = []

        # Validation phase
        for tr in schedule.transactions:
            print(f"Validating transaction {tr.id}...")
            transaction_ts[tr.id].validationTS = schedule.operations.index(tr.operations[-2])
            if transaction_ts[tr.id].isFirstTr:
                transaction_ts[tr.id].finishTS = transaction_ts[tr.id].validationTS
                executedTr.append(tr)
                        
            else:
                success = False
                while not success:
                    # Check if data used was updated in previous transaction

                    for executed in executedTr:
                        # print("TR:", tr.id, "compareTR:", executed.id, "finishTS:", transaction_ts[executed.id].finishTS, "startTS:", transaction_ts[tr.id].startTS, "validTS:", transaction_ts[tr.id].validationTS)
                        if transaction_ts[executed.id].finishTS < transaction_ts[tr.id].startTS:
                            success = True

                        elif transaction_ts[tr.id].startTS < transaction_ts[executed.id].finishTS < transaction_ts[tr.id].validationTS:
                            for execWrite in transaction_ts[executed.id].write_set:
                                if execWrite.data_item in [op.data_item for op in tr.operations]:
                                    print(f"Operation intersect detected with {execWrite} from transaction {executed.id}")
                                    success = False
                                    break
                                success = True
                        else:
                            success = False
                        # sleep(5)

                        if not success:
                            break
                    
                    if not success:
                        print(f"Abort, restarting transaction {tr.id} at timestamp {transaction_ts[executed.id].finishTS + 1}")
                        transaction_ts[tr.id].startTS = transaction_ts[executed.id].finishTS + 1
                        transaction_ts[tr.id].validationTS += (transaction_ts[executed.id].finishTS + 1 - transaction_ts[tr.id].startTS)

                        for op in tr.operations:
                            restart: Operation = preExecutedOp.pop(preExecutedOp.index(op))
                            preExecutedOp.append(restart)
                    else:
                        for op in tr.operations:
                            executedOperations.append(op)
                        transaction_ts[tr.id].finishTS = transaction_ts[tr.id].validationTS + 1
                        executedTr.append(tr)
                    # failed = False
            print(f"Transaction {tr.id} has been validated, adding to schedule\n")
        print("Finished schedule:", executedOperations)
        schedule.operations = preExecutedOp