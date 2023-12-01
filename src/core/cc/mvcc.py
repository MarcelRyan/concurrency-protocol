try:
    from typing import Dict, List, Set
except:
    from typing_extensions import Dict, List, Set
from core.cc.strategy import CCStrategy
from structs.schedule import Schedule
from structs.transaction import Operation, OperationType

class _Version:
    def __init__(self, data_item: str, version: int, read_t: int, write_t: int) -> None:
        self.data_item = data_item
        self.version = version
        self.read_t = read_t
        self.write_t = write_t
    
    def __repr__(self) -> str:
        return f'<{self.data_item}{self.version}, RTS: {self.read_t}, WTS: {self.write_t}>'

class MultiversionTimestampCC(CCStrategy):
    def accept(self, schedule: Schedule) -> None:
        versions: Dict[str, List[_Version]] = dict()
        
        # Get initial timestamp as one unit before the smallest transaction timestamp
        init_t = min([t for t in list(map(lambda t: t.timestamp, schedule.transactions.values()))]) - 1
        
        # Add all initial versions of each data item
        for op in schedule.operations:
            if op.data_item is not None and op.data_item not in versions:
                versions[op.data_item] = [_Version(op.data_item, 0, init_t, init_t)]
        
        print('Initial versions:')
        print('\n'.join([f'{dat}: ' + repr(ver) for dat, ver in versions.items()]))

        print('Beginning MVCC protocol...')
        i = 0
        sched_len = len(schedule.operations)
        for i in range(sched_len):
            op = schedule.operations[i]

            # Check the type of this operation
            if op.op_type == OperationType.COMMIT:
                # Commit this transaction
                print(f'    {op}: Commit transaction T{op.transaction_id}')
            
            else:
                # Find the latest version with less or equal WTS to this transaction's TS
                ts = schedule.transactions[op.transaction_id].timestamp
                ver = max(
                    filter(
                        lambda v: v.write_t <= ts,
                        versions[op.data_item]
                    ),
                    key=lambda v: v.write_t
                )

                if op.op_type == OperationType.READ:
                    # Update the RTS of this version to this transaction's TS
                    if ver.read_t < ts:
                        print(f'    {op}: Read from and update RTS of version {ver} to {ts}')
                        ver.read_t = ts
                    else:
                        print(f'    {op}: Read from version {ver}')
                elif op.op_type == OperationType.WRITE:
                    # Compare the timestamps of this version to the transaction's TS
                    if ver.read_t > ts:
                        # This version already has a newer transaction reading its value:
                        # Rollback the current transaction
                        print(f'    {op}: Rollback transaction T{op.transaction_id}')

                        # Remove this transaction's operations from the schedule
                        to_abort: List[Operation] = list()
                        for j in range(len(schedule.operations) - 1, -1, -1):
                            if schedule.operations[j].transaction_id == op.transaction_id:
                                to_abort.insert(0, schedule.operations.pop(j))
                                if j <= i: i -= 1
                        
                        # Append the operations at the end of the schedule and update the
                        # corresponding transaction's timestamp
                        schedule.operations += to_abort
                        new_ts = max(list(map(lambda t: t.timestamp, schedule.transactions.values()))) + 1
                        schedule.transactions[op.transaction_id].timestamp = new_ts

                    elif ver.write_t == ts:
                        # Overwrite the content of this version
                        print(f'    {op}: Overwrite data at {ver}')
                    else:
                        # Create a new version of this data item with its timestamps set
                        # to the current transactions' TS
                        new_ver = _Version(op.data_item, len(versions[op.data_item]), ts, ts)
                        versions[op.data_item].append(new_ver)
                        print(f'    {op}: Add new version {new_ver}')
            # Move to the next operation
            i += 1
        print('MVCC protocol finished')
            
        print('Final versions:')
        print('\n'.join([f'{dat}: ' + repr(ver) for dat, ver in versions.items()]))
