try:
    from typing import Dict, List, Set
except:
    from typing_extensions import Dict, List, Set
from core.cc.strategy import CCStrategy
from structs.schedule import Schedule
from structs.transaction import OperationType

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
        abort_set: Set[int] = set()
        commit_set: Set[int] = set()
        
        # Get initial timestamp as one unit before the smallest transaction timestamp
        init_t = min([t for t in list(map(lambda t: t.timestamp, schedule.transactions.values()))]) - 1
        
        # Add all initial versions of each data item
        for op in schedule.operations:
            if op.data_item is not None and op.data_item not in versions:
                versions[op.data_item] = [_Version(op.data_item, 0, init_t, init_t)]
        
        print('Initial versions:')
        print('\n'.join([f'{dat}: ' + repr(ver) for dat, ver in versions.items()]))

        print('Beginning MVCC protocol...')
        while True:
            print('  Beginning next pass...')
            for op in schedule.operations:
                # Ignore operations of aborted and committed transactions
                if op.transaction_id in abort_set or op.transaction_id in commit_set: continue

                # Commit this transaction
                if op.op_type == OperationType.COMMIT:
                    commit_set.add(op.transaction_id)
                    print(f'    {op}: Commit transaction T{op.transaction_id}')
                    continue
                
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
                        abort_set.add(op.transaction_id)
                    elif ver.write_t == ts:
                        # Overwrite the content of this version
                        print(f'    {op}: Overwrite data at {ver}')
                    else:
                        # Create a new version of this data item with its timestamps set
                        # to the current transactions' TS
                        new_ver = _Version(op.data_item, len(versions[op.data_item]), ts, ts)
                        versions[op.data_item].append(new_ver)
                        print(f'    {op}: Add new version {new_ver}')
            
            print(f'  End of pass: {len(abort_set)} transactions aborted')

            # Break if no transactions were aborted
            if len(abort_set) == 0: break

            # Collect operations of aborted transactions and reschedule
            for id in abort_set:
                # Filter out aborted operations
                ops = list(filter(lambda op: op.transaction_id == id, schedule.operations))
                for op in ops: schedule.operations.remove(op)

                # Append the operations at the end of the schedule
                schedule.operations += ops

                # Bump the transaction timestamp
                new_ts = max(list(map(lambda t: t.timestamp, schedule.transactions.values()))) + 1
                schedule.transactions[id].timestamp = new_ts

                print(f'    T{id}: Bumped timestamp to {new_ts}, rescheduled {len(ops)} operations')
            abort_set.clear()
        print('MVCC protocol finished')
            
        print('Final versions:')
        print('\n'.join([f'{dat}: ' + repr(ver) for dat, ver in versions.items()]))
