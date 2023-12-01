from argparse import ArgumentParser
from structs.schedule import Schedule
from core.cc.tpl import TwoPhaseLockingCC
from core.cc.occ import OptimisticCC
from core.cc.mvcc import MultiversionTimestampCC
import utils.parser as parse
from structs.schedule import Schedule

_STRATEGY_SELECT = {
    '2pl': TwoPhaseLockingCC,
    'occ': OptimisticCC,
    'mvcc': MultiversionTimestampCC
}
_STRATEGY_DEFAULT = '2pl'

parser = ArgumentParser(
    description=(
        'Concurrency control protocol implementation that takes in a concurrent'
        ' schedule and transforms it into an equivalent schedule using a selected'
        ' concurrency control strategy.'
    ),
    epilog='Made with ♡ by K01-G11'
)
parser.add_argument('schedule',
    help='A string containing the schedule to be checked'
)
parser.add_argument('--strategy', '-s',
    help='What concurrency control protocol to use (defaults to 2pl)',
    choices=list(_STRATEGY_SELECT.keys()),
    default=_STRATEGY_DEFAULT
)

if __name__ == '__main__':
    args = parser.parse_args()
    schedule_str: str = args.schedule
    operations = parse.parseInput(schedule_str)
    sched = Schedule(operations)
    print(f'Input schedule:\n{sched}')

    strategy = _STRATEGY_SELECT[args.strategy]()
    sched.apply_cc(strategy)
    print(f'\nNew schedule (using CC strategy "{strategy.__class__.__name__}"):\n{sched}')
