from argparse import ArgumentParser
from structs.schedule import Schedule
from core.cc.tpl import TwoPhaseLockingCC
import utils.parser as parse

parser = ArgumentParser(description='Memprotokolkan sebuah konkurensi')
parser.add_argument('schedule')

if __name__ == '__main__':
    args = parser.parse_args()
    schedule_str: str = args.schedule
    operations = parse.parseInput(schedule_str)
    sched = Schedule(operations)
    print(f'Input schedule:\n{sched}')

    strategy = TwoPhaseLockingCC()
    sched.apply_cc(strategy)
    print(f'\nNew schedule (using CC strategy "{strategy.__class__.__name__}"):\n{sched}')
