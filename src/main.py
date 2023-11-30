from argparse import ArgumentParser
from structs.schedule import Schedule
import utils.parser as parse

parser = ArgumentParser(description='Memprotokolkan sebuah konkurensi')
parser.add_argument('schedule')

if __name__ == '__main__':
    args = parser.parse_args()
    schedule_str: str = args.schedule
    operations = parse.parseInput(schedule_str)
    sched = Schedule(operations)
    print(sched)
