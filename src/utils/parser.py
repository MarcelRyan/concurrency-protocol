try:
    from typing import List
except ImportError:
    from typing_extensions import List
import re
from structs.transaction import Operation, OperationType

def parseInput(input: str) -> List[Operation]:
    # Split input by ;
    parsed_input = input.split(";")

    # Remove whitespaces from parsed_input
    parsed_input = [i.strip() for i in parsed_input]

    # Validating parsed_input doesn't contain empty value
    parsed_input = [i for i in parsed_input if i != ""]

    # print(parsed_input)

    operation_list = []

    for op in parsed_input:

        pattern = re.compile(r'([RWC])(\d+)(?:\((\w*)\))?')
        match = pattern.match(op)

        if match:
            op_type = OperationType.from_str(match.group(1))
            transaction_id = int(match.group(2)) 
            data_item = match.group(3) if match.group(3) else None

            op = Operation(transaction_id, op_type, data_item)
            operation_list.append(op)

    return operation_list
