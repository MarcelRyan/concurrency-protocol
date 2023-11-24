from structs.transaction import Action, ActionType
from typing import List
import re

def parseInput(input: str) -> List[Action]:
    # Split input by ;
    parsed_input = input.split(";")

    # Remove whitespaces from parsed_input
    parsed_input = [i.strip() for i in parsed_input]

    # Validating parsed_input doesn't contain empty value
    parsed_input = [i for i in parsed_input if i != ""]

    # print(parsed_input)

    action_list = []

    for action in parsed_input:

        pattern = re.compile(r'([RWC])(\d+)(?:\((\w*)\))?')
        match = pattern.match(action)

        if match:
            action_type = ActionType.from_str(match.group(1))
            transaction_id = int(match.group(2)) 
            data_item = match.group(3) if match.group(3) else ""

            action = Action(transaction_id, action_type, data_item)
            action_list.append(action)

    return action_list
