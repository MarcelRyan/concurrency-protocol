import utils.parser as parse
from structs.transaction import Action, ActionType

class TestParser:
    def test_given_empty_string_should_return_empty_list(self):
        assert len(parse.parseInput('')) == 0
    
    def test_given_string_should_return_correct_list(self):
        test_list = [
            Action(1, ActionType.READ, 'A'),
            Action(2, ActionType.READ, 'A'),
            Action(1, ActionType.WRITE, 'A'),
            Action(1, ActionType.COMMIT),
            Action(2, ActionType.COMMIT)
        ]
        assert test_list[0] == Action(1, ActionType.READ, 'A')

        test_string = ';'.join([str(a) for a in test_list])
        assert parse.parseInput(test_string) == test_list
