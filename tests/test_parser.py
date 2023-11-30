import utils.parser as parse
from structs.transaction import Operation, OperationType

class TestParser:
    def test_given_empty_string_should_return_empty_list(self):
        assert len(parse.parseInput('')) == 0
    
    def test_given_string_should_return_correct_list(self):
        test_list = [
            Operation(1, OperationType.READ, 'A'),
            Operation(2, OperationType.READ, 'A'),
            Operation(1, OperationType.WRITE, 'A'),
            Operation(1, OperationType.COMMIT),
            Operation(2, OperationType.COMMIT)
        ]
        assert test_list[0] == Operation(1, OperationType.READ, 'A')

        test_string = ';'.join([str(a) for a in test_list])
        assert parse.parseInput(test_string) == test_list
