from structs.transaction import Operation, OperationType
from structs.schedule import Schedule

class TestSchedule:
    def test_given_empty_list_should_return_empty_schedule(self):
        s = Schedule([])
        assert len(s.operations) == 0
        assert len(s.transactions) == 0
    
    def test_given_operation_list_should_return_correct_schedule(self):
        test_list = [
            Operation(1, OperationType.READ, 'A'),
            Operation(2, OperationType.READ, 'A'),
            Operation(1, OperationType.WRITE, 'A'),
            Operation(1, OperationType.COMMIT),
            Operation(2, OperationType.COMMIT)
        ]
        s = Schedule(test_list)
        expected_ids = set(map(lambda op: op.transaction_id, test_list))

        assert len(s.transactions) == len(expected_ids)
        for id in expected_ids:
            assert len(list(filter(lambda t: t.id == id, s.transactions.values())))
        for i in range(len(test_list)):
            assert s.operations[i] == test_list[i]
