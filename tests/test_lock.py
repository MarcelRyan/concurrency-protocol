from core.lock import LockManager, LockType

class TestLockManager:
    def test_given_initial_state_should_grant_lock(self):
        lm1 = LockManager()
        assert lm1.grant_lock('A', LockType.SHARED, 0)

        lm2 = LockManager()
        assert lm2.grant_lock('B', LockType.EXCLUSIVE, 1)

    def test_given_s_lock_should_grant_s_lock(self):
        lm = LockManager()
        lm.grant_lock('A', LockType.SHARED, 0)
        assert lm.grant_lock('A', LockType.SHARED, 1)
    
    def test_given_s_lock_should_reject_x_lock(self):
        lm = LockManager()
        lm.grant_lock('A', LockType.SHARED, 0)
        assert not lm.grant_lock('A', LockType.EXCLUSIVE, 1)
    
    def test_given_x_lock_should_reject_s_lock(self):
        lm = LockManager()
        lm.grant_lock('A', LockType.EXCLUSIVE, 0)
        assert not lm.grant_lock('A', LockType.SHARED, 1)
    
    def test_given_x_lock_should_reject_x_lock(self):
        lm = LockManager()
        lm.grant_lock('A', LockType.EXCLUSIVE, 0)
        assert not lm.grant_lock('A', LockType.EXCLUSIVE, 1)
    
    def test_given_s_lock_should_grant_upgrade_lock(self):
        lm = LockManager()
        lm.grant_lock('A', LockType.SHARED, 0)
        assert lm.grant_lock('A', LockType.EXCLUSIVE, 0)

    def test_given_release_x_lock_should_grant_s_lock(self):
        lm = LockManager()
        lm.grant_lock('A', LockType.EXCLUSIVE, 0)
        lm.release_lock('A', 0)
        assert lm.grant_lock('A', LockType.SHARED, 1)

    def test_given_release_x_lock_should_grant_x_lock(self):
        lm = LockManager()
        lm.grant_lock('A', LockType.EXCLUSIVE, 0)
        lm.release_lock('A', 0)
        assert lm.grant_lock('A', LockType.EXCLUSIVE, 1)
    
    def test_given_release_s_lock_should_grant_x_lock(self):
        lm = LockManager()
        lm.grant_lock('A', LockType.SHARED, 0)
        lm.release_lock('A', 0)
        assert lm.grant_lock('A', LockType.EXCLUSIVE, 1)
