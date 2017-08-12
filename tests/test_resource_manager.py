def test_acquire_resources(resource_manager):
    r1 = resource_manager.acquire()
    assert r1

    r2 = resource_manager.acquire()
    assert not r2


def test_reclaim_resources(resource_manager):
    r1 = resource_manager.acquire()
    assert r1

    resource_manager.reclaim(r1)

    r2 = resource_manager.acquire()
    assert r2
