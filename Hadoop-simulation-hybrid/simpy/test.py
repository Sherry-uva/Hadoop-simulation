import simpy

def test_condition(env):
	t1, t2 = env.timeout(3, value=['h1', 'job2', 't3']), env.timeout(3, value='eggs')
	events = [t1, t2]
	ret = yield simpy.AnyOf(env, events)
	print ret
	for value in ret.values():
		print value

	t1, t2 = env.timeout(1, value='spam'), env.timeout(2, value='eggs')
	ret = yield t1 & t2
	assert ret == {t1: 'spam', t2: 'eggs'}

    # You can also concatenate & and |
	e1, e2, e3 = [env.timeout(i) for i in range(3)]
	yield (e1 | e2) & e3
	assert all(e.processed for e in [e1, e2, e3])

env = simpy.Environment()
proc = env.process(test_condition(env))
env.run()
