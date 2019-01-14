import simpy, random

def producer(name, env, issues):
	for i in range(3):
		yield env.timeout(3)
		priority = random.randint(0,3)
# 		priority = 'P'+str(random.randint(0,3))
		yield issues.put(simpy.PriorityItem(priority, 'spam %s from producer %d' % (i, name)))
		print(name, 'Produced spam at ', env.now, ' with priority ', priority)

def consumer(name, env, issues):
	while True:
		item = yield issues.get()
		print(name, 'got', item[1], 'at', env.now)

env = simpy.Environment()
issues = simpy.PriorityStore(env)


# prod = env.process(producer(env, store))
# consumers = [env.process(consumer(i, env, store)) for i in range(2)]

producers = [env.process(producer(i, env, issues)) for i in range(2)]
consumer = env.process(consumer(1, env, issues))

env.run(until=11)
