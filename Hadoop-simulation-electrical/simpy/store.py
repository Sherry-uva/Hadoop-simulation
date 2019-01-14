import simpy, random

# def producer(name, env, store):
# 	for i in range(3):
# 		yield env.timeout(random.randint(1,3))
# 		store.put(['spam %s from producer %d' % (i, name), name])
# 		print(name, 'Produced spam at', env.now)
# 
# def consumer(name, env, store1, store2):
# 	while True:
# # 		item = yield (store1.get() | store2.get())
# 		item = yield store1.get()
# 		print(name, 'got', item, 'at', env.now)
# 
# env = simpy.Environment()
# store1 = simpy.Store(env)
# store2 = simpy.Store(env)

# prod = env.process(producer(env, store))
# consumers = [env.process(consumer(i, env, store)) for i in range(2)]

# producer1 = env.process(producer(1, env, store1))
# producer2 = env.process(producer(2, env, store2))
# 
# consumer = env.process(consumer(1, env, store1, store2))

def producer(name, env, store):
	for i in range(3):
		yield env.timeout(random.randint(1,3))
		store.put(['spam %s from producer %d' % (i, name), name])
# 		store.put([])
		print(name, 'Produced spam at', env.now)

def consumer(name, env, store):
	while True:
		msg = yield store.get()
		print 'Get a new message! ' + msg[0]
# 		item = yield store.get() 
# 		print(name, 'got', item, 'at', env.now)

env = simpy.Environment()
store = simpy.Store(env)
producers = [env.process(producer(i, env, store)) for i in range(2)]
consumer = env.process(consumer(1, env, store))

# def producer(name, env, consumer):
# 	for i in range(3):
# 		yield env.timeout(random.randint(1,3))
# 		consumer.interrupt('Interrpt from producer' + str(name))
# 		print 'Producer' + str(name) + ' produced spam at ' + str(env.now)
# 
# def consumer(name, env):
# 	while True:
# 		try:
# 			yield env.timeout(20)
# 		except simpy.Interrupt as i:
# 			print 'Customer' + str(name) + ' got ' + i.cause + ' at ' + str(env.now)
# 
# env = simpy.Environment()
# consumer = env.process(consumer(1, env))
# producers = [env.process(producer(i, env, consumer)) for i in range(2)]
env.run(until=20)








