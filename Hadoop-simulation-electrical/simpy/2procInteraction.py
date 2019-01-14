import simpy, random

# class Car1:
# 	def __init__(self, env, car2):
# 		self.env = env
# 		self.action = self.env.process(self.run(car2))
# 		self.wait = env.event()
# 
# 	def run(self, car2):
# 		print('Interrupting car2 at %d' % self.env.now)
# 		car2.action.interrupt(self.wait)
# 		yield self.wait
# 		print('Car1 is interrupted by Car2 at %d' % self.env.now)
# 			
# 		
# class Car2:
# 	def __init__(self, env):
# 		self.env = env
# 		self.action = self.env.process(self.run())
# 	
# 	def run(self):
# 		while True:
# 			try:
# 				yield env.timeout(40)
# 			except simpy.Interrupt as i:
# 				print 'Car2 is Interrupted at ' + str(env.now)
# 				yield env.timeout(10)
# 				i.cause.succeed()
# 
# 
# env = simpy.Environment()
# car2 = Car2(env)
# car1 = Car1(env, car2)

def producer(name, env, consumer):
	for i in range(3):
		yield env.timeout(random.randint(1,3))
		consumer.interrupt(name)
		print(name, 'Produced spam at', env.now)

def consumer(name, env):
	while True:
		try:
			yield env.timeout(40)
		except simpy.Interrupt as i:
			print 'Get a new message from producer ' + str(i.cause) + ' at ' + str(env.now)


env = simpy.Environment()
consumer = env.process(consumer(1, env))
producers = [env.process(producer(i, env, consumer)) for i in range(3)]


env.run(until=40)
