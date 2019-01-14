import simpy

class Car(object):
	def __init__(self, env):
		self.env = env
		self.action1 = self.env.process(self.run())
		self.action2 = self.env.process(self.yieldedproc())
		self.wait = env.event()

	def run(self):
		print('Start at %d' % self.env.now)
		yield self.wait
		print('Finish at %d' % self.env.now)
			

	def yieldedproc(self):
		yield self.env.timeout(20)
		self.wait.succeed()


env = simpy.Environment()
car = Car(env)
env.run(until=40)
