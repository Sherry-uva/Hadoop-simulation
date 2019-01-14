import simpy

shared = 0
lock = 0

class Car1:
	def __init__(self, env):
		self.env = env
		self.action = self.env.process(self.run())

	def run(self):
		global shared
		print 'Starting...'
		yield env.timeout(10)
		print 'Car1: Increase shared by 1 at time ' + str(env.now) + ', shared is equal to ' + str(shared)
		shared += 1
		print 'Car1: shared is equal to ' + str(shared)
			
		
class Car2:
	def __init__(self, env):
		self.env = env
		self.action = self.env.process(self.run())
	
	def run(self):
		global shared
		print 'Starting...'
		yield env.timeout(10)
		print 'Car2: Decrease shared by 1 at time ' + str(env.now) + ', shared is equal to ' + str(shared)
		shared -= 1
		print 'Car2: shared is equal to ' + str(shared)


env = simpy.Environment()
Car2(env)
Car1(env)
env.run(until=20)
print 'shared is equal to ' + str(shared)
