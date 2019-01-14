import simpy 
from random import randint

class EV:
	def __init__(self, env, scheduler):
		self.env = env
		self.drive_proc = env.process(self.drive(env, scheduler))

	def drive(self, env, scheduler):
		while True:
            # Drive for 20-40 min
			yield env.timeout(randint(20, 40))
			scheduler.interrupt()
			print('Container finishes at ', env.now)

def sche(env):
	print('Start scheduler at ', env.now)
	while True:
		try:
			yield env.timeout(50)
			print('Finish at ', env.now)
		except simpy.Interrupt as i:
			print('Interrupted at', env.now)

env = simpy.Environment()
scheduler = env.process(sche(env))
ev = EV(env, scheduler)
env.run(until=300)
