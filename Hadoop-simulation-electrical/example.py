import simpy
from random import randint

def speaker(env): 
	try:
		speakTime = randint(25, 35)
		print 'Start speaking at ' + str(env.now) + ' for ' + str(speakTime)
 		yield env.timeout(speakTime)
 	except simpy.Interrupt as interrupt:
 		print(interrupt.cause)
 		
def moderator(env):
 	for i in range(3):
 		speaker_proc = env.process(speaker(env))
 		results = yield speaker_proc | env.timeout(30)
 		
 		if speaker_proc not in results:
 			speaker_proc.interrupt('No time left at ' + str(env.now))

env = simpy.Environment()
env.process(moderator(env))
env.run(until=100)
