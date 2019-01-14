class Car:
	def __init__(self, id):
		self.id = id
		self.run()

	def run(self):
		print('ID is %d' % self.id)
		
car = Car(45)
