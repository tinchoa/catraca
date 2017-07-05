import psycopg2
import psycopg2.extras

class DB:
	def __init__(self):
		self.cur = None
		self.con = None

	def connection(self):
		#try:
		self.con = psycopg2.connect("dbname='postgres' user='postgres' password='password'")
		#except:
		#	print "I am unable to connect to the database"
		# Open a cursor to perform database operations
		self.cur = self.con.cursor()

	def send_to_db(self, packet):
		teste = "INSERT INTO pacotes VALUES (%s,%s,'%s','%s','%s','%s',%s,'%s','%s','%s',%s,%s,'%s',%s,'%s')"%packet
		#print teste
		self.cur.execute(teste)
		self.con.commit()

	def get_from_db(self, dictValues):
		query = "SELECT payload FROM pacotes WHERE "
		for i in dictValues.items():
			query += i[0]
			query += " = "
			query += ("'"+ i[1] + "'")
			query += " AND "
		query = query[:-5] + ";"

		self.cur.execute(query)
		return self.cur.fetchall()


	def close_comunication(self): 
		# Close communication with the database
		self.cur.close()
		self.con.close()
	