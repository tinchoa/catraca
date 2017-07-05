import time
import BaseHTTPServer
import threading, json

class MyServer(threading.Thread):
	def __init__(self,port=8000,flowList=[]):
		threading.Thread.__init__(self)
		self.HOST_NAME = '' 
		self.PORT_NUMBER = port
		self.flowList = flowList


	class MyHandler(BaseHTTPServer.BaseHTTPRequestHandler):
			def do_POST(s):
				"""Respond to a GET request."""
				global a
				print type(s)
				content = s.rfile.read(int(s.headers.getheader('content-length', 0)))

				ret = "ok"
				print s.path
				if s.path == "/add":
					ret = s.add(content)
				elif s.path == "/del":
					ret = s.delete(content)

				#retorno para o cliente
				s.send_response(200)
				s.send_header("Content-type", "text/txt")
				s.end_headers()
				s.wfile.write(ret)

			def add(self,content):
				if json.loads(content) not in self.flows:
					self.flows.append(json.loads(content))
				return "Adicionei o "+content


			def delete(self,content):
				try:
					flow = json.loads(content)
					self.flows.remove(json.loads(content))
				except:
					pass
				return "Deletei o "+content

	def run(self):

		server_class = BaseHTTPServer.HTTPServer
		httpd = server_class((self.HOST_NAME, self.PORT_NUMBER), MyServer.MyHandler)
		httpd.RequestHandlerClass.flows = self.flowList
		print time.asctime(), "Server Starts - %s:%s" % (self.HOST_NAME, self.PORT_NUMBER)
		try:
		     httpd.serve_forever()
		except KeyboardInterrupt:
		     pass
		     httpd.server_close()
		     print time.asctime(), "Server Stops - %s:%s" % (self.HOST_NAME, self.PORT_NUMBER)
