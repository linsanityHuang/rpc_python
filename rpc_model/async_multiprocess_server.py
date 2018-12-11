# coding: utf8
import os
import pickle
import struct
import socket
import asyncore
from io import BytesIO

'''
代码实现和前面的单进程异步模型差别不大，就是多了个 prefork 调用。

prefork 在服务器套接字启用监听队列之后进行，这样每个子进程都可以使用服务器套接字来获取新连接进行处理。
'''


class RPCHandler(asyncore.dispatcher_with_send):

	def __init__(self, sock, addr):
		asyncore.dispatcher_with_send.__init__(self, sock=sock)
		self.addr = addr
		self.handlers = {
			"ping": self.ping
		}
		self.rbuf = BytesIO()  # 读缓冲

	def handle_connect(self):
		print(self.addr, 'comes')

	def handle_close(self):
		print(self.addr, 'bye')
		self.close()

	def handle_read(self):
		while True:
			content = self.recv(1024)
			if content:
				self.rbuf.write(content)  # 追加到读缓冲
			if len(content) < 1024:  # 说明内核缓冲区空了，等待下个事件循环再继续读吧
				break
		self.handle_rpc()  # 处理新读到的消息

	def handle_rpc(self):
		while True:
			self.rbuf.seek(0)
			length_prefix = self.rbuf.read(4)
			if len(length_prefix) < 4:  # 半包
				break
			length, = struct.unpack("I", length_prefix)
			body = self.rbuf.read(length)
			if len(body) < length:  # 还是半包
				break
			request = pickle.loads(body)
			in_ = request['in']
			params = request['params']
			print(os.getpid(), in_, params)
			handler = self.handlers[in_]
			handler(params) # 处理 RPC
			left = self.rbuf.getvalue()[length + 4:]  # 截断读缓冲
			self.rbuf = BytesIO()
			self.rbuf.write(left)
		self.rbuf.seek(0, 2)  # 移动游标到缓冲区末尾，便于后续内容直接追加

	def ping(self, params):
		self.send_result("pong", params)

	def send_result(self, out, result):
		response = {"out": out, "result": result}
		body = pickle.dumps(response)
		length_prefix = struct.pack("I", len(body))
		self.send(length_prefix)
		self.send(body)


class RPCServer(asyncore.dispatcher):
	
	def __init__(self, host, port):
		asyncore.dispatcher.__init__(self)
		self.create_socket(socket.AF_INET, socket.SOCK_STREAM)
		self.set_reuse_addr()
		self.bind((host, port))
		self.listen(1)
		self.prefork(10)  # 开辟 10 个子进程

	def prefork(self, n):
		pids = list()
		for i in range(n):
			pid = os.fork()
			if pid < 0:  # fork error
				return
			if pid > 0:  # parent process
				pids.append(pid)
				continue
			if pid == 0:
				print('I am child process (%s) and my parent is %s.' % (os.getpid(), os.getppid()))
				break  # child process
		print(pids)

	def handle_accept(self):
		pair = self.accept()  # 获取一个连接
		if pair is not None:
			sock, addr = pair
			RPCHandler(sock, addr)  # 处理连接


if __name__ == '__main__':
	RPCServer("localhost", 8080)
	asyncore.loop()
