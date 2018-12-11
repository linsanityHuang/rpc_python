# coding: utf8

import os
import sys
import math
import pickle
import errno
import struct
import signal
import socket
import asyncore
from io import BytesIO
from kazoo.client import KazooClient

'''
分布式RPC服务端

启动脚本前需要开启zookeeper

COMPOSE_PROJECT_NAME=zk_test docker-compose up

上述命令启动了三个zookeeper节点，分别运行在本地的2181、2182、2183端口

然后运行脚本

python3 rpc_server.py localhost 8090

python3 rpc_server.py localhost 8091

python3 rpc_server.py localhost 8092

启动三个服务节点，分别运行在本地的8090、8091、8092端口，三个节点会自动注册到zookeeper

RPC客户端会从zookeeper节点寻找可用的服务节点

如果其中一个服务节点不可用，也不会影响到客户端
'''


class RPCHandler(asyncore.dispatcher_with_send):
	
	def __init__(self, sock, addr):
		asyncore.dispatcher_with_send.__init__(self, sock=sock)
		self.addr = addr
		self.handlers = {
			"ping": self.ping,
			"pi": self.pi
		}
		self.rbuf = BytesIO()
	
	def handle_connect(self):
		print(self.addr, 'comes')
	
	def handle_close(self):
		print(self.addr, 'bye')
		self.close()
	
	def handle_read(self):
		while True:
			content = self.recv(1024)
			if content:
				self.rbuf.write(content)
			if len(content) < 1024:
				break
		self.handle_rpc()
	
	def handle_rpc(self):
		while True:
			self.rbuf.seek(0)
			length_prefix = self.rbuf.read(4)
			if len(length_prefix) < 4:
				break
			length, = struct.unpack("I", length_prefix)
			body = self.rbuf.read(length)
			if len(body) < length:
				break
			request = pickle.loads(body)
			in_ = request['in']
			params = request['params']
			print(os.getpid(), in_, params)
			handler = self.handlers[in_]
			handler(params)
			left = self.rbuf.getvalue()[length + 4:]
			self.rbuf = BytesIO()
			self.rbuf.write(left)
		self.rbuf.seek(0, 2)
	
	def ping(self, params):
		self.send_result("pong", params)
	
	def pi(self, n):
		s = 0.0
		for i in range(n + 1):
			s += 1.0 / (2 * i + 1) / (2 * i + 1)
		result = math.sqrt(8 * s)
		self.send_result("pi_r", result)
	
	def send_result(self, out, result):
		response = {"out": out, "result": result}
		body = pickle.dumps(response)
		length_prefix = struct.pack("I", len(body))
		self.send(length_prefix)
		self.send(body)


class RPCServer(asyncore.dispatcher):
	zk_root = "/demo"
	zk_rpc = zk_root + "/rpc"
	
	def __init__(self, host, port):
		asyncore.dispatcher.__init__(self)
		self.host = host
		self.port = port
		self.create_socket(socket.AF_INET, socket.SOCK_STREAM)
		self.set_reuse_addr()
		self.bind((host, port))
		self.listen(1)
		self.child_pids = []
		if self.prefork(10):  # 产生子进程
			self.register_zk()  # 注册服务
			self.register_parent_signal()  # 父进程善后处理
		else:
			self.register_child_signal()  # 子进程善后处理
	
	def prefork(self, n):
		for i in range(n):
			# 正常情况下fork的返回值 >= 0
			pid = os.fork()
			if pid < 0:  # fork error
				raise Exception()
			# pid大于0表明这个是子进程的ID
			if pid > 0:  # parent process
				self.child_pids.append(pid)
				continue
			if pid == 0:
				return False  # child process
		return True
	
	def register_zk(self):
		self.zk = KazooClient(hosts='127.0.0.1:2182')
		self.zk.start()
		# 确保根节点存在，如果没有会自动创建
		self.zk.ensure_path(self.zk_root)
		value = pickle.dumps({"host": self.host, "port": self.port})
		# 创建服务子节点
		# Create a node with data, value就是data
		self.zk.create(self.zk_rpc, value, ephemeral=True, sequence=True)
	
	# 父进程需要在进程退出之前杀死所有子进程并收割之。
	def exit_parent(self, sig, frame):
		self.zk.stop()  # 关闭 zk 客户端
		self.close()  # 关闭 serversocket
		asyncore.close_all()  # 关闭所有 clientsocket
		pids = []
		for pid in self.child_pids:
			print('before kill')
			try:
				os.kill(pid, signal.SIGINT)  # 关闭子进程
				pids.append(pid)
			except OSError as ex:
				if ex.args[0] == errno.ECHILD:  # 目标子进程已经提前挂了
					continue
				raise ex
			print('after kill', pid)
		for pid in pids:
			while True:
				try:
					os.waitpid(pid, 0)  # 收割目标子进程
					break
				except OSError as ex:
					if ex.args[0] == errno.ECHILD:  # 子进程已经割过了
						break
					if ex.args[0] != errno.EINTR:
						raise ex  # 被其它信号打断了，要重试
			print('wait over', pid)
	
	def reap_child(self, sig, frame):
		print('before reap')
		while True:
			try:
				info = os.waitpid(-1, os.WNOHANG)  # 收割任意子进程
				break
			except OSError as ex:
				if ex.args[0] == errno.ECHILD:
					return  # 没有子进程可以收割
				if ex.args[0] != errno.EINTR:
					raise ex  # 被其它信号打断要重试
		pid = info[0]
		try:
			self.child_pids.remove(pid)
		except ValueError:
			pass
		print('after reap', pid)
	
	# 信号处理函数
	def register_parent_signal(self):
		signal.signal(signal.SIGINT, self.exit_parent)
		signal.signal(signal.SIGTERM, self.exit_parent)
		signal.signal(signal.SIGCHLD, self.reap_child)  # 监听子进程退出
	
	def exit_child(self, sig, frame):
		self.close()  # 关闭 serversocket
		asyncore.close_all()  # 关闭所有 clientsocket
		print('all closed')
	
	def register_child_signal(self):
		signal.signal(signal.SIGINT, self.exit_child)
		signal.signal(signal.SIGTERM, self.exit_child)
	
	def handle_accept(self):
		pair = self.accept()  # 接收新连接
		if pair is not None:
			sock, addr = pair
			RPCHandler(sock, addr)


if __name__ == '__main__':
	# python rpc_server.py localhost 8090
	# 从命令行获取IP和端口号
	if len(sys.argv) < 3:
		print('启动命令缺少必要参数')
		sys.exit(0)
	host = sys.argv[1]
	port = int(sys.argv[2])
	# 启动RPC服务器
	RPCServer(host, port)
	asyncore.loop()  # 启动事件循环
