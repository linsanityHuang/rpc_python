# coding: utf-8

import pickle
import time
import struct
import socket
import random
from kazoo.client import KazooClient

zk_root = "/demo"

G = {"servers": None}  # 全局变量，RemoteServer 对象列表


class RemoteServer(object):  # 封装 rpc 套接字对象
	
	def __init__(self, addr):
		self.addr = addr
		self._socket = None
	
	@property
	def socket(self):  # 懒惰连接
		if not self._socket:
			self.connect()
		return self._socket
	
	def ping(self, twitter):
		return self.rpc("ping", twitter)
	
	def pi(self, n):
		return self.rpc("pi", n)
	
	def rpc(self, in_, params):
		sock = self.socket
		request = pickle.dumps({"in": in_, "params": params})
		length_prefix = struct.pack("I", len(request))
		sock.sendall(length_prefix)
		sock.sendall(request)
		length_prefix = sock.recv(4)
		length, = struct.unpack("I", length_prefix)
		body = sock.recv(length)
		response = pickle.loads(body)
		return response["out"], response["result"]
	
	def connect(self):
		sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		host, port = self.addr.split(":")
		sock.connect((host, int(port)))
		self._socket = sock
	
	def reconnect(self):  # 重连
		self.close()
		self.connect()
	
	def close(self):
		if self._socket:
			self._socket.close()
			self._socket = None


def get_servers():
	zk = KazooClient(hosts="127.0.0.1:2182")
	# 启动客户端，尝试连接
	zk.start()
	# 当前活跃地址列表
	current_addrs = set()
	
	# 闭包函数
	def watch_servers(*args):
		new_addrs = set()
		# 获取新的服务地址列表，并持续监听服务列表变动
		for child in zk.get_children(zk_root, watch=watch_servers):
			node = zk.get(zk_root + "/" + child)
			addr = pickle.loads(node[0])
			new_addrs.add("%s:%d" % (addr["host"], addr["port"]))
		# 新增的地址
		add_addrs = new_addrs - current_addrs
		# 删除的地址
		del_addrs = current_addrs - new_addrs
		del_servers = []
		# 先找出所有的待删除 server 对象
		for addr in del_addrs:
			for s in G["servers"]:
				if s.addr == addr:
					del_servers.append(s)
					break
		# 依次删除每个 server
		for server in del_servers:
			G["servers"].remove(server)
			current_addrs.remove(server.addr)
		# 新增 server
		for addr in add_addrs:
			G["servers"].append(RemoteServer(addr))
			current_addrs.add(addr)
	
	# 首次获取节点列表并持续监听服务列表变更
	for child in zk.get_children(zk_root, watch=watch_servers):
		node = zk.get(zk_root + "/" + child)
		addr = pickle.loads(node[0])
		current_addrs.add("%s:%d" % (addr["host"], addr["port"]))
	G["servers"] = [RemoteServer(s) for s in current_addrs]
	return G["servers"]


def random_server():  # 随机获取一个服务节点
	if G["servers"] is None:
		get_servers()  # 首次初始化服务列表
	if not G["servers"]:
		return
	return random.choice(G["servers"])


if __name__ == '__main__':
	for i in range(100):
		server = random_server()
		if not server:
			break  # 如果没有节点存活，就退出
		time.sleep(0.5)
		try:
			out, result = server.ping("ireader %d" % i)
			print(server.addr, out, result)
		except Exception as ex:
			server.close()  # 遇到错误，关闭连接
			print(ex)
		server = random_server()
		if not server:
			break  # 如果没有节点存活，就退出
		time.sleep(0.5)
		try:
			out, result = server.pi(i)
			print(server.addr, out, result)
		except Exception as ex:
			server.close()  # 遇到错误，关闭连接
			print(ex)
