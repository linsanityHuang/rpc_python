# coding: utf8
# blocking_single.py

import pickle
import struct
import socket
from random import randint

'''
单线程同步模型的服务器是最简单的服务器模型，每次只能处理一个客户端连接，其它连接必须等到前面的连接关闭了才能得到服务器的处理。

否则发送过来的请求会悬挂住，没有任何响应，直到前面的连接处理完了才能继续。

服务器根据 RPC 请求的 in 字段来查找相应的 RPC Handler 进行处理。

例子中只展示了 ping 消息的处理器。

如果你想支持多种消息，可以在代码中增加更多的处理器函数，并将处理器函数注册到全局的 handlers 字典中。
'''


def send_result(conn: socket.socket, out: str, result: str) -> None:
	response = pickle.dumps({"out": out, "result": result})  # 响应消息体
	length_prefix = struct.pack("I", len(response))  # 响应长度前缀
	conn.sendall(length_prefix)
	conn.sendall(response)


def handle_conn(conn: socket.socket, addr: tuple, handlers: dict) -> None:
	print(addr, "comes")
	# 这里的循环只服务一个新连接
	while True:  # 循环读写
		length_prefix = conn.recv(4)  # 请求长度前缀
		if not length_prefix:  # 连接关闭了
			print(addr, "bye")
			conn.close()
			break  # 退出内层循环，处理下一个连接
		length, = struct.unpack("I", length_prefix)
		# print('请求长度', length)
		body = conn.recv(length)  # 请求消息体
		# {'in': 'ping', 'params': 'ireader 0'}
		request = pickle.loads(body)
		# print('请求体', request)
		in_ = request['in']
		params = request['params']
		print(in_, params)
		handler = handlers[in_]  # 查找请求处理器
		handler(conn, params)  # 处理请求


def loop(sock, handlers):
	# 这里的循环是为了不断接受新连接
	while True:
		# 返回的conn是一个新的socket实例
		# addr是一个元组，形如('127.0.0.1', 61731)
		conn, addr = sock.accept()  # 接收连接
		handle_conn(conn, addr, handlers)  # 处理连接


def ping(conn, params):
	send_result(conn, "pong", params)
	

def random_(conn, params):
	send_result(conn, "random_", randint(1, 100))


if __name__ == '__main__':
	sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)  # 创建一个 TCP 套接字
	sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)  # 打开 reuse addr 选项
	sock.bind(("localhost", 8080))  # 绑定端口
	sock.listen(1)  # 监听客户端连接
	handlers = {  # 注册请求处理器
		"ping": ping,
		'random_': random_
	}
	loop(sock, handlers)  # 进入服务循环
