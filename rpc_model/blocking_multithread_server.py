# coding: utf8

import pickle
import struct
import socket
from threading import Thread
from concurrent.futures import ThreadPoolExecutor


def handle_conn(conn, addr, handlers):
	print(addr, "comes")
	while True:  # 循环读写
		length_prefix = conn.recv(4)  # 请求长度前缀
		if not length_prefix:  # 连接关闭了
			print(addr, "bye")
			conn.close()
			break  # 退出循环，退出线程
		length, = struct.unpack("I", length_prefix)
		body = conn.recv(length)  # 请求消息体
		request = pickle.loads(body)
		in_ = request['in']
		params = request['params']
		print(in_, params)
		handler = handlers[in_]  # 查找请求处理器
		handler(conn, params)  # 处理请求


def loop(sock, handlers):
	while True:
		conn, addr = sock.accept()
		# 开启新线程进行处理，就这行代码不一样
		t = Thread(target=handle_conn, args=(conn, addr, handlers))
		t.start()


def ping(conn, params):
	send_result(conn, "pong", params)


def send_result(conn, out, result):
	response = pickle.dumps({"out": out, "result": result})
	length_prefix = struct.pack("I", len(response))
	conn.sendall(length_prefix)
	conn.sendall(response)


if __name__ == '__main__':
	sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
	sock.bind(("localhost", 8080))
	sock.listen(1)
	handlers = {
		"ping": ping
	}
	loop(sock, handlers)
