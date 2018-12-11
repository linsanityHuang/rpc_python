# coding: utf8

import pickle
import struct
import socket
from multiprocessing import Process, Pool


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
	p = Pool(4)
	while True:
		conn, addr = sock.accept()
		# 使用进程池
		p.apply_async(handle_conn, args=(conn, addr, handlers))

		# 使用Process进程对象
		# p = Process(target=handle_conn, args=(conn, addr, handlers))
		# print('Child process will start.')
		# p.start()
		
		# 使用fork方法，不兼容Windows平台
		# pid = os.fork()  # 好戏在这里，创建子进程处理新连接
		# if pid < 0:  # fork error
		# 	return
		# if pid > 0:  # parent process
		# 	conn.close()  # 关闭父进程的客户端套接字引用
		# 	continue
		# if pid == 0:
		# 	sock.close()  # 关闭子进程的服务器套接字引用
		# 	handle_conn(conn, addr, handlers)
		# 	break  # 处理完后一定要退出循环，不然子进程也会继续去 accept 连接


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
