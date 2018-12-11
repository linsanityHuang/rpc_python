# coding: utf-8
# client.py

import pickle
import time
import struct
import socket

'''
RPC客户端

向服务器连续发送 10 个 RPC 请求，并输出服务器的响应结果。

使用约定的长度前缀法对请求消息进行编码，对响应消息进行解码。

如果要演示多个并发客户端进行 RPC 请求，那就启动多个客户端进程。
'''


'''
值得注意的是在函数rpc中中有两个地方调用了socket.recv方法来读取期望的消息，通过传入一个长度值来获取想要的字节数组。

实际上这样使用是不严谨的，甚至根本就是错误的。

socket.recv(int)默认是阻塞调用，不过这个阻塞也是有条件的。

如果内核的套接字接收缓存是空的，它才会阻塞。

只要里面有哪怕只有一个字节，这个方法就不会阻塞，

它会尽可能将接受缓存中的内容带走指定的字节数，然后就立即返回，而不是非要等待期望的字节数全满足了才返回。

这意味着我们需要尝试循环读取才能正确地读取到期望的字节数。

但是为了简单起见，我们后面的章节代码都直接使用socket.recv，

在开发环境中网络延迟的情况较少发生，一般来说很少会遇到recv方法一次读不全的情况发生。
'''


def receive(sock, n):
	rs = []  # 读取的结果
	while n > 0:
		r = sock.recv(n)
		if not r:  # EOF
			return rs
		rs.append(r)
		n -= len(r)
	return ''.join(rs)


def rpc(sock, in_, params):
	request = pickle.dumps({"in": in_, "params": params})  # 请求消息体
	length_prefix = struct.pack("I", len(request))  # 请求长度前缀
	sock.sendall(length_prefix)
	sock.sendall(request)
	length_prefix = sock.recv(4)  # 响应长度前缀
	length, = struct.unpack("I", length_prefix)
	body = sock.recv(length)  # 响应消息体
	response = pickle.loads(body)
	return response["out"], response["result"]  # 返回响应类型和结果


if __name__ == '__main__':
	# 创建一个套接字
	# ipv4, tcp
	s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	# 连接远程服务器
	s.connect(("localhost", 8080))
	for i in range(100):  # 连续发送 10 个 rpc 请求
		out, result = rpc(s, "ping", "ireader %d" % i)
		print(out, result)
		time.sleep(1)  # 休眠 1s，便于观察
	
	# for i in range(10):
	# 	out, result = rpc(s, "random_", "%d" % i)
	# 	print(out, result)
	# 	time.sleep(1)
	# 关闭连接
	s.close()
