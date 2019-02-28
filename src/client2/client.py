from __future__ import with_statement
import rpyc
import os
import sys,getopt
import inspect
import json
import codecs
argv = sys.argv[1:]
server_ip = None
server_port = None
client_index = None
MY_ROOT = None#mnt
CLIENT_ROOT = "."
SERVER_ROOT = None#init:file
DataNode_ip = '127.0.0.1'
NewFileMap = {}
Json_Name = "upload_file_log.json"
from rpyc.lib.compat import pickle, execute, is_py3k
from rpyc.core.service import ClassicService, Slave
from rpyc.utils import factory
from rpyc.core.service import ModuleNamespace
from contextlib import contextmanager
#合并文件辅助函数
def readjson(Json_Name):
	data ={}
	with codecs.open(Json_Name, "r", "utf-8") as f:
		for line in f:
			dic = json.loads(line)
			# print(dic)
			data[dic[0]] = dic[1]
	# print(data)
	return data
def writejson(Json_Name,data):
	with open(Json_Name, 'w') as json_file:
		for key,value in data.items():
			temp_list = []
			temp_list.append(key)
			temp_list.append(value)
			json_str = json.dumps(temp_list)
			json_file.write(json_str)
			json_file.write("\n")
	return 1
def joinfile(fromdir,filename,todir):
	if not os.path.exists(todir):
		os.mkdir(todir)
	if not os.path.exists(fromdir):
		print('Wrong directory')
	outfile = open(os.path.join(todir,filename),'wb')
	files = os.listdir(fromdir) #list all the part files in the directory
	files.sort()	            #sort part files to read in order
	# print(files)
	for file in files:
		if file == ".DS_Store":
			continue
		filepath = os.path.join(fromdir,file)
		infile = open(filepath,'rb')
		data = infile.read()
		outfile.write(data)
		infile.close()
	outfile.close()
#合并文件
def join_file(fromdir,filename,todir):
	try:
		joinfile(fromdir,filename,todir)
	except:
		print('Error joining files:')
		print(sys.exc_info()[0],sys.exc_info()[1])
		return -1
	return 1
def get_param(argv):
	try:
		options,args = getopt.getopt(sys.argv[1:],"m:i:d:", ["mount=","index=","dest="])
	except getopt.GetoptError:
		sys.exit()
	for name,value in options:
		if name in ("-i","--index"):
			client_index = value
		if name in ("-m","--mount"):
			temp = value.split(':')
			server_ip = temp[0]
			server_port = temp[1]
			SERVER_ROOT = temp[2]
		if name in ("-d","--dest"):
			MY_ROOT = value
	return MY_ROOT,SERVER_ROOT,server_port,client_index,server_ip
def client_to_server_dir(cur_root,dest_dir):
	temp_dir = cur_root + '/' + dest_dir
	temp_dir = temp_dir.split('/')
	final_dir = '.'
	for element in temp_dir[1:-1]:
		final_dir = final_dir + '/' + element
	return final_dir
def cd_client(dest_dir = '.',cur_root = SERVER_ROOT):
	global CLIENT_ROOT
	if dest_dir == "..":
		temp_dir = cur_root.split('/')
		if len(temp_dir) == 1:
			return cur_root,0
		final_dir = temp_dir[:-1]
		cur_root = final_dir[0]
		final = "."
		for index in range(len(final_dir[1:])):
			cur_root = cur_root +"/"+ final_dir[index + 1]
			final = final +"/"+final_dir[index + 1]
		c.root.exposed_changedir(final,client_index)
		CLIENT_ROOT = final
		return cur_root,1
	elif dest_dir == '.':
		return cur_root,1
	else:
		final_dir = client_to_server_dir(cur_root,dest_dir)
		temp_dir = final_dir + '/' + dest_dir
		exist = c.root.exposed_exist_dir(temp_dir)
		if exist == 0:
			return cur_root,exist#0代表没有这个目录，输出错误，1代表有这个目录，输出正确
		else:
			CLIENT_ROOT = final_dir + '/' + dest_dir
			c.root.exposed_changedir(CLIENT_ROOT,client_index)
			return cur_root + '/' + dest_dir,exist
def ls_client(dest_dir = '.',cur_root = SERVER_ROOT):
	if dest_dir == '.':
		return c.root.exposed_ls(CLIENT_ROOT),1
	if dest_dir == '..':
		temp_dir = CLIENT_ROOT.split('/')
		if len(temp_dir) == 1:
			return cur_root,0
		final_dir = temp_dir[:-1]
		final = '.'
		for index in range(len(final_dir[1:])):
			final = final +"/"+final_dir[index + 1]
		return c.root.exposed_ls(final),1
	else:
		final_dir = CLIENT_ROOT + "/" + dest_dir
		exist = c.root.exposed_exist_dir(final_dir)
		if exist == 0:
			return 0
		else:
			return c.root.exposed_ls(final_dir),1
def mkdir_client(name):
	final_dir = CLIENT_ROOT + "/" + name
	exist = c.root.exposed_exist_dir(final_dir)
	if exist == 1:
		return 0#存在该文件夹
	else:
		return c.root.exposed_mkdir(name,CLIENT_ROOT)
def getFileServerName_client(name):
	server_list,error,version_list = c.root.exposed_getFile_ServerList(name)
	return server_list,error,version_list
def download_file(conn, remotepath, localpath, chunk_size = 16000):
	with conn.builtin.open(remotepath, "rb") as rf:
		with open(localpath, "wb") as lf:
			while True:
				buf = rf.read(chunk_size)
				if not buf:
					break
				lf.write(buf)
def check_local_file(name):
	file_dir = CLIENT_ROOT + ":" + name + "_dir"
	file_dir = MY_ROOT + "/" + file_dir[2:]
	real_dir = CLIENT_ROOT + ":" +name + "_dir"
	real_dir = real_dir[2:]
	cur_dir = "mnt"
	# cur_dir =  MY_ROOT + "/" + cur_dir[2:]
	symbol = 1#本地无此文件
	list = os.listdir(cur_dir)
	if real_dir not in list:
		return 1,[],{}
	list = os.listdir(file_dir)
	if name in list:
		symbol =  2#本地有此文件
	exist_chunk = []
	local_chunk_version = {}
	for element in list:
		element = element.split("_")
		if len(element) > 1:
			exist_chunk.append(element[-2])
			local_chunk_version[element[-2]] = element[-1]
	return symbol,exist_chunk,local_chunk_version
def get_file(server_list,name,chunk_list,version_list,local_chunk_version,chunk_size = 1024):
	fromdir = CLIENT_ROOT + ":" + name + "_dir"
	to_dir =  MY_ROOT + "/" + fromdir[2:]
	if server_list == {}:
		return 0
	for key,value in server_list.items():
		if key in chunk_list and version_list[key] <= int(local_chunk_version[key]):
			c.root.exposed_trans_finish(value,name,key)
			continue
		conn = rpyc.classic.connect(DataNode_ip,int(value))
		fromfilename = CLIENT_ROOT + ":" + name + "_dir" + "/" + name + "_" + key 
		fromfilename = "Data/" + fromfilename[2:]
		if not os.path.exists(to_dir):#check whether to_dir exists or not
			os.mkdir(to_dir)
		if local_chunk_version != {}:
			oldfilename = to_dir + "/" + name + "_" + key+ "_" + local_chunk_version[key]
			os.remove(oldfilename)
		tofilename = to_dir + "/" + name + "_" + key+ "_" + str(version_list[key])
		count=0
		with conn.builtins.open(fromfilename,"rb") as rf:
			with open(tofilename,"wb") as lf:
				while True:
					# print(chunk_size,count)
					count = count + 1
					buf = rf.read(chunk_size)
					if not buf:
						print("outing",count)
						break
					lf.write(buf)
		conn.close()
		c.root.exposed_trans_finish(value,name,key)
	filename = name
	error = join_file(to_dir,filename,to_dir)
	return error
def write_file_lock(name,chunk_index):
	c.root.exposed_updatedFileinfo(name,"lock_symbol",1,chunk_index)
def read_file_lock(name,chunk_index):
	c.root.exposed_updatedFileinfo(name,"lock_symbol",2,chunk_index)
def file_unlock(name,chunk_index):
	c.root.exposed_updatedFileinfo(name,"lock_symbol",0,chunk_index)
def writefile_client(name,chunk_index,chunk_size = 1024):
	while c.root.exposed_file_wlock(name,chunk_index) == 0:
		pass
	prev_file_name = CLIENT_ROOT + ":" + name + "_dir" + "/" + name
	remote_file_name = "Data/"+prev_file_name[2:]
	remote_file_chunk_name= "Data/"+prev_file_name[2:] + "_chunk0001"
	prev_file_dir = CLIENT_ROOT + ":" + name + "_dir"
	remote_file_dir = "Data/" + prev_file_dir[2:]
	local_file_dir = "mnt/" + prev_file_dir[2:]
	local_file_name = "mnt/" + prev_file_name[2:]
	remote_port = upload_log[remote_file_name][chunk_index][1]
	conn = rpyc.classic.connect(DataNode_ip,int(remote_port))
	if not conn.modules.os.path.isdir(remote_file_dir):
		conn.modules.os.makedirs(remote_file_dir)
	with open(local_file_name, "rb") as lf:
		with conn.builtin.open(remote_file_chunk_name, "wb") as rf:
			while True:
				buf = lf.read(chunk_size)
				if not buf:
					break
				rf.write(buf)
	conn.close()
	temp_list = []
	temp_list.append(upload_log[remote_file_name]["chunk0001"][0])
	c.root.exposed_updatedFileinfo(name,"file_server_list",temp_list,"chunk0001")
	# c.root.
	#会出现本地版本比远端版本旧，然后覆盖了远端版本的问题（paxos算法）
	#
	upload_log[remote_file_name]["chunk0001"][2] = upload_log[remote_file_name]["chunk0001"][2] + 1
	writejson(Json_Name,upload_log)
	c.root.exposed_updatedFileinfo(name,"Version",upload_log[remote_file_name]["chunk0001"][2],"chunk0001")
	# end = c.root.
	return c.root.exposed_file_unlock(name,"chunk0001")
def rm_client(name):
	error = c.root.exposed_rmfile(name)
	return error
def mkfile_client(name,upload_log):
	exist = c.root.exposed_exist_file(name)
	if exist == 1:
		return 0#存在该文件了
	else:
		remote_file_dir = CLIENT_ROOT + ":" + name + "_dir"
		remote_file_name = CLIENT_ROOT + ":" + name + "_dir" + "/" + name
		local_file_dir = "mnt/" + remote_file_dir[2:]
		local_file_name = "mnt/" + remote_file_name[2:]
		local_file_name_chunk = local_file_name + "_chunk0001_0"
		exist = os.path.exists(local_file_dir)
		if not exist:
			os.makedirs(local_file_dir)
		else:
			print("本地已有该文件夹，创建文件夹失败")
		exist = os.path.exists(local_file_name)
		if not exist:
			file = open(local_file_name,'w')
			file.close()
		else:
			print("本地已有该文件，创建文件失败")
		remote_file_name = "Data/"+remote_file_name[2:]
		remote_file_dir = "Data/" + remote_file_dir[2:]
		error,PortMap = c.root.exposed_mkfile(name)
		if error == 0:
			temp = None
			for key,value in PortMap.items():
				temp = key
				if value[1] == 0:
					NewFileMap[remote_file_name] = {}
					NewFileMap[remote_file_name]["chunk0001"] = []
					NewFileMap[remote_file_name]["chunk0001"].append(key)
					NewFileMap[remote_file_name]["chunk0001"].append(value[0])
					NewFileMap[remote_file_name]["chunk0001"].append(0)
					break
			if remote_file_name not in NewFileMap.keys():
				NewFileMap[remote_file_name] = {}
				NewFileMap[remote_file_name]["chunk0001"] = []
				NewFileMap[remote_file_name]["chunk0001"].append(temp)
				NewFileMap[remote_file_name]["chunk0001"].append(PortMap[temp][0])
				NewFileMap[remote_file_name]["chunk0001"].append(0)
		upload_log[remote_file_name] = NewFileMap[remote_file_name]
		
		c.root.exposed_updatedFileinfo(name,"lock_symbol",0,"chunk0001")
		c.root.exposed_updatedFileinfo(name,"Version",0,"chunk0001")
		
		writejson(Json_Name,upload_log)
MY_ROOT,SERVER_ROOT,server_port,client_index,server_ip = get_param(argv)
print(server_ip,server_port,client_index,MY_ROOT,SERVER_ROOT)
c = rpyc.connect(server_ip, int(server_port),config={'allow_all_attrs': True})
print(c.root.build(client_index,CLIENT_ROOT))
check_local_file("test_dir:trainData.txt")
upload_log= {}
upload_log = readjson(Json_Name)
# print( cd_client("test_dir",SERVER_ROOT))
while 1:
	cmd_prev = SERVER_ROOT
	cmd = input(cmd_prev+","+CLIENT_ROOT + "," +  MY_ROOT + "# ")
	decode_cmd = cmd.split(" ")
	print(decode_cmd)
	if decode_cmd[0] == "cd":
		SERVER_ROOT,error = cd_client(decode_cmd[1],SERVER_ROOT)
		print(SERVER_ROOT)
		if error == 0:
			print("没有此文件夹 请重新输入")
	elif(decode_cmd[0] == "ls"):
		if len(decode_cmd) == 1:
			result,error = ls_client('.',SERVER_ROOT)
			print(result)
		else:
			result,error = ls_client(decode_cmd[1],SERVER_ROOT)
			if error == 0:
				print("没有此文件夹 请重新输入")
			else:
				print(result)
	elif (decode_cmd[0] == "mkdir"):
		if len(decode_cmd) == 1:
			print("请输入文件夹名字")
			continue
		error = mkdir_client(decode_cmd[1])
		if error == 0:
			print("已有同名文件夹，创建失败")
		elif error == 0:
			print("创建成功")
	elif (decode_cmd[0] == "get"):
		if len(decode_cmd) == 1:
			print("请输入文件名")
			continue
		else:
			exsit_file,chunk_list,local_chunk_version = check_local_file(decode_cmd[1])
			print(exsit_file,local_chunk_version)
			if exsit_file == 2:
				reply = input("本地已有此文件，是否获取最新版本？(Y/N)")
				if reply == "N" or reply == "n":
					continue
			server_list,error,version_list = getFileServerName_client(decode_cmd[1])
			print(server_list,version_list,local_chunk_version)
			if error == 0:
				print("没有此文件，重新输入命令")
				continue
			error = get_file(server_list,decode_cmd[1],chunk_list,version_list,local_chunk_version)
			if error == 1:
				print(decode_cmd[1],"传输成功")
			else:
				print(decode_cmd[1],"数据节点数据丢失，传输失败")
	elif (decode_cmd[0] == "mkfile"):
		if len(decode_cmd) == 1:
			print("请输入文件名")
		else:
			error = mkfile_client(decode_cmd[1],upload_log)
			if error == 0:
				print("已有该文件名，请换个名字")
			else:
				print("创建成功")
	elif (decode_cmd[0] == "write"):
		if len(decode_cmd) == 1:
			print("请输入文件名")
		else:
			error = writefile_client(decode_cmd[1],"chunk0001")
			if error == 1:
				print("更新文件成功")
	elif(decode_cmd[0] == "rm"):
		if len(decode_cmd) == 1:
			print("请输入文件名")
		else:
			error = rm_client(decode_cmd[1])
			if error == 1:
				print("删除成功")
	elif (decode_cmd[0] == "exit"):
		break
c.close()
# print(c.root.get_answer())