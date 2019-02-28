import rpyc
import json
import codecs
import sys,os
import random
from rpyc.utils.server import ThreadedServer
from rpyc.lib.compat import pickle, execute, is_py3k
from rpyc.core.service import ClassicService, Slave
from rpyc.utils import factory
from rpyc.core.service import ModuleNamespace
from contextlib import contextmanager
ROOT = "file"
file_tree = ROOT + "/" + "file_tree.txt"
map_json = ROOT + "/" +"chunk_map.json"
client = {}
CLIENT_INDEX = -1
kilobytes = 1024
megabytes = kilobytes*1000
chunksize = int(10*megabytes)#default chunksize
DN_num = 5
port1 = 18862
port2 = 18863
port3 = 18864
port4 = 18865
port5 = 18866
DataNode_ip = "127.0.0.1"
Port_Map = {"1":[port1,0],"2":[port2,0],"3":[port3,0],"4":[port4,0],"5":[port5,0]}
server_map = {port1:"1",port2:"2",port3:"3",port4:"4",port5:"5"}
lock_info = {}#文件名-块号-被谁锁了
def read_Json(map_json):
	data = {}
	with codecs.open(map_json, "r", "utf-8") as f:
		for line in f:
			dic = json.loads(line)
			# print(dic["dir_name"])
			# print(dic["file"])
			temp_dic = {}
			for element in dic["file"]:
				# print(element)
				final_temp = {}
				for file_info in element["file_chunk"]:
					# print(file_info)
					chunk_name = file_info["chunk_name"]
					file_info.pop("chunk_name")
					final_temp[chunk_name] = file_info
					# print(final_temp)
				temp_dic[element["file_name"]] = final_temp
			temp_dic["Null"] = {}
			data[dic["dir_name"]] = temp_dic
	return data
def write_Json(data,result_json = None,dest_root = None,dest_file = None,change_chunk_index = None,change_info_name = None,info = None):
	final_list = []
	for key,value in data.items():
		final_json = {}
		# print(key)
		final_json["dir_name"] = key
		file_list = []
		file_json = {}
		for file_name,file_value in value.items():
			if file_name == "Null":
				continue
			# print(file_name,file_value)
			file_json["file_name"] = file_name
			file_chunk_list = []
			for chunk_name,chunk_value in file_value.items():
				file_chunk = {}
				file_chunk["chunk_name"] = chunk_name
				file_chunk["file_server_list"] = chunk_value["file_server_list"]
				file_chunk["client_list"] = chunk_value["client_list"]
				file_chunk["Version"] = chunk_value["Version"]
				file_chunk["lock_symbol"] = chunk_value["lock_symbol"]
				if chunk_name == change_chunk_index and dest_root == key and dest_file == file_name:
					# print("&*&*&*&*",file_chunk,info)
					file_chunk[change_info_name] = info
					# print("&*&*&*&*",file_chunk,info,type(file_chunk["client_list"]),type(file_chunk["file_server_list"]))
				file_chunk_list.append(file_chunk)
			file_json["file_chunk"] = file_chunk_list
			file_list.append(file_json.copy())
		final_json["file"] = file_list
		final_list.append(final_json.copy())
		# print(final_list)
	with open(result_json, 'w') as json_file:
		for index in final_list:
			json_str = json.dumps(index)
			json_file.write(json_str)
			json_file.write("\n")
#split文件的辅助函数

data = (read_Json(map_json))
write_Json(data,map_json)
def splitfile(fromfile,todir,chunksize=chunksize):
	if not os.path.exists(todir):#check whether todir exists or not
		os.mkdir(todir)		
	else:
		for fname in os.listdir(todir):
			os.remove(os.path.join(todir,fname))
	partnum = 0
	inputfile = open(fromfile,'rb')#open the fromfile
	while True:
		chunk = inputfile.read(chunksize)
		if not chunk:             #check the chunk is empty
			 break
		partnum += 1
		filename = os.path.join(todir,(fromfile + '_chunk%04d'%partnum))
		fileobj = open(filename,'wb')#make partfile
		fileobj.write(chunk)		#write data into partfile
		fileobj.close()
	return partnum
#split 文件
def split(fromfile,todir):
	try:
		parts = splitfile(fromfile,todir,chunksize)
	except:
		return -1
	else:
		return parts
#合并文件辅助函数
def joinfile(fromdir,filename,todir):
	if not os.path.exists(todir):
		os.mkdir(todir)
	if not os.path.exists(fromdir):
		print('Wrong directory')
	outfile = open(os.path.join(todir,filename),'wb')
	files = os.listdir(fromdir) #list all the part files in the directory
	files.sort()	            #sort part files to read in order
	for file in files:
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
	return 0
#ls的辅助函数
def read_file_tree(file_tree):
	f = open(file_tree,'r')
	file_dict = {}
	for line in f.readlines():
		line = line.strip('\n')
		f_list = line.split(' ')
		temp_list = []
		f_list[1] = f_list[1].split(',')
		if '' in f_list[1]:
			f_list[1].remove('')
		f_list[2] = f_list[2].split(',')
		if '' in f_list[2]:
			f_list[2].remove('')
		temp_list.append(f_list[1])
		temp_list.append(f_list[2])
		file_dict[f_list[0]] = temp_list
	# print(file_dict)
	f.close()
	return file_dict
def ls_tree(root = '.'):
	file_dict = read_file_tree(file_tree)
	return file_dict[root]
def mkdir_tree(name,root = '.'):
	file_dict = read_file_tree(file_tree)
	file_dict[root][0].append(name)
	name = root + '/' + name
	file_dict[name]=[[],[]]
	f = open(file_tree,'w')
	for key,value in file_dict.items():
		temp_str = str(key) + ' '
		for temp in value[0]:
			temp_str = temp_str + temp + ','
		if(len(value[0]) == 0):
			temp_str = temp_str + ','
		temp_str = temp_str[:-1]+' '
		for temp in value[1]:
			temp_str = temp_str +temp +','
		if(len(value[1]) == 0):
			temp_str = temp_str + ','
		temp_str = temp_str[:-1]+'\n'
		f.write(temp_str)
	f.close()
	return 1
def mkfile_server(name):
	pass
class MyService(rpyc.Service):
	def on_connect(self,port):
		# code that runs when a connection is created
		# (to init the serivce, if needed)
		global CLIENT_INDEX
		CLIENT_INDEX = CLIENT_INDEX + 1
		temp = []
		self.FS = None#file tree
		self.FileList = read_Json(map_json)#fileServer list
		# print(self.getconfig())
	def on_disconnect(self):
		# code that runs when the connection has already closed
		# (to finalize the service, if needed)
		global CLIENT_INDEX
		CLIENT_INDEX = CLIENT_INDEX - 1
		client_symbol = self.FS.client_index
		client = client.pop(client_index)
		print(client_index,"is out")
	class exposed_ServerFile:
		def __init__(self,client_symbol,client_namespace):
			self.client_index = client_symbol
			self.client_namespace = client_namespace
			self.client_cache = []
			self.file_dict = read_file_tree(file_tree)
	#init the server_FS object when the client connect to server
	def exposed_build(self,client_symbol,client_namespace):
		self.FS = self.exposed_ServerFile(client_symbol,client_namespace)
		client[client_symbol] = self
		return 1
	def print_for_debug(self):
		print(self.FS.client_index)
		print(self.FS.client_namespace)
		print(self.FS.client_cache)
		print(self.FS.file_dict)
	##cd:
	def exposed_changedir(self,dest_dir,client_symbol):

		self.FS.client_namespace = dest_dir
		# self.print_for_debug(client_symbol)
	def exposed_exist_dir(self,final_dir):
		self.FS.file_dict = read_file_tree(file_tree)
		return (final_dir in self.FS.file_dict.keys())
	def exposed_exist_file(self,file_name):
		# all_file_list = self.FileList[self.FS.client_namespace]
		# print(all_file_list.keys(),"~~~")
		self.FS.file_dict = read_file_tree(file_tree)
		return (file_name in self.FS.file_dict[self.FS.client_namespace][1])
	## ls:
	def exposed_ls(self,name):
		self.print_for_debug()
		self.FS.file_dict = read_file_tree(file_tree)
		return self.FS.file_dict[name]
	def exposed_rmfile(self,name):
		if name in self.FS.file_dict[self.FS.client_namespace][1]:
			self.FS.file_dict[self.FS.client_namespace][1].remove(name)
		print(self.FS.file_dict[self.FS.client_namespace],"%%%%%")
		f = open(file_tree,'w')
		for key,value in self.FS.file_dict.items():
			temp_str = str(key) + ' '
			for temp in value[0]:
				temp_str = temp_str + temp + ','
			if(len(value[0]) == 0):
				temp_str = temp_str + ','
			temp_str = temp_str[:-1]+' '
			for temp in value[1]:
				temp_str = temp_str +temp +','
			if(len(value[1]) == 0):
				temp_str = temp_str + ','
			temp_str = temp_str[:-1]+'\n'
			f.write(temp_str)
		f.close()
		for key in client.keys():
			print(client[key].FileList[self.FS.client_namespace])
			if name in client[key].FileList[self.FS.client_namespace]:
				del client[key].FileList[self.FS.client_namespace][name]
			print(client[key].FileList[self.FS.client_namespace])
		write_Json(self.FileList,map_json)

		return 1
	def exposed_mkdir(self,name,root = '.'):
		self.FS.file_dict[root][0].append(name)
		name = root + '/' + name
		self.FS.file_dict[name]=[[],[]]
		f = open(file_tree,'w')
		for key in client.keys():
			client[key].FS.file_dict = self.FS.file_dict.copy()
		for key,value in self.FS.file_dict.items():
			temp_str = str(key) + ' '
			for temp in value[0]:
				temp_str = temp_str + temp + ','
			if(len(value[0]) == 0):
				temp_str = temp_str + ','
			temp_str = temp_str[:-1]+' '
			for temp in value[1]:
				temp_str = temp_str +temp +','
			if(len(value[1]) == 0):
				temp_str = temp_str + ','
			temp_str = temp_str[:-1]+'\n'
			f.write(temp_str)
		f.close()
		self.FileList[name] = {"Null":{}}
		write_Json(self.FileList,map_json)
		for key in client.keys():
			client[key].FS.file_dict = self.FS.file_dict.copy()
			client[key].FileList[name]= {"Null":{}}
		return 1
	def exposed_mkfile(self,name):#暂时只支持小文件的创建与修改，也就是小于一个chunk大小10M
		print("-----",self.FS.client_namespace)
		print(self.FileList)
		alllist = self.FileList[self.FS.client_namespace]
		print(alllist,"^^^^^^^^^")
		chunk_info = {}
		file_info = {}
		chunk_info['file_server_list'] = []
		chunk_info['client_list'] = []
		chunk_info['client_list'].append(self.FS.client_index)
		chunk_info['Version'] = -1
		chunk_info['lock_symbol'] = 1#1代表写锁，为了debug先为0
		chunk_name = "chunk0001"
		chunk_lock = {}
		chunk_lock[chunk_name] = self.FS.client_index
		lock_info[name] = chunk_lock
		file_info[chunk_name] = chunk_info
		self.FileList[self.FS.client_namespace][name] = file_info
		print(self.FileList[self.FS.client_namespace])
		self.FS.file_dict[self.FS.client_namespace][1].append(name)
		f = open(file_tree,'w')
		for key,value in self.FS.file_dict.items():
			temp_str = str(key) + ' '
			for temp in value[0]:
				temp_str = temp_str + temp + ','
			if(len(value[0]) == 0):
				temp_str = temp_str + ','
			temp_str = temp_str[:-1]+' '
			for temp in value[1]:
				temp_str = temp_str +temp +','
			if(len(value[1]) == 0):
				temp_str = temp_str + ','
			temp_str = temp_str[:-1]+'\n'
			f.write(temp_str)
		f.close()
		write_Json(self.FileList,map_json)
		for key in client.keys():
			client[key].FileList[self.FS.client_namespace][name] = file_info
		return 0,Port_Map
	# 这里有可能出现冲突问题
	def exposed_getFile_ServerList(self,name):
		print(self.FS.client_namespace)
		self.FileList = read_Json(map_json)
		all_file_list = self.FileList[self.FS.client_namespace]
		if name not in all_file_list.keys():
			return {},0,{}
		chunk_list = all_file_list[name]
		print(chunk_list,"***")
		final_dic = {}
		version_dic = {}
		#默认datanode一定有所有的数据
		print(type(chunk_list))
		for key,value in chunk_list.items():
			if value["lock_symbol"] == 1 :#1代表写锁
				continue
			for all_server_list in value["file_server_list"]:
				if Port_Map[all_server_list][1] == 0 :
					final_dic[key] = Port_Map[all_server_list][0]
					version_dic[key] = value["Version"]
					Port_Map[all_server_list][1] = 1
					for key_client in client.keys():
						client[key_client].FileList[self.FS.client_namespace][name][key]["lock_symbol"] = 2#2代表读锁
					break
			if key not in final_dic:
				final_dic[key] = Port_Map[value["file_server_list"][0]][0]
				version_dic[key] = value["Version"]
				for key_client in client.keys():
					client[key_client].FileList[self.FS.client_namespace][name][key]["lock_symbol"] = 2#2代表读锁
		all_file_list = self.FileList[self.FS.client_namespace]
		chunk_list = all_file_list[name]
		for key,value in chunk_list.items():
			if key not in final_dic:
				while self.exposed_file_rlock(name,key) != 1:#自旋锁
					continue
				for all_server_list in value["file_server_list"]:
					if Port_Map[all_server_list][1] == 0 : #1代表写锁
						final_dic[key] = Port_Map[all_server_list][0]
						version_dic[key] = value["Version"]
						Port_Map[all_server_list][1] = 1
						for key_client in client.keys():
							client[key_client].FileList[self.FS.client_namespace][name][key]["lock_symbol"] = 2#2代表读锁
						break
				if key not in final_dic:
					version_dic[key] = value["Version"]
					final_dic[key] = Port_Map[value["file_server_list"][0]][0]
					for key_client in client.keys():
						client[key_client].FileList[self.FS.client_namespace][name][key]["lock_symbol"] = 2#2代表读锁
		print(type(final_dic))
		return final_dic,1,version_dic
	def exposed_updatedFileinfo(self,name,info_name,information,chunk_index):
		local_inf = information
		if type(information) != int:
			local_inf = list(information)
		if info_name == "file_server_list":
			chunk_size = 40960
			cur_index = local_inf[0]
			local_inf = self.FileList[self.FS.client_namespace][name][chunk_index][info_name]
			cur_port = Port_Map[cur_index][0]
			if len(self.FileList[self.FS.client_namespace][name][chunk_index][info_name]) == 0:
				self.FileList[self.FS.client_namespace][name][chunk_index][info_name].append(cur_index)
			if len(self.FileList[self.FS.client_namespace][name][chunk_index][info_name]) == 1:
				
				backup_index = int(cur_index) % 4 + 1
				
				local_inf.append(str(backup_index))
				
			for backup_index in local_inf:
				if backup_index == cur_index:
					continue
				backup_port = Port_Map[str(backup_index)][0]
				conn1 = rpyc.classic.connect(DataNode_ip,int(cur_port))
				conn2 = rpyc.classic.connect(DataNode_ip,int(backup_port))
				fromfilename = self.FS.client_namespace + ":" + name + "_dir" + "/" + name + "_" + chunk_index
				fromfiledir = self.FS.client_namespace + ":" + name + "_dir"
				fromfilename = "Data/" + fromfilename[2:]
				fromfiledir = "Data/" + fromfiledir[2:]
				count = 0
				if not conn2.modules.os.path.isdir(fromfiledir):
					conn2.modules.os.makedirs(fromfiledir)

				with conn1.builtin.open(fromfilename, "rb") as rf:
					with conn2.builtin.open(fromfilename, "wb") as lf:
						while True:
							print(chunk_size,count)
							count = count+1
							buf = rf.read(chunk_size)
							print("success...")
							if not buf:
								print("outing???")
								break
							lf.write(buf)
				conn1.close()
				conn2.close()
				# for 
		self.FileList[self.FS.client_namespace][name][chunk_index][info_name] = local_inf
		write_Json(self.FileList,map_json,dest_root = self.FS.client_namespace,dest_file = name,change_chunk_index = chunk_index,change_info_name = info_name,info = local_inf)
		# print("*******")
		# return 1
	def exposed_file_wlock(self,name,chunk_index):
		lock = self.FileList[self.FS.client_namespace][name][chunk_index]["lock_symbol"]
		if lock != 0:
			return 0
		for key in client.keys():
			client[key].FileList[self.FS.client_namespace][name][chunk_index]["lock_symbol"] = 1
		# print("ABCDEFG~~~~")
		write_Json(self.FileList,map_json)
		return 1
	def exposed_file_rlock(self,name,chunk_index):
		while self.FileList == {}:
			self.FileList = read_Json(map_json)
		lock = self.FileList[self.FS.client_namespace][name][chunk_index]["lock_symbol"]
		if self.FileList[self.FS.client_namespace][name][chunk_index]["lock_symbol"] == 1:
			return 0
		for key in client.keys():
			client[key].FileList[self.FS.client_namespace][name][chunk_index]["lock_symbol"] = 2
		# write_Json(self.FileList,map_json)
		return 1
	def exposed_file_unlock(self,name,chunk_index):
		self.FileList[self.FS.client_namespace][name][chunk_index]["lock_symbol"] = 0
		for key in client.keys():
			client[key].FS.file_dict = self.FS.file_dict.copy()
			client[key].FileList = self.FileList.copy() 
		write_Json(self.FileList,map_json)
		return 1
	def exposed_trans_finish(self,port,name,chunk_index):
		server_index = server_map[port]
		Port_Map[server_index][1] = 0
		version = self.FileList[self.FS.client_namespace][name][chunk_index]["Version"]
		if self.FS.client_index not in self.FileList[self.FS.client_namespace][name][chunk_index]["client_list"]:
			self.FileList[self.FS.client_namespace][name][chunk_index]["client_list"].append(self.FS.client_index)
		self.FileList[self.FS.client_namespace][name][chunk_index]["lock_symbol"] = 0
		for key in client.keys():
			client[key].FS.file_dict = self.FS.file_dict.copy()
			client[key].FileList[self.FS.client_namespace][name][chunk_index]["lock_symbol"] = 0
		write_Json(self.FileList,map_json)
		# for element in chunk_list:
# FileList = read_Json(map_json)
# print(FileList)
t = ThreadedServer(MyService, port = 18861,protocol_config = {"allow_public_attrs" : True})
t.start()
# print(mkdir_tree('try'))