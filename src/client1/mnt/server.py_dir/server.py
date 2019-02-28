import rpyc
import json
import codecs
import sys,os
from rpyc.utils.server import ThreadedServer
ROOT = "file"
file_tree = ROOT + "/" + "file_tree.txt"
map_json = ROOT + "/" +"chunk_map.json"
client = {}
CLIENT_INDEX = -1
kilobytes = 1024
megabytes = kilobytes*1000
chunksize = int(10*megabytes)#default chunksize
port1 = 18862
port2 = 18863
port3 = 18864
port4 = 18865
port5 = 18866
Port_Map = {"1":[port1,0],"2":[port2,0],"3":[port3,0],"4":[port4,0],"5":[port5,0]}
server_map = {port1:"1",port2:"2",port3:"3",port4:"4",port5:"5"}
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
			data[dic["dir_name"]] = temp_dic
	return data
#split文件的辅助函数
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
		self.FS = None
		self.FileList = read_Json(map_json)
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
		return (final_dir in self.FS.file_dict.keys())
	## ls:
	def exposed_ls(self,name):
		self.print_for_debug()
		return self.FS.file_dict[name]
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
		return 1
	def exposed_getList(self,name):
		print(self.FS.client_namespace)
		all_file_list = self.FileList[self.FS.client_namespace]
		if name not in all_file_list.keys():
			return None,0
		chunk_list = all_file_list[name]
		# print(chunk_list)
		final_dic = {}
		#默认datanode一定有所有的数据
		print(type(chunk_list))
		for key,value in chunk_list.items():
			for all_server_list in value["file_server_list"]:
				if Port_Map[all_server_list][1] == 0:
					final_dic[key] = Port_Map[all_server_list][0]
					Port_Map[all_server_list][1] = 1
					break
			if key not in final_dic:
				final_dic[key] = Port_Map[value["file_server_list"][0]][0]
		print(type(final_dic))
		return final_dic,1
	def exposed_trans_finish(self,port,name,chunk_index):
		server_index = server_map[port]
		Port_Map[server_index][1] = 0
		self.FileList[self.FS.client_namespace][name][chunk_index]["client_list"].append(self.FS.client_index)

		# for element in chunk_list:
# FileList = read_Json(map_json)
# print(FileList)
t = ThreadedServer(MyService, port = 18861,protocol_config = {"allow_public_attrs" : True})
t.start()
# print(mkdir_tree('try'))