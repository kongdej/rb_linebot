#!/usr/local/bin/python3
import sys
import os
import os.path
import glob
import meilisearch
import json
import time
import secrets

# default Settings
homedir = os.path.dirname(os.path.realpath(__file__))
master_key = ''
clearAll = False

# get options
i = 0
try:
    for opt in sys.argv:
        if opt == '-i': # index
            indexs = sys.argv[i+1]
        elif opt == '-d': # document directory
            documents_in = homedir + sys.argv[i+1]
        elif opt == '-h': # meilisearch host address
            host_addr = sys.argv[i+1]
        elif opt == '-k': # meilisearch master-key
            master_key = sys.argv[i+1]
        elif opt == '-X': # clear all database and remove pdfs files
            clearAll = True
        i += 1

    print ('Index: ', indexs)
    print ('Directory:', documents_in)
    print ('Server Adddress:', host_addr)

except Exception as e:
    print ('** Error: need more options!')
    print ('\nExample: python3 update.py -i RB -d /documents/RB/pdfs -h http://127.0.0.1:7700 -k xxxx-xxxx -X\n')
    exit()

print ('Home directory: ', homedir)
print ('Master-Key:' , master_key)

# check document input
if not os.path.exists(documents_in):
    print('** Error: ', documents_in, 'is not found.!!')
    exit()

# check document out and create directory
documents_out = homedir + '/pdfs/'+indexs+''
if not os.path.exists(documents_out):
    os.makedirs(documents_out)

# meilisearch select index (database)
client = meilisearch.Client(host_addr, master_key)
index = client.index(indexs)

# check index exists
def indexExist():
    exist = False
    indexLists = client.get_indexes()
    for item in indexLists:
        if item['name'] == indexs:
            exist = True
    return exist


print (f':Start-----')
# option -X clear all
if clearAll:
    print (f':Delete pdf {documents_out} and database.')
    filelist = glob.glob(documents_out+"/*")
    for f in filelist:
        os.remove(f)    # delete all files
    if indexExist():
        client.index(indexs).delete()  # delete index

# Step 1) Add new documents ----------------------------------------------------
print ('\n*** add new documents ***')
n = 0    # total files
cnt = 0  # total new documents
found = { # find meilisearch with filename
    'nbHits' : 0
}
homepath = len(homedir.split('/')) # number of homedir path
path = os.walk(documents_in)
for root, directories, files in path:
    for file in files: # loop files in directory
        if file != '.DS_Store':  # skip mac dump file
            n += 1
            name = root+'/'+file
            indexLists = client.get_indexes()
            if indexExist(): #if not empty index
                found = index.search('',{'filters': 'title="'+file+'"'}) # search file in database

            if found['nbHits'] == 0: # if new document
                cnt += 1
                cat = root.split('/')[homepath+1]  # get directory name for category
                print (n, cat ,name)

                #extract pdf to output.txt
                os.system('java -jar ' + homedir + '/pdfbox-app-3.0.0-RC1.jar export:text  -i "'+ name +'" -o '+homedir+'/output.txt')
                #read output.text
                inputf = open(homedir + '/output.txt', 'r')
                text = ''
                for line in inputf:
                    try:
                        text += line   # todo-> dosomethings eg,limit content size or thainlp
                    except KeyError:
                        pass

                # insert documents into indexs
                token = secrets.token_hex(20) # getnerate pdf 20 charectors filename
                document_file = root.replace(homedir,"")
                # add document to meilisearch server
                index.add_documents([{
                    'id': token,
                    'title': file,
                    'category': cat,
                    'path': document_file,
                    'fullcontent': text
                }])
                inputf.close()
                # delete output.html
                os.remove(homedir + '/output.txt')
                # copy file to pdfs directory
                cpfiles = "cp '" + name + "' "+documents_out+"/"+token+".pdf"
                os.system(cpfiles)
                print(cpfiles)
                print ('------------------------------------------')

if cnt == 0 :
    print ('---> 0 document')
else:
    print ('***> insert new documents ', cnt)

# Step 2) remove documents -------------------------------------------------------------
print ("\n*** deleted documents ***")
results  = index.search('',{'limit':100000})
#print (results['hits'])
nr = 0
for item in results['hits']:
    file = homedir + item['path']+'/'+item['title']
    if not os.path.isfile(file):
        nr += 1
        rfile = documents_out+'/'+item['id']+'.pdf'
        print (rfile , '*-*' ,file)
        # remode index document
        client.index(indexs).delete_document(item['id'])
        # delete file
        os.remove(rfile)

if nr == 0:
    print ('---> 0 document')
else:
    print ('***> remove meilisearch documents  ',nr)
