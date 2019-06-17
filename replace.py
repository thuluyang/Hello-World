import os
import sys
import pyspark.sql.functions

def inplace_change(filename, old_string, new_string):
  #The replace string function
    with open(filename,"r") as fin:
        filedata = fin.read()
        if old_string not in filedata:
            print('"{old_string}" not found in file, skipped {filename}'.format(**locals()))
            return
        #Check if the hdfs file path is replaced with adfs file path or not.
        #if it is replaced, in the ddl file, there will be string like "/mnt/data/b2b"
        if new_string in filedata and old_string not in filedata:
          #If /mnt/ in file and /mnt in new_string, that means this files are processed to adfs path
          print('"{old_string}" has already been replaced with "{new_string}" in file, skipped {filename}'.format(**locals()))
          return

    with open(filename, 'w') as fout:
      #/mnt/ not in file OR /mnt not in new_string, process the replace with other strings.
      fout.write(filedata.replace(old_string, new_string))
      print('"{old_string}" to "{new_string}" in {filename} has been replaced'.format(**locals()))
    fout.close()
    fin.close()
    
def executeDDL(filename):
    # Open and read the file as a single buffer
    fd = open(filename, 'r')
    ddlFile = fd.read()
    fd.close()
    # all SQL commands (split on ';')
    sqlCommands = ddlFile.split(';')
    # Execute every command from the input file
    for command in sqlCommands:
      if not command.isspace():
      # if the command is empty commands, with whitespace, \n or \t, then below command will not be executed.
        try:
          if "/mnt/" not in command:
            command=command.replace("/data/","/mnt/data/")
          spark.sql(command)
          print("DDL command\n" + command + "\nis executed successfully.\n")
        except Exception as ex:
          print("Error when run below sql command "+command+"\n"+ ":: {0}".format(ex))

#directory is the file path which contains the DDL files
directory="/dbfs/FileStore/ddlfiles_yang/"

hdfs_path="/data/b2b/"
dbfs_path="/mnt/data/b2b/"

# directory=sys.argv[1]
# hdfs_path=sys.argv[2]
# adfs_path=sys.argv[3]

#execute 1 single ddl
executeDDL("/dbfs/FileStore/ddlfiles_yang/rd_manager.hql")

#Iterate thru the HQL files in a folder
# for filename in os.listdir(directory):
#     if filename.endswith(".hql"):
#       #inplace_change(directory+filename,hdfs_path,dbfs_path)
#       executeDDL(directory+filename)
