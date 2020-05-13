from multiprocessing import Process
import os
from datetime import datetime
import subprocess

parallel_process = 50
SOURCE_DIR_ABOSPLUTE_PATH = '/home/aditya/my-work'
#Tracker file and this script should not be in the SOURCE_DIR_ABOSPLUTE_PATH location
tracker_filename = '/home/aditya/tracker.txt'
s3_bucket="new-py-test-bucket"

def copy_dir(dir_path):
    # Print -> size
    #Print -> 
    print("Start copy for folder "+str(dir_path))
    connected = False
    if(os.path.isdir(dir_path)):
        while not connected:
            try:
                subprocess.check_output(['/usr/local/bin/aws', 's3', 'cp', dir_path, "s3://"+s3_bucket+"/"+str(dir_path.split('/')[-1]), '--recursive', '--sse', 'aws:kms'])
                connected = True
            except:
                continue
                
    else:
        
        while not connected:
            try:                
                subprocess.check_output(['/usr/local/bin/aws', 's3', 'cp', dir_path, "s3://"+s3_bucket+"/", '--sse', 'aws:kms'])
                connected = True
            except:
                continue
                
        
    # copy dir_path to S3 (silent)
    print("copy for folder "+str(dir_path)+" completed")
    
    with open(tracker_filename, 'a+') as out_file:
        out_file.write(dir_path+"\n")
    
def check_if_file_already_synced(dir_path):
    if(os.path.exists(tracker_filename)):
        with open(tracker_filename, 'r') as read_obj:
            for line in read_obj:
                if dir_path in line:
                    return True
    return False


def main():
    startTime = datetime.now()
    processes = []
    subdirs = [os.path.join(SOURCE_DIR_ABOSPLUTE_PATH, o) for o in os.listdir(SOURCE_DIR_ABOSPLUTE_PATH)]
    for dir_path in subdirs:
        if(check_if_file_already_synced(dir_path) != True):
            p = Process(target=copy_dir, args=(dir_path,))
            p.start()
            processes.append(p)
            if (len(processes) == parallel_process):
                for p in processes:
                    p.join() 
                processes = []
    print("All dir copied to S3 ")
    print("It took "+str(datetime.now() - startTime)+ " to complete")

if __name__=="__main__":
    main()
