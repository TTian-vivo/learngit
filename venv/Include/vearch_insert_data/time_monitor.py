import time
import os

start_time=time.time()
os.system("python /home/app/make_index_mltiprocess.py > out.log")
end_time=time.time()

print("the total time is :",end_time-start_time)