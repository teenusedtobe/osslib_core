from model.common_model import *

from multiprocessing.managers import BaseManager
import queue
 
from core.code_analysis import *
from core.statistics import *
 
task_queue = queue.Queue()
 

def read_file(q, i):
    while(True):
        pass

def return_task_queue():
    global task_queue
    return task_queue
class QueueManager(BaseManager): pass

def main_process():
    CodeAnalysis().get_code()
    #CodeAnalysis().code_analysis()
    Statistics().oss_stat()

if __name__ == '__main__':
    '''
    sched = BlockingScheduler(timezone='MST')
    sched.add_job(main_process, 'interval', seconds=3)
    sched.start()
    '''
    Statistics().oss_stat()


    #train()





    '''
    all_json_file = list(path.glob('**/*.json'))
    QueueManager.register('get_task_queue', callable=return_task_queue)
    manager = QueueManager(address=('127.0.0.1', 34512), authkey=b'abc')
    manager.start()
    task = manager.get_task_queue()
    for json_file in all_json_file:
        task.put(json_file)
    for i in range(50):
        p = multiprocessing.Process(target=read_file, args=(task,i,))
        p.start()
    p.join()
    '''
#github_event_watch(repo_id=1,event_id=1,event_payload='1',event_time='1',update_time='1',user_id=1,org_id=1,event_public=1)