from model.common_model import *
from core.common_git import *
from core.datacollector import *
from core.common import *
from apscheduler.schedulers.background import BlockingScheduler
import datetime


# 数据统计
class Statistics:

    def __init__(self):
        manager = get_thread_task_queue('statistics_queue')
        manager2 = get_thread_task_dict('statistics_dict')
        self.task_queue = manager.statistics_queue()
        self.task_dict = manager2.statistics_dict()

    def oss_stat(self):
        community_info = OsslibCommunity.select()
        data_dict = dict()
        data_dict.__setitem__('loc', 0)
        data_dict.__setitem__('doc', 0)
        data_dict.__setitem__('coc', 0)
        data_dict.__setitem__('foc', 0)

        lock = multiprocessing.Lock()
        for per_community_info in community_info:
            community_id = per_community_info.id
            community_list_info = OsslibCommunityList.select(OsslibCommunityList.q.community_id == community_id)
            item = dict()
            item['community_id'] = community_id
            item['update_time'] = time.strftime('%Y-%m-%d')
            OsslibStatistic(**item)
            osslib_statistic = OsslibStatistic.select(OsslibStatistic.q.community_id == community_id)
            #global_dict.__setitem__(str(community_id), osslib_statistic[0])
            self.task_dict.update({str(community_id):data_dict})
            #print(global_dict)
            for per_community_list in community_list_info:
                trans_info = []
                trans_info.append(per_community_list.oss_id)
                trans_info.append(per_community_list.community_id)
                self.task_queue.put(trans_info)
        print(self.task_dict)
        for i in range(4):
            p = multiprocessing.Process(target=self._statistic, args=(self.task_queue, self.task_dict, i, lock))
            p.start()
        p.join()

    @staticmethod
    def _statistic(q, global_dict, i, lock):
        print(global_dict)
        while True:
            info = q.get()
            oss_id = info[0]
            community_id = info[1]
            oss_mata = OsslibMetadata_2.get(oss_id)
            loc = oss_mata.oss_line_count
            doc = oss_mata.oss_developer_count
            foc = oss_mata.oss_file_count
            coc = oss_mata.oss_commit_count
            '''
            oss_issue = OsslibIssue.select(OsslibIssue.q.oss_id == oss_mata.oss_id)
            issue_count = 0
            issue_close_count = 0
            if oss_issue:
                for per_oss_issue in oss_issue:
                    issue_count += 1
                    if per_oss_issue.issue_state == 1:
                        issue_close_count += 1
            '''
            lock.acquire()
            a = dict()
            #global_dict[str(community_id)].update({'loc': global_dict[str(community_id)]['loc']+loc})
            global_dict.update({str(community_id):{'loc': 200}})
            #global_dict[str(community_id)]['loc'] += loc
            #global_dict[str(community_id)]['coc'] += coc
            #global_dict[str(community_id)]['foc'] += foc
            #global_dict[str(community_id)]['foc'] += foc
            #global_dict[str(community_id)]['coc'] += coc
            #global_dict[str(community_id)].issue_count += issue_count
            #global_dict[str(community_id)].issue_close_count += issue_close_count
            print(global_dict)
            lock.release()





