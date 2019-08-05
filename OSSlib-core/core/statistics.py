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
            return_info = OsslibStatistic(**item)
            print(return_info.id)
            #exit(0)
            #osslib_statistic = OsslibStatistic.select(OsslibStatistic.q.community_id == community_id)
            #self.task_dict.update({str(community_id):osslib_statistic[0]})
            #self.task_dict.update({str(community_id):data_dict})
            #print(global_dict)
            for per_community_list in community_list_info:
                trans_info = []
                trans_info.append(per_community_list.oss_id)
                trans_info.append(per_community_list.community_id)
                trans_info.append(return_info.id)
                self.task_queue.put(trans_info)
        for i in range(4):
            p = multiprocessing.Process(target=self._statistic, args=(self.task_queue, i, lock))
            p.start()
        p.join()
        print('over')

    @staticmethod
    def _statistic(q, i, lock):
        while True:
            if q.empty():
                break
            try:
                info = q.get()
                oss_id = info[0]
                community_id = info[1]
                statistic_id = info[2]
                oss_mata = OsslibMetadata_2.get(oss_id)
                loc = oss_mata.oss_line_count
                print(loc)
                doc = oss_mata.oss_developer_count
                foc = oss_mata.oss_file_count
                coc = oss_mata.oss_commit_count

                #分析issue
                oss_issue = OsslibIssue.select(OsslibIssue.q.oss_id == oss_mata.oss_id)
                issue_count = 0
                issue_close_count = 0
                issue_close_time = 0
                core_issue_count = 0
                issue_comment_count = 0
                if oss_issue:
                    for per_oss_issue in oss_issue:
                        issue_count += 1
                        if per_oss_issue.issue_state == 1:
                            issue_close_count += 1
                            close_at = per_oss_issue.issue_close_time
                            open_at = per_oss_issue.issue_create_time
                            issue_close_time += time.mktime(
                                time.strptime(close_at, "%Y-%m-%dT%H:%M:%SZ")) - time.mktime(
                                time.strptime(open_at, "%Y-%m-%dT%H:%M:%SZ"))
                        if per_oss_issue.issue_user_type =='MEMBER' or per_oss_issue.issue_user_type =='COLLABORATOR':
                            core_issue_count += 1
                        issue_comment_count += per_oss_issue.issue_comment_count

                #分析pull
                oss_pull = oss_pull.select(oss_pull.q.oss_id == oss_mata.oss_id)
                pull_count = 0
                pull_comment_count = 0
                pull_review_comment_count = 0
                pull_review_count = 0
                core_pull_count = 0
                pull_merged_count = 0
                pull_merged_time = 0
                core_developer = []
                core_developer_count = 0
                if oss_pull:
                    for per_oss_pull in oss_pull:
                        pull_count += 1
                        pull_comment_count += per_oss_pull.pull_comments
                        pull_review_comment_count += per_oss_pull.review_comments
                        if per_oss_pull.pull_is_reviewed == 1:
                            pull_review_count += 1
                        if per_oss_pull.pull_is_merged == 1:
                            pull_merged_count += 1
                            merged_at = per_oss_pull.pull_merged_time
                            create_at = per_oss_pull.pull_create_time
                            pull_merged_time += time.mktime(
                                time.strptime(merged_at, "%Y-%m-%dT%H:%M:%SZ")) - time.mktime(
                                time.strptime(create_at, "%Y-%m-%dT%H:%M:%SZ"))
                        if per_oss_pull.pull_author_association =='MEMBER' or per_oss_pull.pull_author_association =='COLLABORATOR':
                            core_pull_count += 1
                            if per_oss_pull.user_id not in core_developer:
                                core_developer.append(per_oss_pull.user_id)
                                core_developer_count += 1


                lock.acquire()
                #global_dict[str(community_id)].update({'loc': global_dict[str(community_id)]['loc']+loc})
                '''
                global_dict.update({str(community_id):{'loc': global_dict.get(str(community_id)).get('loc')+loc,
                                                       'doc': global_dict.get(str(community_id)).get('doc')+doc,
                                                       'coc': global_dict.get(str(community_id)).get('coc')+coc,
                                                       'foc': global_dict.get(str(community_id)).get('foc')+foc}})
                '''
                osslib_statistic_info = OsslibStatistic.get(statistic_id)
                osslib_statistic_info.loc = osslib_statistic_info.loc + loc
                osslib_statistic_info.doc = osslib_statistic_info.doc + doc
                osslib_statistic_info.coc = osslib_statistic_info.coc + coc
                osslib_statistic_info.foc = osslib_statistic_info.foc + foc
                osslib_statistic_info.issue_comment_count = osslib_statistic_info.issue_comment_count + issue_comment_count
                osslib_statistic_info.core_issue_count = osslib_statistic_info.core_issue_count + core_issue_count
                osslib_statistic_info.issue_close_time = osslib_statistic_info.issue_close_time + issue_close_time/3600/24
                osslib_statistic_info.issue_count = osslib_statistic_info.issue_count + issue_count
                osslib_statistic_info.issue_close_count = osslib_statistic_info.issue_close_count + issue_close_count

                #global_dict[str(community_id)]['loc'] += loc
                #global_dict[str(community_id)]['coc'] += coc
                #global_dict[str(community_id)]['foc'] += foc
                #global_dict[str(community_id)]['foc'] += foc
                #global_dict[str(community_id)]['coc'] += coc
                #global_dict[str(community_id)].issue_count += issue_count
                #global_dict[str(community_id)].issue_close_count += issue_close_count

                lock.release()
            except BaseException as ex:
                print(ex)






