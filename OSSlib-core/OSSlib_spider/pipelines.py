# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html
import pymysql
import datetime
from OSSlib_spider import settings
from OSSlib_spider.items import *
def dbHandle():
    conn = pymysql.connect(
        host=settings.MYSQL_HOST,
        db=settings.MYSQL_DBNAME,
        user=settings.MYSQL_USER,
        passwd=settings.MYSQL_PASSWD,
        charset='utf8',
        use_unicode=True
    )
    return conn
class OsslibSpiderPipeline(object):
    def process_item(self, item, spider):
        dbObject = dbHandle()
        cursor = dbObject.cursor()
        if isinstance(item, OsslibIssue):
            cursor.execute("select * from osslib_issue where issue_id=%s and issue_number = %s",
                           (item['issue_id'], item['issue_number']))
            result = cursor.fetchone()
            if result:
                sql = "update osslib_issue set  oss_id =%s,issue_state = %s,user_id=%s, " \
                      "issue_user_type = %s,issue_create_time = %s,issue_update_time = %s," \
                      "issue_close_time = %s,issue_comment_count = %s,update_time=%s ,issue_body=%s,issue_title=%s  " \
                      " where issue_id = %s and issue_number=%s"
                try:
                    cursor.execute(sql, (item['oss_id'], item['issue_state'], item['user_id'], item['issue_user_type'],
                                         item['issue_create_time'], item['issue_update_time'], item['issue_close_time'],
                                         item['issue_comment_count'], item['update_time'], item['issue_body'],
                                         item['issue_title'],
                                         item['issue_id'], item['issue_number']))
                    cursor.connection.commit()
                except BaseException as e:
                    print(e)
                    dbObject.rollback()
            else:
                sql = "insert into osslib_issue(oss_id , issue_state , user_id,issue_user_type," \
                      "issue_create_time,issue_update_time,issue_close_time,issue_comment_count,update_time," \
                      "issue_id,issue_number,issue_body,issue_title) " \
                      "VALUES(%s,%s,%s, %s,%s,%s,%s, %s,%s,%s,%s,%s,%s)"
                try:
                    cursor.execute(sql, (item['oss_id'], item['issue_state'], item['user_id'], item['issue_user_type'],
                                         item['issue_create_time'], item['issue_update_time'], item['issue_close_time'],
                                         item['issue_comment_count'], item['update_time'],
                                         item['issue_id'], item['issue_number'], item['issue_body'],
                                         item['issue_title']))
                    cursor.connection.commit()
                except BaseException as e:
                    print(item)
                    print(e)
                    dbObject.rollback()
        elif isinstance(item, OsslibPulls):
            cursor.execute("select * from osslib_pulls where pull_id=%s and pull_number = %s",
                           (item['pull_id'], item['pull_number']))
            result = cursor.fetchone()
            if result:
                sql = "update osslib_pulls set  oss_id =%s,pull_state = %s,user_id=%s, " \
                      "pull_author_association = %s,pull_create_time = %s,pull_update_time = %s," \
                      "pull_closed_time = %s,pull_merged_time = %s,pull_is_merged = %s,pull_title = %s," \
                      "pull_body = %s,pull_is_reviewed = %s,pull_comments = %s,review_comments = %s,request_reviewer=%s,update_time=%s " \
                      " where pull_id = %s and pull_number=%s"
                try:

                    cursor.execute(sql, (
                    item['oss_id'], item['pull_state'], item['user_id'], item['pull_author_association'],
                    item['pull_created_at'], item['pull_update_at'], item['pull_closed_at'],
                    item['pull_merged_at'], item['pull_is_merged'], item['pull_title'],item['pull_body'],
                    item['pull_is_reviewed'],item['pull_comments'],item['review_comments'],item['request_reviewer'],item['update_time'],
                    item['pull_id'], item['pull_number']))
                    cursor.connection.commit()
                except BaseException as e:
                    print(e)
                    dbObject.rollback()
            else:
                sql = "insert into osslib_pulls(oss_id , pull_state , user_id,pull_author_association," \
                      "pull_create_time,pull_update_time,pull_closed_time,pull_merged_time,pull_is_merged,pull_title," \
                      "pull_body,pull_is_reviewed,update_time,pull_comments,review_comments,request_reviewer," \
                      "pull_id,pull_number) " \
                      "VALUES(%s,%s,%s, %s,%s,%s,%s, %s,%s,%s,%s, %s,%s,%s,%s,%s,%s,%s)"
                try:
                    cursor.execute(sql, (
                    item['oss_id'], item['pull_state'], item['user_id'], item['pull_author_association'],
                    item['pull_created_at'], item['pull_update_at'], item['pull_closed_at'],
                    item['pull_merged_at'], item['pull_is_merged'], item['pull_title'],item['pull_body'],
                    item['pull_is_reviewed'],item['update_time'],item['pull_comments'],item['review_comments'],item['request_reviewer'],
                    item['pull_id'], item['pull_number']))
                    cursor.connection.commit()
                except BaseException as e:
                    print(e)
                    dbObject.rollback()
        elif isinstance(item, OsslibPullsReviewers):
            cursor.execute("select * from osslib_pulls_review where review_id=%s ",
                           (item['review_id']))
            result = cursor.fetchone()
            if result:
                sql = "update osslib_pulls_review set  is_requested =%s,reviewed_time =%s,update_time =%s,author_association=%s " \
                      "where review_id=%s "
                try:
                    cursor.execute(sql, (item['is_requested'],item['reviewed_time'],item['update_time'],item['author_association'],
                                         item['review_id']))
                    cursor.connection.commit()
                except BaseException as e:
                    print(e)
                    dbObject.rollback()
            else:
                sql = "insert into osslib_pulls_review(review_id,oss_id , reviewer_id,pull_id,is_requested,reviewed_time,update_time,author_association) " \
                      "VALUES(%s,%s,%s,%s,%s,%s,%s,%s)"
                try:
                    cursor.execute(sql, (item['review_id'],item['oss_id'],item['reviewer_id'],item['pull_id'],
                                         item['is_requested'], item['reviewed_time'], item['update_time'],item['author_association']))
                    cursor.connection.commit()
                except BaseException as e:
                    print(e)
                    dbObject.rollback()
        elif isinstance(item, OsslibCommit):
            cursor.execute("select * from osslib_commit where commit_node_id=%s ",
                           (item['commit_node_id']))
            result = cursor.fetchone()
            if result:
                sql = "update osslib_commit set  oss_id =%s,user_id =%s,update_time =%s,commit_time=%s " \
                      "where commit_node_id=%s "
                try:
                    cursor.execute(sql, (item['oss_id'], item['user_id'], item['update_time'], item['commit_time'],
                                         item['commit_node_id']))
                    cursor.connection.commit()
                except BaseException as e:
                    print(e)
                    dbObject.rollback()
            else:
                sql = "insert into osslib_commit(commit_node_id, oss_id , update_time, commit_time) " \
                      "VALUES(%s,%s,%s,%s)"
                try:
                    cursor.execute(sql, (item['commit_node_id'], item['oss_id'], item['update_time'], item['commit_time']))
                    cursor.connection.commit()
                except BaseException as e:
                    print(e)
                    dbObject.rollback()
        elif isinstance(item, OsslibDeveloper):
            cursor.execute("select * from osslib_developer where oss_id=%s and user_id = %s",
                           (item['oss_id'], item['user_id']))
            result = cursor.fetchone()
            if result:
                sql = "update osslib_developer set  user_commit_count =%s,update_time = %s" \
                      "where oss_id = %s and user_id=%s"
                try:
                    cursor.execute(sql, (item['user_commit_count'], item['update_time'], item['oss_id'], item['user_id']))
                    cursor.connection.commit()
                except BaseException as e:
                    print(e)
                    dbObject.rollback()
            else:
                sql = "insert into osslib_developer(oss_id, user_id, user_commit_count, update_time) " \
                      "VALUES(%s,%s,%s, %s)"
                try:
                    cursor.execute(sql, (item['oss_id'], item['user_id'], item['user_commit_count'], item['update_time']))
                    cursor.connection.commit()
                except BaseException as e:
                    print(e)
                    dbObject.rollback()
        elif isinstance(item, OsslibUser):
            cursor.execute("select * from osslib_user where user_id=%s", (item['user_id']))
            result = cursor.fetchone()
            if result:

                sql = "update osslib_user set user_name=%s , user_fullname =%s , " \
                      "avatar_url=%s,follows_count=%s,repos_count=%s ,blog_url=%s ,email_url=%s ," \
                      "org_member_count=%s,user_type =%s,user_create_time=%s ,user_update_time=%s ,update_time =%s ," \
                      "user_location = %s,user_company = %s where user_id =%s"
                try:
                    cursor.execute(sql, (item['user_name'], item['user_fullname'],
                                         item['avatar_url'], item['follows_count'], item['repos_count'],
                                         item['blog_url'], item['email_url'],
                                         item['org_member_count'], item['user_type'], item['user_create_time'],
                                         item['user_update_time'], item['update_time'], item['location'],
                                         item['company'],
                                         item['user_id']))
                    cursor.connection.commit()
                except BaseException as e:
                    print(e)
                    dbObject.rollback()

                #pass
            else:
                sql = "insert into osslib_user(user_id , user_name , user_fullname , " \
                      "avatar_url,follows_count,repos_count ,blog_url ,email_url ," \
                      "org_member_count,user_type ,user_create_time ,user_update_time ,update_time," \
                      "user_location,user_company)" \
                      " VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
                try:

                    cursor.execute(sql, (item['user_id'], item['user_name'], item['user_name'],
                                         item['avatar_url'], item['follows_count'], item['repos_count'],
                                         item['blog_url'], item['email_url'],
                                         item['org_member_count'], item['user_type'], item['user_create_time'],
                                         item['user_update_time'], item['update_time'], item['location'],
                                         item['company']))
                    cursor.connection.commit()
                except BaseException as e:
                    print(e)
                    dbObject.rollback()
        return item
