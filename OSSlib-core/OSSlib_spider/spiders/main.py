import scrapy
import json
import requests
from OSSlib_spider.items import *
from OSSlib_spider.spiders.request_header import getHeader
import time
import re
import random
import pymysql
import emoji
from OSSlib_spider import settings
from bs4 import BeautifulSoup
import re

emoji_pattern = re.compile(u"(\ud83d[\ude00-\ude4f])|"
                           # emoticons 
                           u"(\ud83c[\udf00-\uffff])|"
                           # symbols & pictographs (1 of 2) 
                           u"(\ud83d[\u0000-\uddff])|"
                           # symbols & pictographs (2 of 2) 
                           u"(\ud83d[\ude80-\udeff])|"
                           # transport & map symbols 
                           u"(\ud83c[\udde0-\uddff])"
                           # flags (iOS) 
                           "+", flags=re.UNICODE)


def remove_emoji(text):
    return emoji_pattern.sub(r'', text)


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


from scrapy.downloadermiddlewares.retry import RetryMiddleware


class OsslibSpider(scrapy.Spider):
    name = "repos"

    def __init__(self, repo=None, *args, **kwargs):
        super(OsslibSpider, self).__init__(*args, **kwargs)

    # f = open("1000.txt", "r")
    # fw = open('scrapyed.txt', 'a')
    scrapyed_list = []
    dbObject = dbHandle()
    cursor = dbObject.cursor()
    cursor.execute("select oss_repo_url from osslib_metadata_2")
    results = cursor.fetchall()
    for result in results:
        scrapyed_list.append(result[0])
    # urlList = f.read().splitlines()
    repo_index = 0
    start_urls = scrapyed_list

    # scrapyed_list.append("vanpelt/jsawesome")
    def parse(self, response):
        try:
            repos_data = json.loads(response.body.decode('utf-8'))
            # yield self.detail_parse(response)
            repo_url = repos_data['url']
            commit_url = repos_data['commits_url']
            oss_id = repos_data['id']
            '''

            '''
            '''
            dbObject = dbHandle()
            cursor = dbObject.cursor()
            cursor.execute("select * from github_repo_base_info where repo_id=%s", (repo_id))
            result = cursor.fetchone()
            if result:
                cursor.close()
                continue
            '''
            '''
            yield scrapy.Request(repo_url,callback = self.detail_parse,headers = getHeader())

            commit_activity_url = repos_data["url"] + "/stats/commit_activity"
            yield scrapy.Request(commit_activity_url, meta={"repo_id": repo_id}, callback=self.commit_activity_parse,headers=getHeader())

            code_frequency_url = repos_data["url"] + "/stats/code_frequency"
            yield scrapy.Request(code_frequency_url, meta={"repo_id": repo_id}, callback=self.code_frequency_parse,headers=getHeader())

            user_contributors_url = repos_data['url'] + "/stats/contributors"
            yield scrapy.Request(user_contributors_url, meta={"repo_id": repo_id},callback=self.user_contributors_parse, headers=getHeader())
            '''
            '''
            #用户数据采集
            owner_url = repos_data['owner']['url']
            yield scrapy.Request(owner_url, callback=self.detail_owner_parse, headers=getHeader())
            '''
            # developer数据采集
            contributors_url = repos_data['contributors_url'] + "?per_page=100"
            yield scrapy.Request(contributors_url, meta={"oss_id": oss_id}, callback=self.detail_contributors_parse, headers=getHeader())

            # pulls数据采集
            #pulls_url = repos_data["pulls_url"][0:-9] + "?state=all&per_page=100"
            #yield scrapy.Request(pulls_url, meta={"oss_id": oss_id, "pull_url": repos_data["pulls_url"][0:-9]}, callback=self.detail_pulls_parse, headers=getHeader())

            '''
            #comment数据采集
            command_url = repos_data["comments_url"][0:-9] + "?per_page=100"
            yield scrapy.Request(command_url,meta={"repo_id":repo_id}, callback=self.detail_comment_parse, headers=getHeader())
 '''
            # issues数据采集
            #issues_url = repos_data["issues_url"][0:-9] + "?state=all&per_page=100"
            #yield scrapy.Request(issues_url, meta={"oss_id": oss_id}, callback=self.detail_issues_parse, headers=getHeader())

            # label数据采集
            #label_url = repos_data["labels_url"][0:-7] + "?per_page=100"
            #yield scrapy.Request(label_url, meta={"repo_id": repo_id}, callback=self.label_parse, headers=getHeader())

        except BaseException as e:
            print(e)
        if self.repo_index < len(self.scrapyed_list):
            self.repo_index += 1
            next_url = self.scrapyed_list[self.repo_index]
            yield scrapy.Request(next_url, callback=self.parse, headers=getHeader())



    def detail_issues_parse(self, response):
        repos_data = json.loads(response.body.decode('utf-8'))
        repos_header = response.headers
        oss_id = response.meta['oss_id']
        for repos_per_data in repos_data:
            if ("pull_request" in repos_per_data):
                continue
            Issues_Info_item = OsslibIssue()
            Issues_Info_item['oss_id'] = oss_id
            Issues_Info_item['issue_id'] = repos_per_data['id']
            Issues_Info_item['issue_number'] = repos_per_data['number']
            if (repos_per_data['state'] == 'open'):
                Issues_Info_item['issue_state'] = 0
            elif (repos_per_data['state'] == 'closed'):
                Issues_Info_item['issue_state'] = 1
            else:
                Issues_Info_item['issue_state'] = 2

            Issues_Info_item['issue_create_time'] = repos_per_data['created_at']
            Issues_Info_item['issue_update_time'] = repos_per_data['updated_at']
            if (repos_per_data['closed_at'] is None):
                Issues_Info_item['issue_close_time'] = ""
            else:
                Issues_Info_item['issue_close_time'] = repos_per_data['closed_at']
            Issues_Info_item['issue_comment_count'] = repos_per_data['comments']
            Issues_Info_item['issue_user_type'] = repos_per_data['author_association']
            Issues_Info_item['user_id'] = repos_per_data['user']['id']
            '''
            user_exit = self.is_user_exit(repos_per_data['user']['id'])
            if (user_exit == 1):
                pass
            else:
                owner_url = repos_per_data['user']['url']
                yield scrapy.Request(owner_url, callback=self.detail_owner_parse, headers=getHeader())
            '''
            Issues_Info_item['update_time'] = time.strftime('%Y-%m-%d', time.localtime(time.time()))
            if (repos_per_data['body'] is None):
                Issues_Info_item['issue_body'] = ""
            else:
                Issues_Info_item['issue_body'] = str(repos_per_data['body'].replace('\n', ''))
            if (repos_per_data['title'] is None):
                Issues_Info_item['issue_title'] = ""
            else:
                Issues_Info_item['issue_title'] = str(repos_per_data['title'].replace('\n', ''))
            yield Issues_Info_item
        listLink_next_url = re.findall(r'(?<=<).[^<]*(?=>; rel=\"next)', str(repos_header))
        if (len(listLink_next_url) > 0):
            yield scrapy.Request(listLink_next_url[0], meta={"oss_id": oss_id},
                                 callback=self.detail_issues_parse,
                                 headers=getHeader())

    def detail_pulls_parse(self, response):
        repos_data = json.loads(response.body.decode('utf-8'))
        repos_header = response.headers
        oss_id = response.meta['oss_id']
        pull_url = response.meta['pull_url']
        for repos_per_data in repos_data:
            Pulls_Info_item = OsslibPulls()
            Pulls_Info_item['oss_id'] = oss_id
            Pulls_Info_item['pull_id'] = repos_per_data['id']
            pull_id = repos_per_data['id']
            Pulls_Info_item['pull_number'] = repos_per_data['number']
            pull_no =  repos_per_data['number']
            if (repos_per_data['state'] == 'open'):
                Pulls_Info_item['pull_state'] = 0
            elif (repos_per_data['state'] == 'closed'):
                Pulls_Info_item['pull_state'] = 1
            else:
                Pulls_Info_item['pull_state'] = 2
            Pulls_Info_item['pull_created_at'] = repos_per_data['created_at']
            Pulls_Info_item['pull_update_at'] = repos_per_data['updated_at']
            Pulls_Info_item['pull_closed_at'] = repos_per_data['closed_at']
            Pulls_Info_item['pull_merged_at'] = repos_per_data['merged_at']
            if (repos_per_data['merged_at'] == None):
                Pulls_Info_item['pull_is_merged'] = 0
            else:
                Pulls_Info_item['pull_is_merged'] = 1
            Pulls_Info_item['pull_author_association'] = repos_per_data['author_association']
            Pulls_Info_item['user_id'] = repos_per_data['user']['id']
            Pulls_Info_item['pull_body'] = repos_per_data['body']
            Pulls_Info_item['pull_title'] = repos_per_data['title']

            Pulls_Info_item['update_time'] = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
            Pulls_Info_item['request_reviewer'] = ''
            pull_request_reviewer = []
            if len(repos_per_data['requested_reviewers']) > 0:
                for reviewer in repos_per_data['requested_reviewers']:
                    reviewer_url = reviewer['url']
                    '''
                    user_exit = self.is_user_exit(reviewer['id'])
                    if (user_exit == 1):
                        pass
                    else:
                        yield scrapy.Request(reviewer_url, callback=self.detail_owner_parse, headers=getHeader())
                    '''
                    Pulls_Info_item['request_reviewer'] += str(reviewer['id'])+"|"
                    pull_request_reviewer.append(reviewer['id'])
            #查询review
            pull_review_url = pull_url + "/"+str(pull_no)+"/reviews"
            pull_review_html = requests.get(pull_review_url, headers=getHeader()).text
            pull_review_html_info = json.loads(pull_review_html)
            if (pull_review_html_info and len(pull_review_html_info) > 0 and "message" not in pull_review_html_info):
                Pulls_Info_item['pull_is_reviewed'] = 1
                for reviewer in pull_review_html_info:
                    review_item = OsslibPullsReviewers()
                    review_item['review_id'] = reviewer['id']
                    review_item['oss_id'] = oss_id
                    review_item['pull_id'] = repos_per_data['id']
                    review_item['reviewer_id'] = reviewer['user']['id']
                    if(review_item['reviewer_id'] in Pulls_Info_item):
                        review_item['is_requested'] = 1
                    else:
                        review_item['is_requested'] = 0
                    '''
                    user_exit = self.is_user_exit(reviewer['user']['id'])
                    if (user_exit == 1):
                        pass
                    else:
                        reviewer_url = reviewer['user']['url']
                        yield scrapy.Request(reviewer_url, callback=self.detail_owner_parse, headers=getHeader())
                    '''
                    review_item['author_association'] = reviewer['author_association']
                    try:
                        review_item['reviewed_time'] = reviewer['submitted_at']
                    except BaseException as e:
                        review_item['reviewed_time'] = ''
                    review_item['update_time'] = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
                    yield review_item
            else:
                Pulls_Info_item['pull_is_reviewed'] = 0
            #查询pull详细
            pull_detail_url = pull_url + "/" + str(pull_no)
            pull_detail_html = requests.get(pull_detail_url, headers=getHeader()).text
            pull_datail_html_info = json.loads(pull_detail_html)
            if (pull_datail_html_info and len(pull_datail_html_info) > 0 and "message" not in pull_datail_html_info):
                Pulls_Info_item['pull_comments'] = pull_datail_html_info['comments']
                Pulls_Info_item['review_comments'] = pull_datail_html_info['review_comments']
            yield Pulls_Info_item
        listLink_next_url = re.findall(r'(?<=<).[^<]*(?=>; rel=\"next)', str(repos_header))
        if (len(listLink_next_url) > 0):
            yield scrapy.Request(listLink_next_url[0], meta={"oss_id": oss_id,"pull_url":pull_url},
                                 callback=self.detail_pulls_parse,
                                 headers=getHeader())

    def detail_contributors_parse(self, response):
        repos_data = json.loads(response.body.decode('utf-8'))
        repos_header = response.headers
        oss_id = response.meta['oss_id']
        for repos_per_data in repos_data:
            #判断是匿名用户还是真实用户
            if ('id' in repos_per_data):
                owner_url = repos_per_data['url']
                user_exit = self.is_user_exit(repos_per_data['id'])
                if(user_exit==1):
                    pass
                else:
                    yield scrapy.Request(owner_url, callback=self.detail_owner_parse, headers=getHeader())
                OsslibDeveloper_item = OsslibDeveloper()
                OsslibDeveloper_item['oss_id'] = oss_id
                OsslibDeveloper_item['user_id'] = repos_per_data['id']
                OsslibDeveloper_item['user_commit_count'] = repos_per_data['contributions']
                OsslibDeveloper_item['update_time'] = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
                yield OsslibDeveloper_item
            else:
                pass

        listLink_next_url = re.findall(r'(?<=<).[^<]*(?=>; rel=\"next)', str(repos_header))
        if (len(listLink_next_url) > 0):
            yield scrapy.Request(listLink_next_url[0], meta={"oss_id": oss_id}, callback=self.detail_contributors_parse,
                                 headers=getHeader())

    def detail_owner_parse(self, response):
        repos_data = json.loads(response.body.decode('utf-8'))
        User_Info_item = OsslibUser()
        User_Info_item['user_id'] = repos_data['id']
        User_Info_item['user_name'] = repos_data['login']
        User_Info_item['user_fullname'] = repos_data['name'] if repos_data['name']!=None else ''
        User_Info_item['avatar_url'] = repos_data['avatar_url']
        try:
            User_Info_item['follows_count'] = repos_data['followers']
        except BaseException as e:
            User_Info_item['follows_count'] = 0
        User_Info_item['repos_count'] = repos_data['public_repos']
        User_Info_item['blog_url'] = str(repos_data['blog'])
        User_Info_item['location'] = str(repos_data['location'])
        User_Info_item['email_url'] = str(repos_data['email'])
        User_Info_item['company'] = str(repos_data['company'])
        User_Info_item['org_member_count'] = 0
        User_Info_item['user_type'] = repos_data['type']
        User_Info_item['user_create_time'] = repos_data['created_at']
        User_Info_item['update_time'] = time.strftime('%Y-%m-%d', time.localtime(time.time()))
        User_Info_item['user_update_time'] = repos_data['updated_at']
        yield User_Info_item

    def is_user_exit(self, uid):
        dbObject = dbHandle()
        cursor = dbObject.cursor()
        cursor.execute("select * from osslib_user where user_id=%s", (uid))
        result = cursor.fetchone()
        if result:
            return 1
        else:
            return 0

