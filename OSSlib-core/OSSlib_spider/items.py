# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# https://doc.scrapy.org/en/latest/topics/items.html

import scrapy


class OsslibMetadata(scrapy.Item):

    pass


class OsslibUser(scrapy.Item):
    user_id = scrapy.Field()   #用户id
    user_name = scrapy.Field()                #用户登陆姓名
    user_fullname = scrapy.Field()                #用户姓名全程
    avatar_url = scrapy.Field()               #头像地址
    follows_count = scrapy.Field()              #被关注数
    repos_count = scrapy.Field()                #项目数
    blog_url = scrapy.Field()                 #bolg地址
    email_url = scrapy.Field()                #emall地址
    belong_org = scrapy.Field()               #所属组织
    org_member_count = scrapy.Field()           # 组织会员数
    user_type = scrapy.Field()                  #类别 0 user 1 org 存字符
    user_create_time = scrapy.Field()
    user_update_time = scrapy.Field()
    update_time = scrapy.Field()
    location = scrapy.Field()
    company = scrapy.Field()


class OsslibIssue(scrapy.Item):
    issue_id = scrapy.Field()      #issue_id
    issue_number = scrapy.Field()               #issue_number
    oss_id = scrapy.Field()                 #关联repo_id
    user_id = scrapy.Field()                  #关联组织用户id
    issue_user_type = scrapy.Field()            #issue用户类型 0 member 1 user
    issue_state = scrapy.Field()                #issue 状态 0 open 1 close
    issue_create_time = scrapy.Field()                   #issue创建时间
    issue_update_time = scrapy.Field()
    issue_close_time = scrapy.Field()                       #issue关闭时间
    update_time = scrapy.Field()
    issue_comment_count = scrapy.Field()
    issue_body = scrapy.Field()
    issue_title = scrapy.Field()


#/pulls?state=all
class OsslibPulls(scrapy.Item):
    pull_id = scrapy.Field()  # pull_id
    pull_number = scrapy.Field()  # pull_number
    oss_id = scrapy.Field()  # 关联repo_id
    pull_state = scrapy.Field()  # pullrequest状态 0 open 1close
    user_id = scrapy.Field()  # pull的请求着
    pull_author_association = scrapy.Field()
    pull_created_at = scrapy.Field()
    pull_update_at = scrapy.Field()
    pull_closed_at = scrapy.Field()
    pull_merged_at = scrapy.Field()
    pull_is_merged = scrapy.Field()  # 是否merged 0 false 1 true
    pull_title = scrapy.Field()
    pull_body = scrapy.Field()
    pull_is_reviewed = scrapy.Field()
    pull_comments = scrapy.Field()
    review_comments = scrapy.Field()
    request_reviewer = scrapy.Field()
    update_time = scrapy.Field()


class OsslibPullsReviewers(scrapy.Item):
    review_id = scrapy.Field()
    pull_id = scrapy.Field()
    reviewer_id = scrapy.Field()
    oss_id = scrapy.Field()
    reviewed_time = scrapy.Field()
    is_requested = scrapy.Field()
    update_time = scrapy.Field()
    author_association = scrapy.Field()


class OsslibCommit(scrapy.Item):
    oss_id = scrapy.Field()  # 关联repo_id
    user_id = scrapy.Field()  # commit提交着
    committer_user = scrapy.Field()  # committer
    commit_file_count = scrapy.Field()  # 改动的文件数量
    commit_file_per = scrapy.Field()  # 文件改动具体情况file:add,del
    commit_node_id = scrapy.Field()


class OsslibDeveloper(scrapy.Item):
    oss_id = scrapy.Field()                 #关联repo_id
    user_id = scrapy.Field()                  #关联组织用户id
    user_commit_count = scrapy.Field()                        #用户commit数量
    update_time = scrapy.Field()