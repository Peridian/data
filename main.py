print('----- Data execution initialized -----')

from facebookads import adobjects
from facebookads.api import FacebookSession
from facebookads.api import FacebookAdsApi
from facebookads.adobjects.adaccountuser import AdAccountUser
from facebookads.adobjects.adaccount import AdAccount
from facebookads.adobjects.user import User
from facebookads.adobjects.campaign import Campaign

import json
import pandas as pd
import json
import os


def facebook_get_service(config):
    '''
    Connects to Facebook Ads platform
    '''
    session = FacebookSession(
        config['app_id'],
        config['app_secret'],
        config['access_token']
    )
    api = FacebookAdsApi(session)
    FacebookAdsApi.set_default_api(api)
    FacebookAdsApi.init(
        config['app_id'],
        config['app_secret'],
        config['access_token']
    )


def facebook_data_extraction(config):
    '''
    Facebook Ads platform data extraction
    '''
    facebook_get_service(config)
    me = User(fbid='me')
    print('me', me)
    print(
        'me.remote_read(fields=[User.Field.permissions])',
        me.remote_read()
    )
    ad_account = AdAccount(config['ad_account_id'])
    print('ad_account', ad_account.remote_read())
    print('ad_account', ad_account.get_campaigns())
    return ''


def youtube_data_extraction():
    return ''


def instagram_data_extraction():
    return ''


def analytics_data_extraction():
    return ''


def extract(config):
    '''
        Returns tupple of data of sources, respectively:
        - YT
        - GA
        - FB
        - Inst
    '''
    fb_data = facebook_data_extraction(config['facebook'])
    yt_data = youtube_data_extraction()
    ga_data = youtube_data_extraction()
    inst_data = youtube_data_extraction()
    return yt_data, ga_data, fb_data, inst_data


def youtube_data_transform(yt_data):
    '''
        Returns formatted df of YT data
    '''
    result = yt_data
    return result


def facebook_data_transform(fb_data):
    '''
        Returns formatted df of FB data
    '''
    result = fb_data
    return result


def analytics_data_transform(ga_data):
    '''
        Returns formatted df of FB data
    '''
    result = ga_data
    return result


def instagram_data_transform(inst_data):
    '''
        Returns formatted df of Inst data
    '''
    result = inst_data
    return result


def transform(data):
    '''
        Transforms and formats data into Pandas dataFrame

        Returns tuple of sources: 
        - YT
        - GA
        - FB
        - Inst
    '''
    yt_formatted_data = youtube_data_transform(data[0])
    ga_formatted_data = analytics_data_transform(data[1])
    fb_formatted_data = facebook_data_transform(data[2])
    inst_formatted_data = instagram_data_transform(data[3])
    return (
        yt_formatted_data,
        ga_formatted_data,
        fb_formatted_data,
        inst_formatted_data
    )


def load(data):
    '''
    General data loading function
    '''
    return ''


def main():
    '''
    General marketing data ETL script for New Content\'s work for Electro Lux
    '''
    this_dir = os.path.dirname(__file__)
    config_filename = os.path.join(this_dir, 'config.json')
    config_file = open(config_filename)
    config = json.load(config_file)
    config_file.close()
    
    load(
        transform(
            extract(config)
        )
    )
    print('----- Data execution finished successfully -----')


if __name__ == '__main__':
    main()
