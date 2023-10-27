import pandas as pd
from datetime import datetime, timedelta
import pytz
import requests
from loguru import logger
from retrying import retry


@retry
def safe_request(url, method, session=None, **kwargs):
    try:
        if session is None:
            session = requests.session()
        response = session.request(method, url, **kwargs)
        # if response.status_code not in [200, 400, 201, 302]:
        response.raise_for_status()
        return response
    except requests.RequestException as e:
        logger.error(f'Request to {url} failed: {str(e)}')
        raise


class BusinessPost:
    def __init__(self, app_id, app_secret, instagram, access_token):
        self.app_id = app_id
        self.app_secret = app_secret
        self.instagram = instagram
        self.access_token = access_token
        self.page_access_token = ""
        self.page_id = 'me'
        self.refresh_token()
        self.fetch_page_access_token()

    def refresh_token(self):
        url = f"https://graph.facebook.com/v18.0/oauth/access_token"
        params = {
            'grant_type': 'fb_exchange_token',
            'client_id': self.app_id,
            'client_secret': self.app_secret,
            'fb_exchange_token': self.access_token
        }
        res = safe_request(url, 'GET', params=params)
        self.access_token = res.json().get('access_token')

    def fetch_page_access_token(self):
        url = f'https://graph.facebook.com/v18.0/{self.page_id}/accounts?access_token={self.access_token}'
        res = safe_request(url, 'GET')
        self.page_access_token = res.json()['data'][0]['access_token']
        logger.info('Page access token was fetched successfully.')

    def upload_image(self, photo_path):
        if not self.page_access_token:
            raise Exception("Please fetch page access token first.")
        url = f'https://graph.facebook.com/v18.0/{self.page_id}/photos?access_token={self.page_access_token}'
        files = {'file': open(photo_path, 'rb')}
        data = {'published': False}
        response = safe_request(url, 'POST', files=files, data=data)
        photo_id = response.json()['id']
        return photo_id

    def post_facebook_page(self, message, scheduled_publish_time, photo_path, photo_id_len=5):
        if not self.page_access_token:
            raise Exception("Please fetch page access token first.")
        photo_file_list = []
        logger.info("Start to upload images.")
        for filename in os.listdir(photo_path):
            if filename.endswith(".jpg"):  # 确保只上传JPEG文件
                image_path = os.path.join(photo_path, filename)
                photo_file_list.append(image_path)
        logger.info(f"Total {len(photo_file_list)} images were found.")
        photo_id_list = []
        for photo_file in photo_file_list[:photo_id_len]:
            photo_id_list.append({'media_fbid': self.upload_image(photo_file)})

        logger.info(f"Total {len(photo_id_list)} images were uploaded.")

        url = f'https://graph.facebook.com/v18.0/me/feed?access_token={self.page_access_token}'
        data = {
            'message': message,
            'attached_media': photo_id_list,
            'scheduled_publish_time': scheduled_publish_time,
            'published': False
        }
        response = safe_request(url, 'POST', json=data)
        logger.info('Post was scheduled successfully.')
        return response.json()

    def post_instagram_page(self, message, photo_urls, photo_id_len=3):
        media_ids = []
        for photo_url in photo_urls:
            url = f'https://graph.facebook.com/v18.0/{self.instagram}/media'
            data = {
                'image_url': photo_url,
                'access_token': access_token
            }
            response = safe_request(url, 'POST', json=data)
            media_id = response.json()['id']
            media_ids.append(media_id)
        # 创建 Carousel 的媒体容器
        url = f'https://graph.facebook.com/v18.0/{self.instagram}/media'
        data = {
            'media_type': 'CAROUSEL',
            'children': ','.join(media_ids[:photo_id_len]),
            'caption': message,
            'access_token': access_token
        }
        response = safe_request(url, 'POST', json=data)
        carousel_id = response.json()['id']

        # 发布 Carousel
        url = f'https://graph.facebook.com/v18.0/{self.instagram}/media_publish'
        data = {'creation_id': carousel_id, 'access_token': access_token}
        response = safe_request(url, 'POST', json=data)
        if response.status_code == 200:
            logger.info("Instagram published successfully.")
        else:
            logger.info("Instagram published Failed.")


def schedule_post(app_id, app_secret, instagram, access_token):
    business_post = BusinessPost(app_id, app_secret, instagram, access_token)
    # 读取 Excel 文件
    df = pd.read_excel('schedule.xlsx')
    # 获取当前的时间
    hk_tz = pytz.timezone('Asia/Hong_Kong')
    now = datetime.now(hk_tz)
    # 计算 5 分钟前的时间
    past = now - timedelta(minutes=20)
    # 计算 5 分钟后的时间
    future = now + timedelta(minutes=20)
    flag = 0
    # 遍历 Excel 文件中的所有行
    for index, row in df.iterrows():
        # 获取行中的时间
        task_time = row['規劃時間']
        # 如果任务的时间在过去的 20 分钟和未来的 20 分钟之间
        task_time = hk_tz.localize(task_time)
        if past <= task_time <= future:
            # 执行任务
            business_post.post_instagram_page(row['內容'], row['圖片列表'].split(','))
            flag = 1
    if flag == 0:
        logger.info("No post was scheduled.")


if __name__ == '__main__':
    import os

    app_id = os.getenv('APP_ID')
    app_secret = os.getenv('APP_SECRET')
    instagram = os.getenv('INSTAGRAM')
    access_token = os.getenv('ACCESS_TOKEN')
    schedule_post(app_id, app_secret, instagram, access_token)
