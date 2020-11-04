import os
import pandas as pd

from telethon import TelegramClient
from telethon.errors import SessionPasswordNeededError
from telethon.tl.functions.messages import GetHistoryRequest
from tqdm import tqdm
from config import username, api_id, api_hash, phone

os.makedirs('data', exist_ok=True)


async def get_all_messages(client, my_channel, oldest_date):
    offset_id = 0
    limit = 100
    total_count_limit = 20
    all_messages = []

    while True:
        history = await client(GetHistoryRequest(
            peer=my_channel,
            offset_id=offset_id,
            offset_date=None,
            add_offset=0,
            limit=limit,
            max_id=0,
            min_id=0,
            hash=0
        ))
        if not history.messages:
            break
        messages = history.messages
        for message in messages:
            if message.date < oldest_date or len(all_messages) >= total_count_limit:
                return all_messages

            msg = message.to_dict()
            msg['channel'] = my_channel
            msg['msg_id'] = f'{msg["channel"]}_{msg["date"]:%Y%m%d_}'
            if 'message' in msg and msg['message']:
                all_messages.append(msg)

        offset_id = messages[-1].id
        return all_messages


def main():
    client = TelegramClient(username, api_id, api_hash)
    now = pd.Timestamp.now()
    oldest_date = pd.Timestamp(str(now).split(' ')[0], tz='utc') \
                  - pd.Timedelta('1d')

    target_channels = ['devjobs',
                       # 'devops_jobs',
                       'fordev',
                       'fordevops',
                       'gamedevjob',
                       'getitrussia',
                       'it_hunters',
                       # 'javascript_jobs',
                       'jobGeeks',
                       'jobskolkovo',
                       'logic',
                       'mobile_jobs',
                       'myjobit',
                       'qa_jobs',
                       'remoteit',
                       'remowork_ru',
                       'ru_pythonjobs',
                       'tproger_official',
                       'webfrl',
                       'distantsiya']
    ofile_path = f'data/channels_messages.csv'

    print('now:', now)
    print('oldes date:', oldest_date)
    print('ofile_path:', ofile_path)

    async def main_crawler(phone):
        await client.start()
        if await client.is_user_authorized() == False:
            await client.send_code_request(phone)
            try:
                await client.sign_in(phone, input('Enter the code: '))
            except SessionPasswordNeededError:
                await client.sign_in(password=input('Password: '))

        all_messages = []
        for my_channel in tqdm(target_channels):
            all_messages.extend(await get_all_messages(client, my_channel, oldest_date))

        df = pd.DataFrame(all_messages)
        print('found rows:', df.shape[0])
        print('found shape:', df.shape)
        print('last date:', all_messages[-1]['date'])
        need_cols = ['date', 'channel', 'message', 'id', 'views', 'forwards']
        df = df[need_cols]
        df.to_csv(ofile_path, index=False, encoding='utf-8')

    with client:
        client.loop.run_until_complete(
            main_crawler(phone)
        )


if __name__ == '__main__':
    main()
