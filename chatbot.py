from telegram import ParseMode, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Updater, CallbackQueryHandler, PicklePersistence
from collections import deque
from telegram.ext import CommandHandler
from telegram.ext import MessageHandler, Filters
import os
import pandas as pd

from utils import create_logger

logger = create_logger(__name__, 'logs/chatbot.log')
logger.info('bot started')

replies = {'info': "Hello I'm test job bot!\n /info current message"
                   "\n /look_vacancies show last 1 day vacancies\n/stats get vacancies stats",
           'no_vacancies': 'No more vacancies found'
           }

my_persistence = PicklePersistence(filename='persistence.pickle')

updater = Updater(token=os.environ['TOKEN'], use_context=True, persistence=my_persistence)
dispatcher = updater.dispatcher

keyboard = [
    [InlineKeyboardButton("Next", callback_data='Next'),
     InlineKeyboardButton("Send", callback_data='Send')],
]
reply_markup = InlineKeyboardMarkup(keyboard)

ifile_path = 'data/channels_messages.csv'
target_channel_id = '@job_bot_channel'


def format_vacancy(row):
    return f"from @{row['channel']}\n{row['date']:%Y-%m-%d %H:%M}\n{row['message']}\n\n"


def is_valid_vacancy(row):
    forbidden_words = ['КУРС', 'КУРСЫ', '#реклама']
    return all(x not in row['message'] for x in forbidden_words)


def start(update, context):
    logger.info('start user: @%s', update.effective_chat.username)
    context.bot.send_message(chat_id=update.effective_chat.id, text=replies['info'])


def look_vacancies(update, context):
    df = pd.read_csv(ifile_path)
    df['date'] = pd.to_datetime(df['date'])
    vacancies_msg_list = deque()
    for i, row in df.iterrows():
        if is_valid_vacancy(row):
            msg = format_vacancy(row)
            vacancies_msg_list.append(msg)

    if vacancies_msg_list:
        cur_msg = vacancies_msg_list.popleft()
        context.user_data['vacancies_msg_list'] = vacancies_msg_list
    else:
        cur_msg = replies['no_vacancies']

    context.bot.send_message(chat_id=update.effective_chat.id,
                             text=cur_msg,
                             reply_markup=reply_markup,
                             )


def echo(update, context):
    context.bot.send_message(chat_id='@job_bot_channel', text=update.message.text)


def next_button(update, context):
    if context.user_data.get('vacancies_msg_list'):
        cur_msg = context.user_data['vacancies_msg_list'].popleft()
    else:
        cur_msg = replies['no_vacancies']
    logger.info('reply msg user: @%s', update.effective_chat.username)
    context.bot.send_message(chat_id=update.effective_chat.id,
                             text=cur_msg,
                             reply_markup=reply_markup,
                             )


def send_button(update, context):
    query = update.callback_query
    query.answer()
    msg = query.message.text
    context.bot.send_message(chat_id=target_channel_id, text=msg)


def stats(update, context):
    df = pd.read_csv(ifile_path)
    stats_df = '\n'.join([f'{i}\t{v}' for i, v in df.groupby('channel')['date'].count().items()])
    msg = f'Total vacancies: {df.shape[0]}\nTotal channels: {len(df["channel"].unique())}\n {stats_df}'
    logger.info('stats user: @%s', update.effective_chat.username)
    context.bot.send_message(chat_id=update.effective_chat.id,
                             text=msg,
                             )


def main():
    echo_handler = MessageHandler(Filters.text & (~Filters.command), echo)
    start_handler = CommandHandler('start', start)
    info_handler = CommandHandler('info', start)
    stats_handler = CommandHandler('stats', stats)
    look_vacancies_handler = CommandHandler('look_vacancies', look_vacancies)

    dispatcher.add_handler(start_handler)
    dispatcher.add_handler(info_handler)
    dispatcher.add_handler(echo_handler)
    dispatcher.add_handler(look_vacancies_handler)
    dispatcher.add_handler(stats_handler)

    dispatcher.add_handler(CallbackQueryHandler(next_button, pattern="Next"))
    dispatcher.add_handler(CallbackQueryHandler(send_button, pattern="Send"))

    updater.start_polling()


if __name__ == '__main__':
    main()
