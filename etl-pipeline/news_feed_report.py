import pandas as pd
import numpy as np
import pandahouse as ph
from datetime import datetime, timedelta

import seaborn as sns
import matplotlib.pyplot as plt

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

import telegram
import io

my_token = '6987125723:AAHkmaR-s2aEC0jEiIXuIoi_MU-vWMbNDYM'
chat_id = -938659451 
bot = telegram.Bot(token=my_token)

# Функция приведения даты в строковый формат
def d_m_y(df, index=0):
    day = '.'.join(datetime.strptime(str(df.at[index,'date']), "%Y-%m-%d %H:%M:%S").strftime("%d-%m-%Y %H:%M:%S").split( )[0].split('-'))
    return day

# Функция для обработки изображений
def create_and_send_plot(df, y, title, filename):
    sns.lineplot(y=y, x='date', data=df)
    plt.title(title)
    plot = io.BytesIO()
    plt.savefig(plot)
    plot.seek(0)
    plot.name = filename
    plt.close()
    bot.sendPhoto(chat_id=chat_id, photo=plot)

def report_yesterday():
    q1 = """
            SELECT
              toDate(time) as date,
              count(distinct user_id) as dau,
              sum(action = 'view') as views,
              sum(action = 'like') as likes,
              sum(action = 'like')/sum(action = 'view') as ctr
            FROM
              simulator_20231220.feed_actions
            WHERE
              toDate(time) = today()-1
            GROUP BY
              date
            """
    df = ph.read_clickhouse(q1, connection=connection)
    
    day = d_m_y(df)
    msg = 'Ключевые метрики за ' + day + '\n\n' + \
          'DAU: ' + str(df.at[0,'dau']) + '\n' + \
          'Просмотры: ' + str(df.at[0,'views']) + '\n' + \
          'Лайки: ' + str(df.at[0,'likes']) + '\n' + \
          'CTR: ' + str(round(df.at[0,'ctr'],4)) + '\n'
    
    bot.sendMessage(chat_id=chat_id, text=msg)
    
def report_week_before():
    q2 = """
            SELECT
              toDate(time) as date,
              count(distinct user_id) as dau,
              sum(action = 'view') as views,
              sum(action = 'like') as likes,
              sum(action = 'like')/sum(action = 'view') as ctr
            FROM
              simulator_20231220.feed_actions
            WHERE
              toDate(time) between today()-7 and today()-1
            GROUP BY
              date
            ORDER BY
              date
            """
    df_7 = ph.read_clickhouse(q2, connection=connection)
    
    day_start, day_end = d_m_y(df_7, 0), d_m_y(df_7, 6)
    msg_7 = 'Отчет по ключевым метрикам за период с '+ day_start + ' по ' + day_end
    bot.sendMessage(chat_id=chat_id, text=msg_7)
    
    sns.set(rc={'figure.figsize':(12,7)})

    # Создаем и отправляем графики
    create_and_send_plot(df_7, 'dau', 'DAU за предыдущие 7 дней', 'dau.png')
    create_and_send_plot(df_7, 'views', 'Просмотры за предыдущие 7 дней', 'view.png')
    create_and_send_plot(df_7, 'likes', 'Лайки за предыдущие 7 дней', 'likes.png')
    create_and_send_plot(df_7, 'ctr', 'CTR за предыдущие 7 дней', 'ctr.png')

# ClickHouse
connection = {'host': 'https://clickhouse.lab.karpov.courses',
                'password': 'dpo_python_2020',
                'user': 'student',
                'database': 'simulator_20231220'
}

# Дефолтные параметры, которые прокидываются в таски
default_args = {
    'owner': 'v-basargina-17',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2024, 1, 11),
}

# Интервал запуска DAG
schedule_interval = '0 11 * * *'

@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def vb_feed_report():
    
    @task
    def report():
        report_yesterday()
        report_week_before()
        
    report()
        
vb_feed_report = vb_feed_report()

