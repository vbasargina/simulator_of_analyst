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

my_token = 'my_token'
chat_id = -1111
bot = telegram.Bot(token=my_token)

# Функция приведения даты в строковый формат
def d_m_y(df, index=0):
    day = '.'.join(datetime.strptime(str(df.at[index,'date']), "%Y-%m-%d %H:%M:%S").strftime("%d-%m-%Y %H:%M:%S").split( )[0].split('-'))
    return day

# ФУнкция для отправки отчета: текстовое сообщение и график
def report_users():
    # Данные из таблицы feed_actions
    q1 = """
        SELECT
          toDate(time) as date,
          user_id
        FROM
          simulator_20231220.feed_actions
        WHERE
          toDate(time) between today()-8 and today()-1
        GROUP BY
          date, user_id
        ORDER BY date
        """
    df_1 = ph.read_clickhouse(q1, connection=connection)
    
    # Данные из таблицы message_actions
    q2 = """
        SELECT
          toDate(time) as date,
          user_id
        FROM
          simulator_20231220.message_actions
        WHERE
         toDate(time) between today()-8 and today()-1
        GROUP BY
          date, user_id
        ORDER BY date
        """
    df_2 = ph.read_clickhouse(q2, connection=connection)
    
    # все пользователи приложения
    df_all = df_1.merge(df_2, how='outer', on =['user_id', 'date'])

    # пользуются и лентой, и мессенджером
    feed_and_message = df_1.merge(df_2, how='inner', on =['user_id', 'date'])

    # пользуются только новостной лентой
    left_feed = df_1.merge(df_2, how='left', on =['user_id', 'date'], indicator=True)
    only_feed = left_feed[left_feed['_merge'] == 'left_only']

    # пользуются только мессенджером
    left_message = df_2.merge(df_1, how='left', on =['user_id', 'date'], indicator=True)
    only_message = left_message[left_message['_merge'] == 'left_only']
    
    df_all_agg = df_all.groupby('date', as_index = False). \
                    agg({'user_id':'count'}). \
                    rename(columns={'user_id':'all_users'})

    # Группировка таблиц по дате
    feed_and_message_agg = feed_and_message.groupby('date', as_index = False). \
                                            agg({'user_id':'count'}). \
                                            rename(columns={'user_id':'message_and_feed'})

    only_feed_agg = only_feed.groupby('date', as_index = False). \
                              agg({'user_id':'count'}). \
                              rename(columns={'user_id':'only_feed'})

    only_message_agg = only_message.groupby('date', as_index = False). \
                                    agg({'user_id':'count'}). \
                                    rename(columns={'user_id':'only_message'})
    # Join таблиц
    df_users=df_all_agg.merge(feed_and_message_agg, on='date')
    df_users = df_users.merge(only_feed_agg, on='date')
    df_users = df_users.merge(only_message_agg, on='date')
    
    # Расчет количества пользователей в % в зависимости от их вида взаимодействия с приложением
    df_users['message_and_feed_per'] = round(100*df_users['message_and_feed']/df_users['all_users'], 1)
    df_users['only_feed_per'] = round(100*df_users['only_feed']/df_users['all_users'], 1)
    df_users['only_message_per'] = round(100*df_users['only_message']/df_users['all_users'], 1)
    
    # Отношение количества уникальных пользователей вчера и неделю назад в тот же день недели в %
    per_users = round((df_users.loc[7, 'all_users']/df_users.loc[0,'all_users'])*100-100, 1)
    if per_users < 0:
        per_users = per_users*(-1)
        compare = 'меньше'
    else:
        compare = 'больше'
    
    # Текстовый отчет за прошлый день об активностях в приложении
    msg_1 = 'Отчет по приложению за ' + d_m_y(df_users, 7)
    msg_2 = 'Количество уникальных пользователей приложения ' + d_m_y(df_users, 7) + ': \n' + \
              str(df_users.loc[7, 'all_users']) + ' человек \n(на ' + \
              str(per_users) + '% ' + compare + ' чем неделю назад) \n\n' + \
              'Из этих пользователей \n' + \
              'пользовались лентой и мессенджером одновременно: ' + '\n' + str(df_users.loc[7, 'message_and_feed_per']) + '% \n' + \
              'пользовались только лентой новостей: ' + '\n' + str(df_users.loc[7, 'only_feed_per']) + '% \n' + \
              'пользовались только мессенджером: ' +'\n' + str(df_users.loc[7, 'only_message_per']) + '%'
    bot.sendMessage(chat_id=chat_id, text=msg_1)
    bot.sendMessage(chat_id=chat_id, text=msg_2)
    
    # Графический отчет: динамика взаимодействия пользователей с приложением 
    
    
    sns.set(rc={'figure.figsize':(15,9)})

    sns.lineplot(y='all_users', x='date', data=df_users, label = 'Все уникальные пользователи приложения')
    sns.lineplot(y='only_message', x='date', data=df_users, label = 'Пользуются только мессенджером')
    sns.lineplot(y='only_feed', x='date', data=df_users, label = 'Пользуются только лентой новостей')
    sns.lineplot(y='message_and_feed', x='date', data=df_users, label = 'Пользуются лентой новостей и месседжером одновременно')

    plt.legend()
    plt.ylabel("Количество пользователей,чел.")
    plt.title('Динамика взаимодействия пользователей с приложением c ' +  d_m_y(df_users, 0) + ' по ' + d_m_y(df_users, 7))
    
    plot_object = io.BytesIO()
    plt.savefig(plot_object)
    plot_object.seek(0)
    plot_object.name = 'plot_users.png'
    plt.close()
    bot.sendPhoto(chat_id=chat_id, photo=plot_object)
    
# Функция для вычисления отношения метрик в разные дни % и 
def percent(df, num, name_col):
    a = round((df.loc[7, name_col]/df.loc[num, name_col])*100-100, 1)
    if a > 0:
        a = '+' + str(abs(a))
    elif a < 0:
        a = '-' + str(abs(a))
    else:
        a = str(abs(a))
    return a

# Функция для отчета по основным метрикам: лайки, просмотры и т.д.
def report_metrics():
    q3 = """
        SELECT
          toDate(time) as date,
          sum(action = 'view') as views,
          sum(action = 'like') as likes,
          likes/views as ctr
        FROM
          simulator_20231220.feed_actions
        WHERE
          toDate(time) between today()-8 and today()-1
        GROUP BY
          date
        ORDER BY
          date
        """
    df_f = ph.read_clickhouse(q3, connection=connection)
    
    q4 = """
        SELECT
          toDate(time) as date,
          count(user_id) as messages
        FROM
          simulator_20231220.message_actions
        WHERE
          toDate(time) between today()-8 and today()-1
        GROUP BY
          date
        ORDER BY
          date
        """
    df_m = ph.read_clickhouse(q4, connection=connection)
    
    df_new=df_f.merge(df_m)
    
    # Расчет процентного отношения метрик к показателям за день до и за неделю до
    views_day, views_week = percent(df_new, 6, 'views') +'%', percent(df_new, 0, 'views') +'%'
    likes_day, likes_week = percent(df_new, 6, 'likes') +'%', percent(df_new, 0, 'likes') +'%'
    ctr_day, ctr_week = percent(df_new, 6, 'ctr') +'%', percent(df_new, 0, 'ctr') +'%'
    messages_day, messages_week = percent(df_new, 6, 'messages') +'%', percent(df_new, 0, 'messages') +'%'

    # Создание датафрейма со всеми метриками
    data = {'Метрики': ['Просмотры', 'Лайки', 'CTR', 'Отправленные сообщения'],
            'Значения за ' + d_m_y(df_new, 7): [str(df_new.loc[7, 'views']), str(df_new.loc[7, 'likes']), 
                         str(round(df_new.loc[7, 'ctr'],4)), str(df_new.loc[7, 'messages'])],
            'По сравнению с ' + d_m_y(df_new, 6): [views_day, likes_day, ctr_day, messages_day],
            'По сравнению с ' + d_m_y(df_new, 0): [views_week, likes_week, ctr_week, messages_week]}
    df = pd.DataFrame(data)
    
    # Сообщение
    msg_3 = 'Основные метрики приложения за ' + d_m_y(df_new, 7)
    bot.sendMessage(chat_id=chat_id, text=msg_3)
    
    # Создание графика таблицы и отправка в бот
    # Создание фигуры и оси
    fig, ax = plt.subplots(figsize=(16, 5))

    # Установка осей
    ax.axis('off')

    # Получение размеров фигуры
    fig_width, fig_height = fig.get_size_inches()

    # Создание таблицы
    table = ax.table(cellText=df.values, colLabels=df.columns, cellLoc='center', loc='center', colColours=['#f2f2f2']*df.shape[1])

    # Установка размеров шрифта
    table.auto_set_font_size(False)
    table.set_fontsize(14)

    # Установка параметров таблицы для занимания всей площади графика
    table.auto_set_column_width([i for i in range(len(df.columns)+1)])  # +1 для заголовка
    table.scale(fig_width, fig_height)

    plot_object = io.BytesIO()
    plt.savefig(plot_object)
    plot_object.seek(0)
    plot_object.name = 'report_metrics_of_app.png'
    plt.close()
    bot.sendPhoto(chat_id=chat_id, photo=plot_object)

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
    'start_date': datetime(2024, 1, 14),
}

# Интервал запуска DAG
schedule_interval = '0 11 * * *'

@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def vb_app_report():
    
    @task
    def report():
        report_users()
        report_metrics()
        
    report()
        
vb_app_report = vb_app_report()