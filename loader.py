import bson
import logging
import pymongo
import time
import json
import os
import datetime
import pandas as pd
import jqdatasdk as jq

logging.basicConfig(level=logging.DEBUG,
                    filename='./output3.log',
                    datefmt='%Y/%m/%d %H:%M:%S',
                    format='%(asctime)s - %(name)s - %(levelname)s - %(lineno)d - %(module)s - %(message)s')

logger = logging.getLogger("loader")
SAVE_CSV = False
USER_NAME = "15626046299"
PASSWORD = "046299"


def login(user_name, password):
    try:
        jq.auth(user_name, password)
    except Exception as e:
        logger.info(e)
        return False
    # 判断是否登录成功
    if not jq.is_auth():
        return False
    return True


def jz_get_price(security, start_date: datetime.datetime, end_date: datetime.datetime,
                 frequency='1m'):
    # 调用聚宽的接口
    df = jq.get_price(security, start_date, end_date, frequency)

    if SAVE_CSV:
        dt_format = "%Y-%m-%d-%H-%M-%S"
        file_name = "_".join([security, start_date.strftime(dt_format), end_date.strftime(dt_format)])
        write_df_to_csv(df, file_name)
    return df


def write_df_to_csv(df, file_name):
    # 将结果写入 csv 文件 （因为聚宽每天的条数是有限制的）
    file_name = os.path.join("./csv", file_name)
    df.to_csv(file_name, index=True, sep=',')


def read_df_from_csv(csv_file):
    # index_col = 0 的意思是直接使用第一列作为索引
    df = pd.read_csv(csv_file, index_col=0)
    return df


def get_124_coll():
    return pymongo.MongoClient("127.0.0.1:27017")


def get_local_coll():
    return pymongo.MongoClient("127.0.0.1:27018")


def fetch_un_expire_codes(dt: datetime.datetime):
    # 获取到每日的未过期合约；统一 code 的格式
    cli = get_124_coll()
    futures = cli.test_future.info.find({"expire_date": {"$gte": dt}})
    codes = [future.get("code") for future in futures]
    return codes


def _jq_code_format(code):
    # 转换合约代码为聚宽要求的后缀模式
    CON_EXCHANGE_DICT = {'SH': 'XSHG', 'SZ': 'XSHE', 'IX': 'INDX', 'SF': 'XSGE', 'DF': 'XDCE',
                         'ZF': 'XZCE', 'CF': 'CCFX', 'IF': 'XINE'}

    exchange, id = code[:2], code[2:]
    assert exchange in ("CF", "DF", "SF", "ZF", "IF")
    con_exchange = CON_EXCHANGE_DICT.get(exchange, "")
    if con_exchange:
        return ".".join([id, con_exchange])


def generate_inserts(df: pd.DataFrame):
    # 将 money 列重命名为 amount
    df = df.rename(columns={'money': 'amount'})
    # 将索引转换为其中的一列
    df['time'] = pd.to_datetime(df.index)
    # 将聚宽的时间整体减1min
    df['time'] = df['time'].map(lambda dt: dt - pd.Timedelta(minutes=1))
    # 将 df 转换为字典列表 方便插入
    inserts = list(df.to_dict("index").values())
    return inserts


def mongo_bulk_insert(conn, future: str, datas: list):
    # 批量插入的时候 需要自己生成 _id 否则可能会冲突 TODO
    for data in datas:
        data.update({"_id": bson.ObjectId(), "code": future})
    if datas:
        conn.insert_many(datas)


def main(start: datetime.datetime, end: datetime.datetime):
    t1 = time.time()
    # 登录
    login(USER_NAME, PASSWORD)
    # 待插入的数据库
    # 为数据库设置唯一索引
    # use futu
    # db.prices.ensureIndex({"code": 1, "time": 1}, {unique: true})

    # 导出数据库文件
    # mongodump -h dbhost -d dbname -o dbdirectory
    # mongodump -h 127.0.0.1 --port 27018 -d futu -c prices -o /Users/furuiyang/Desktop/mongo/
    conn = get_local_coll().futu.prices

    for dt in pd.date_range(start, end, freq="1d"):
        dt = dt.to_pydatetime()
        # 当天筛选出的合约
        futures = fetch_un_expire_codes(dt)
        logger.info(f"{dt}, {futures}")

        for info_future in futures:
            future = _jq_code_format(info_future)
            df = jz_get_price(future, dt+datetime.timedelta(minutes=1), dt+datetime.timedelta(days=1))
            datas = generate_inserts(df)
            mongo_bulk_insert(conn, info_future, datas)
            logger.info(f"{future} {dt} 插入成功 ")
    t2 = time.time()
    logger.info(f"耗时是: {(t2 - t1) / 60} min")


if __name__ == "__main__":
    main(datetime.datetime(2019, 11, 1), datetime.datetime(2019, 11, 20))
