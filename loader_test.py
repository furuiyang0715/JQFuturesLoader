import datetime
import pprint
import pymongo

from loader import login, jz_get_price, write_df_to_csv, read_df_from_csv, fetch_un_expire_codes, _jq_code_format, \
    generate_inserts, mongo_bulk_insert

security = "AU1912.XSGE"
start_date = datetime.datetime(2019, 11, 21, 14, 0)
end_date = datetime.datetime(2019, 11, 21, 15, 0)
dt_format = "%Y-%m-%d-%H-%M-%S"


def get_local_coll():
    return pymongo.MongoClient("127.0.0.1:27018")


def __login():
    # 测试登录
    print(login("15626046299", "046299"))


def __jz_get_price():
    # 测试聚源的接口调用层
    __login()
    print(jz_get_price(security, start_date, end_date))


def __write_to_csv():
    # 测试写入 csv 文件
    __login()
    file_name = "_".join([security, start_date.strftime(dt_format), end_date.strftime(dt_format)])
    df = jz_get_price(security, start_date, end_date)
    write_df_to_csv(df, file_name)


def __read_from_csv():
    # 测试从 csv 文件中读取 df 数据
    csv_file = "/Users/furuiyang/Desktop/linshi/JQFuturesLoader/csv/AU1912.XSGE_2019-11-21-14-00-00_2019-11-21-15-00-00"
    df = read_df_from_csv(csv_file)
    return df


def __fetch_un_expire_codes():
    # 测试筛选出每日的期货列表
    codes = fetch_un_expire_codes(start_date)
    return codes


def __test_convert():
    # 测试转换代码格式
    codes = __fetch_un_expire_codes()
    futures = []
    for code in codes:
        futures.append(_jq_code_format(code))
    print(futures)


def __insert():
    # 测试生成数据并且批量插入
    df = __read_from_csv()
    inserts = generate_inserts(df)
    cli = get_local_coll()
    conn = cli.test_futures.prices_test
    ret = mongo_bulk_insert(conn, security, inserts)
    print(ret)


# __login()

# __jz_get_price()

# __write_to_csv()

__insert()


