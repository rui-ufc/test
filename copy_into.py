import re
import tempfile
import sqlite3
import sqlalchemy
import sys
import os

stage_names = sys.argv[2]

#引数のファイル名の存在チェック
if not os.path.exists(input_sql):
    sys.exit()
    #エラーメッセージも

#ファイル開いてcreate文を取得する 
with open(input_sql,encoding="utf-8") as f:
     str = f.read()

#create文を分割して、リストに格納する
sql_lists = re.findall("create table .*? [\s\S]*?;",str)

#.dbを作成

with tempfile.TemporaryDirectory() as td:
    try:
        dbname = f'{td}/cpy_test.db'
        db = sqlite3.connect(
            dbname,
            isolation_level = None
            )

        for sql_text in sql_lists:
            db.execute(sql_text)
        db.close()

    #.dbからテーブル情報を持ってくる
    # sqlite3に接続
        engine = sqlalchemy.create_engine(f"sqlite:///{dbname}")
        
        # メタデータを取得
        metadata = sqlalchemy.MetaData()
        metadata.reflect(bind=engine)

        # メタデータに含まれるすべてのテーブルを取得
        table_names = metadata.tables

    except Exception:
        pass

copy_texts = []
# テーブル名とカラム名を取得し、copy文の作成
for table_name in table_names:
    column_info = '('
    for column in table_names[table_name].columns:
        column_info = f"{column_info}{column.name},"
    column_info = column_info[:-1] + ")"

    #copy文の作成
    #ステージ名も引数にする
    copy_texts.append(f'''COPY INTO "{table_name}" {column_info} FROM "{stage_name}" file_format = csv files = '/libraryname_{table_name}.csv.gz';''')

#ファイルに出力(joinでもできる)
with open('copy_into.txt','w') as f:
    f.write('\n'.join(copy_texts))


