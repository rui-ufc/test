import pandas as pd
import sys
import string
import mojimoji
import traceback
import pathlib

try:

    # 引数
    ## ファイルパス
    file_path = sys.argv[1]

    # 抽出したい列名
    column_lists = ["COLUMN_NAME", "SYSTEM_TABLE_NAME", "DATA_TYPE","SYSTEM_TABLE_SCHEMA"]

    # csvファイルを読み込む
    df = pd.read_csv(file_path, usecols = column_lists, encoding="shift-jis")

    # ライブラリ名とテーブル名でgroup化
    df = df.groupby(["SYSTEM_TABLE_SCHEMA","SYSTEM_TABLE_NAME"])

    for (library_name,table_name), group in df:

        # 新しくフレームを作成するための空のリスト
        columns_new = []
        data_new_in = []
        data_new_out = []

        #各データ型のカウンター
        cnt_char = 0
        cnt_graphic = 0
        cnt_num = 1
        cnt_int = 0
        
        for index, row in group.iterrows():
            # カラム
            columns_new.append(row["COLUMN_NAME"])

            # データ
            ## CHAR
            if row["DATA_TYPE"] == "CHAR":
                cnt_char += 1
                if cnt_char <= 26:
                    data_new_in.append(string.ascii_letters[cnt_char-1])
                elif cnt_char > 26:
                    cnt_char_div = int(cnt_char/26)
                    if cnt_char%26 == 0:
                        data_new_in.append(f'{string.ascii_letters[cnt_char-(26*(cnt_char_div-1))-1]}{cnt_char_div-1}')
                    else:
                        data_new_in.append(f'{string.ascii_letters[cnt_char-(26*cnt_char_div)-1]}{cnt_char_div}')

            ## GRAPHIC
            elif row["DATA_TYPE"] == "GRAPHIC":
                cnt_graphic += 1
                data_new_in.append(mojimoji.han_to_zen(f'ダミーデータ{cnt_graphic}'))

            ## NUMERIC,DECIMAL
            elif row["DATA_TYPE"] == "NUMERIC" or row["DATA_TYPE"] == "DECIMAL":
                cnt_num += 0.1
                data_new_in.append(cnt_num)

            ## INTEGER
            elif row["DATA_TYPE"] == "INTEGER":
                cnt_int += 1
                data_new_in.append(cnt_int)

            else:
                raise Exception

        # [[]]の形にする    
        data_new_out.append(data_new_in)

        # テストデータフレームを作成
        df_testdata = pd.DataFrame(data = data_new_out, columns = columns_new)

        #ディレクトリ作成
        dir = pathlib.Path(f'dummy_data/{library_name}')
        dir.mkdir(parents=True, exist_ok=True)

        # .gzファイルへ出力(全角文字が文字化けするのでshift-jisにしている)
        df_testdata.to_csv(f'dummy_data/{library_name}/{table_name}.csv.gz', index = False, encoding="shift-jis")

except:
    print(traceback.format_exc())