import snowflake.connector
ctx = snowflake.connector.connect(
    user = "RSAITO",
    password = "Ufrks5971",
    account = "EJDZPZS-VD41943"
)

cs = ctx.cursor()
cs.execute("use iris2")
cs.execute("select * from iris_data2")

df = cs.fetch_pandas_all()

print(df)