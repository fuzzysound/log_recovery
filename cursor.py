import pymysql

connection = pymysql.connect(
    host="147.46.15.66",
    user="sureter",
    password="bde1234",
    db='sureter',
    charset='utf8',
    cursorclass=pymysql.cursors.DictCursor
)

cursor = connection.cursor()