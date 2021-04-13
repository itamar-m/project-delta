import ibm_db

database = ""
hostname = ""
port = "50000"
protocol = "TCPIP"
uid = ""
pwd = ""


def db_create_connection(logg):
    logg.info('Creating connection DB2 on IBM Cloud.')
    return ibm_db.connect("DATABASE="+database+";HOSTNAME="+hostname+";PORT="+port+";PROTOCOL="+protocol+";"
                          "UID="+uid+";PWD="+pwd+";;", "", "")


def db_close_connection(conn, logg):
    ibm_db.close(conn)
    logg.info('Closing connection DB2 on IBM Cloud.')
