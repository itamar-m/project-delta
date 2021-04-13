from http.server import BaseHTTPRequestHandler, HTTPServer
from multiprocessing import Process
from util import DB2ConnectionHandler
import logging
import random
import ibm_db
import time
import json
import psutil
import requests


# import os
# print (os.environ)


class APIServer(BaseHTTPRequestHandler):

    def _set_response(self):
        self.send_response(200)
        self.send_header('Content-type', 'application/json')
        self.end_headers()

    def do_GET(self):
        action = self.path[2:]
        connection = DB2ConnectionHandler.db_create_connection(logging)
        self._set_response()

        if action == "GET_SINGLE":
            self.wfile.write(get_single(connection).encode("utf-8"))
        elif action == "GET_ALL":
            self.wfile.write(get_all(connection).encode("utf-8"))
        elif action == "GET_INSERT_RANDOM":
            self.wfile.write(get_insert_random(connection).encode("utf-8"))
        else:
            print(json.dumps({"error": 404, "message": "This action: " + action + " does not exists!"}))
            self.wfile.write(json.dumps({"error": 404, "message": "This action: " + action +
                                                                  " does not exists!"}).encode("utf-8"))
        DB2ConnectionHandler.db_close_connection(connection, logging)


def db_create_connection():
    logging.info('Creating connection DB2 on IBM Cloud.')
    return ibm_db.connect("DATABASE=BLUDB;HOSTNAME=dashdb-txn-sbox-yp-dal09-04.services.dal.bluemix.net;PORT=50000;"
                          "PROTOCOL=TCPIP;UID=pnc70984;PWD=dw4^jg1sb8nxdk4h;;", "", "")


def db_close_connection(conn):
    ibm_db.close(conn)
    logging.info('Closing connection DB2 on IBM Cloud.')


def get_single(conn):
    sql = "SELECT * FROM PNC70984.ITA_TABLE"
    statement = ibm_db.prepare(conn, sql)
    ibm_db.execute(statement)
    dataRecord = False
    count = 0
    noData = False

    while noData is False:
        try:
            dataRecord = ibm_db.fetch_tuple(statement)
        except:
            pass

        if dataRecord is False:
            noData = True
        else:
            count = count + 1

    statement = ibm_db.prepare(conn, sql, {ibm_db.SQL_ATTR_CURSOR_TYPE: ibm_db.SQL_CURSOR_KEYSET_DRIVEN})
    ibm_db.execute(statement)
    dataRecord = ibm_db.fetch_assoc(statement, random.randint(0, count))

    return json.dumps({"COL1": dataRecord['COL1']})


def get_all(conn):
    sql = "SELECT * FROM PNC70984.ITA_TABLE"
    statement = ibm_db.prepare(conn, sql)
    ibm_db.execute(statement)
    dataRecord = False
    jsonstring = json.loads("{}")
    noData = False
    results = []

    while noData is False:
        try:
            dataRecord = ibm_db.fetch_tuple(statement)
            results.append(dataRecord[0])
        except:
            pass

        if dataRecord is False:
            noData = True

    jsonstring.update({"COL1": results})
    return json.dumps(jsonstring)


def get_insert_random(conn):
    randomCol = random.randint(0, 500)
    sql = f'{"""INSERT INTO PNC70984.ITA_TABLE VALUES("""}{randomCol}{""")"""}'
    statement = ibm_db.prepare(conn, sql)
    ibm_db.execute(statement)
    # returnCode = ibm_db.execute(statement)x
    # logging.info("Insert return code: %s", str(returnCode))

    sql = f'{"""SELECT * FROM PNC70984.ITA_TABLE WHERE COL1="""}{randomCol}'
    statement = ibm_db.prepare(conn, sql)
    returnCode = ibm_db.execute(statement)
    # message = f'{"Select return code: "}{returnCode}{"."}'
    # logging.info("Select return code: %s", str(returnCode))
    dataRecord = ibm_db.fetch_tuple(statement)

    return json.dumps({"StatusCode": returnCode, "COL1": dataRecord[0]})


def sendHealth(n):
    # """ Function to send health every n seconds """
    while True:
        #
        #
        # Fetch/compute the Mem usage in %, CPU usage %, Network throughput ratio in %
        # Active Request ratio in %, Success rate of requests in % and save in variables
        # mem, cpu, nwtp, arr, srr respectively (variable names can be as per your convenience)
        #
        #
        req_body = {}
        req_body['ip'] = '10.0.0.11'
        req_body['port'] = '32001'
        req_body['service_name'] = 'dbget'
        req_body['status'] = 'up'
        req_body['mem_usage'] = str(psutil.virtual_memory().percent)
        req_body['cpu_usage'] = str(psutil.cpu_percent())
        req_body['nw_tput_bw_ratio'] = str(30)     # current network throughput by the micro-service in %
        req_body['req_active_ratio'] = str('0.3')   # no. of requests currently being processed / Max. no. of request can be processed in %
        req_body['success_rate'] = str(80)          # fraction of requests successfully served in %
        req_body['health_interval'] = str(n)
        # print(req_body)
        r = requests.put(url="http://10.0.0.2:16461/health", data=req_body)
        logging.info('sharkradar health check status: %s.', r.text)
        try:
            time.sleep(n)
        except KeyboardInterrupt:
            pass


def run(server_class=HTTPServer, handler_class=APIServer, port=32001):
    logging.basicConfig(level=logging.INFO)
    server_address = ('', port)
    httpd = server_class(server_address, handler_class)
    logging.info('Starting dbGet API Server.')
    backGroundProc = False

    try:
        backGroundProc = Process(target=sendHealth, args=(25,))
        backGroundProc.start()
        httpd.serve_forever()
    except KeyboardInterrupt:
        backGroundProc.terminate()
        pass

    httpd.server_close()
    logging.info('Stopping dbGet API Server.')


if __name__ == '__main__':
    from sys import argv

    if len(argv) == 2:
        run(port=int(argv[1]))
    else:
        run()