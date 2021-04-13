from http.server import BaseHTTPRequestHandler, HTTPServer
import requests
import logging


class APIGateway(BaseHTTPRequestHandler):

    def _set_response(self):
        self.send_response(200)
        self.send_header('Content-type', 'application/json')
        self.end_headers()

    def do_GET(self):
        action = self.path[1:]
        service = discover("dbget")
        print(service.text)
        response = requests.get("http://"+service.json()['ip']+":"+service.json()['port'], action)
        self._set_response()
        self.wfile.write(response.content)

    def do_POST(self):
        content_length = int(self.headers['Content-Length'])  # <--- Gets the size of data
        post_data = self.rfile.read(content_length)  # <--- Gets the data itself
        service = discover("dbpost")
        print(service.text)
        response = requests.post("http://"+service.json()['ip']+":"+service.json()['port'], post_data)

        self._set_response()
        self.wfile.write(response.content)


def discover(service_name):
    return requests.get(url="http://10.0.0.2:16461/discovery/id/" + "/" + service_name)


def run(server_class=HTTPServer, handler_class=APIGateway, port=8080):
    logging.basicConfig(level=logging.INFO)
    server_address = ('', port)
    httpd = server_class(server_address, handler_class)
    logging.info('Starting apiGateway.')
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        pass

    httpd.server_close()
    logging.info('Stopping apiGateway.')


if __name__ == '__main__':
    from sys import argv

    if len(argv) == 2:
        run(port=int(argv[1]))
    else:
        run()
