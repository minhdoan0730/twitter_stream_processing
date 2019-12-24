import socket
import json

from tweepy import OAuthHandler
from tweepy import Stream
from tweepy.streaming import StreamListener
from codecs import encode, decode

ACCESS_TOKEN = ''
ACCESS_SECRET = ''
CONSUMER_KEY = ''
CONSUMER_SECRET = ''

class StdOutListener(StreamListener):
    def __init__(self, csocket):
        self.client_socket = csocket
    
    def on_data(self, data):
        try:
            msg = json.loads(data)
            if self.client_socket:
                print("<============= SEND THE TWEET ==============> ")
                self.client_socket.send(msg['text'].encode('utf-8'))
            return True
        except BaseException as e:
            print("Error on_data(): %s" % str(e))
        return True

    def on_error(self, status):
        print("Error on_status() ", status)
# end

def send_data(c_socket):
    auth = OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
    auth.set_access_token(ACCESS_TOKEN, ACCESS_SECRET)
    twitter_stream = Stream(auth, StdOutListener(c_socket))
    #This line filter Twitter Streams to capture data by the keywords
    data_stream = twitter_stream.filter(track=['technology', 'code', 'programming'])
# end

if __name__ == "__main__":
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    host = "127.0.0.1"
    port = 5556
    s.bind((host, port))
    print("Listening on port: %s" % str(port))
    s.listen(5)
    while True:
        client_socket, addr = s.accept()
        send_data(client_socket)
        client_socket.close()
