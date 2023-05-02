from websockets.sync.client import connect
import pickle

def get_socket_message(query=""):
    with connect("ws://localhost:8765") as websocket:
        websocket.send(query)
        message = websocket.recv()
        unpickled_message = pickle.loads(message)
        return unpickled_message
