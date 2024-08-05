import json
import psycopg2
import websocket

"""
UWB RTLS 데이터를 Sewio WebSocket을 통해 받아와서 DB에 저장하는 코드
"""

class DataManager:
    def __init__(self, configs):
        self.table_name = configs['table_name']
        self.configs = configs
        self.producer = None
        self.topic_name = None
        self.db_connect(configs)
        
    def db_connect(self, configs):
        try:
            self.conn = psycopg2.connect(
                dbname=configs['db_name'],
                user=configs['db_user'],
                password=configs['db_password'],
                host=configs['db_host'],
                port=configs['db_port']
            )
            self.cursor = self.conn.cursor()
            print("Database connection successfully established.")
        except Exception as e:
            print(f"Failed to connect to the database: {e}")
            
    def store_data_in_db(self, tag_id, posX, posY, timestamp, anchor_info):
        query = f"\
        INSERT INTO {self.table_name} (tag_id, x_position, y_position, timestamp, anchor_info) VALUES (%s, %s, %s, %s, %s)\
        "
        self.cursor.execute(query, (tag_id, posX, posY, timestamp, anchor_info))
        self.conn.commit()
        
        
class SewioWebSocket:
    def __init__(self, manager, api_key, socket_url, resource):
        self.manager = manager
        self.api_key = api_key
        self.resource = resource
        self.socket_url = socket_url
        
    def on_message(self, ws, message):
        # print("Received:", message)
        data = json.loads(message)
        tag_id = data["body"]["id"]
        posX = float(data["body"]["datastreams"][0]["current_value"].replace('%', ''))
        posY = float(data["body"]["datastreams"][1]["current_value"].replace('%', ''))
        timestamp = data["body"]["datastreams"][0]["at"]
        
        # extended_tag_position 존재 여부 확인 및 처리
        if "extended_tag_position" in data["body"]:
            anchor_info = json.dumps(data["body"]["extended_tag_position"])
        else:
            anchor_info = json.dumps({})        
        self.manager.store_data_in_db(tag_id, posX, posY, timestamp, anchor_info)
        
    def on_error(self, ws, error):
        print("Error:", error)

    def on_close(self, ws, close_status_code, close_msg):
        print("### closed ###")

    def on_open(self, ws):
        print("Opened connection")
        subscribe_message = f'{{"headers":{{"X-ApiKey":"{self.api_key}"}},\
                            "method":"subscribe","resource":"{self.resource}"}}'
        print(subscribe_message)
        ws.send(subscribe_message)

    def run(self):
        # websocket.enableTrace(True)
        ws = websocket.WebSocketApp(
            self.socket_url,
            on_message=self.on_message,
            on_error=self.on_error,
            on_close=self.on_close
        )
        ws.on_open = self.on_open
        ws.run_forever()

def main():
    # DB 연결 설정 json
    config_file = "configs.json"
    with open(config_file, 'r') as file:
        configs = json.load(file)
        
    manager = DataManager(configs)
    client = SewioWebSocket(manager, configs['X-ApiKey'], configs['socket_url'], configs['resource'])
    client.run()

if __name__ == "__main__":
    main()
