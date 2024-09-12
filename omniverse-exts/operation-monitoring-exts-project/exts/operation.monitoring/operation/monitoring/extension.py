import omni.ext
import omni.ui as ui
import asyncio
from aiokafka import AIOKafkaConsumer
from pxr import Usd, UsdGeom, Sdf
import omni.kit.pipapi
import os
# omni.kit.pipapi.call_pip(["--help"])
omni.kit.pipapi.install("google-api-python-client")
omni.kit.pipapi.install("google-auth-httplib2")
omni.kit.pipapi.install("google-auth-oauthlib")
from googleapiclient import discovery
from google.oauth2 import service_account
import json

class OperationMonitoringExtension(omni.ext.IExt):
    # def __init__(self):
    #     self.__super.__init__()

        

        
    def on_startup(self, ext_id):
        print("[operation.monitoring] operation monitoring startup")
        
        # 현재 파일이 있는 경로를 기준으로 JSON 파일 경로 설정
        current_dir = os.path.dirname(os.path.abspath(__file__))
        my_config_path = os.path.join(current_dir, "my_config.json")
        
        with open(my_config_path, 'r') as f:
            self.configs = json.load(f)
            
        self.user_json_file_path = os.path.join(current_dir, self.configs['svc_account_file'])                

        # Trakcing 횟수
        self.count = 0
        self._messages = []  # Kafka 메시지를 저장할 리스트
        self._stop_event = asyncio.Event()  # 소비 중지 이벤트 플래그

        # UI 설정
        self._window = ui.Window("Operation Monitoring", width=500, height=300)
        with self._window.frame:
            with ui.VStack():
                self.label = ui.Label("default")
                self.label2 = ui.Label("sheet_default")
                
                def on_start_consumer():
                    print("Start Consumer")
                    if not self._stop_event.is_set():
                        self._stop_event.clear()  # 이전 중지 플래그 초기화
                        asyncio.ensure_future(self.consume())  # Kafka 소비 비동기 함수 실행

                def on_stop_consumer():
                    print("Stop Consumer")
                    self._stop_event.set()  # 소비 중지 플래그 설정
                
                def on_start_sync_sheets():
                    self.start_sheets_syncronization()
                    pass
                
                def on_stop_sync_sheets():
                    pass

                with ui.HStack():
                    ui.Button("Start Consumer", clicked_fn=on_start_consumer)
                    ui.Button("Stop Consumer", clicked_fn=on_stop_consumer)
                    ui.Button("Start Sync Sheets", clicked_fn=on_start_sync_sheets)
                    ui.Button("Stop Sync Sheets", clicked_fn=on_stop_sync_sheets)

    async def consume(self):
        consumer = AIOKafkaConsumer(
            self.configs['kafka_topic'],  # topic
            bootstrap_servers=self.configs['kafka_bootstrap_servers'],
            group_id=self.configs['kafka_group_id']
        )
            
        # Kafka consumer 시작
        await consumer.start()
        try:
            # 무한 루프를 통해 메시지 소비
            async for msg in consumer:
                if self._stop_event.is_set():  # 소비 중지 플래그 확인
                    break
                datas = msg.key.decode('utf-8'), msg.value.decode('utf-8')
                data = msg.value.decode('utf-8')
                self._messages.append(datas)  # 소비한 메시지를 리스트에 저장
                print("Consumed message:", data)

                # UI 업데이트 (메인 쓰레드에서 실행될 수 있도록)
                if len(self._messages) == 5:
                    self.update_ui(self._messages)
                    self._messages = []

        finally:
            # Kafka consumer 종료
            await consumer.stop()

    def update_ui(self, messages):
        # key, value = messages
        print(messages)
        # Kafka에서 받은 메시지를 UI에 업데이트
        if self._window:
            # for message in messages:
            #     key, value = message
            self.label.text = f'Update Count:{self.count}\n{messages[0]}\n{messages[1]}\n{messages[2]}\n{messages[3]}\n{messages[4]}'
            self.count += 1
    
    def start_sheets_syncronization(self):
        # Prim 경로로부터 Usd.Prim 가져오기
        stage = omni.usd.get_context().get_stage()
        prim = stage.GetPrimAtPath('/World/L40_Node1')
        
        sheet_id = self.configs['sheet_id']

        self.credentials = service_account.Credentials.from_service_account_file(
            # Google Cloud > 서비스 계정 > 서비스 계정 만들기 > 키 > 키 추가 > json 파일 다운로드
            self.user_json_file_path, #'ultra-optics-433707-s2-b93cb4a7a435.json',
            scopes = ['https://www.googleapis.com/auth/spreadsheets']
        )
        self.service = discovery.build('sheets','v4', credentials=self.credentials)

        result = (
            self.service.spreadsheets()
            .values()
            .get(spreadsheetId=sheet_id,
                 range='DataCenter!N39'
            )
            .execute()
        )
        # Get the values from the response
        values = result.get('values', [])
        print(values)
        # Print the values
        for row in values:
            print(row)
        value_list = values[0][0].split('\n')
        # value_list[0]
        self.label2.text = f'{value_list[0]}\n{value_list[1]}\n{value_list[2]}'
                          
        # if (not prim.IsValid()) or prim == None:
        #     print("Prim not found")
        #     return
        # else:
        #     # 속성 추가 - 'my_custom_property'라는 이름의 속성 추가
        #     custom_property_name = "my_custom_property"
            
        #     # 속성 타입 설정 (예: Sdf.ValueTypeNames.String으로 문자열 속성 추가)
        #     custom_attr = prim.CreateAttribute(custom_property_name, Sdf.ValueTypeNames.String)
            
        #     # 속성 값 설정
        #     custom_attr.Set("My Custom Value")
            
        #     print(f"Added custom property '{custom_property_name}' with value 'My Custom Value' to Prim at /World/L40_Node1")
        

    def on_shutdown(self):
        print("[operation.monitoring] operation monitoring shutdown")
        if self._stop_event is not None:
            self._stop_event.set()  # 소비 중지 플래그 설정




def json_read(json_file):
    with open(json_file, 'r') as f:
        return json.load(f)
    
class GooglesheetUtils:
    def __init__(self) -> None:
        # sheet id는 구글 스프레드 시트의 url에서 확인 가능
        # ex) ~/spreadsheets/d/sheet_id/~
        sheetID_json = json_read('my_private_setting.json')
        self.spreadsheet_id = sheetID_json["sheet_id"]
        self.credentials = service_account.Credentials.from_service_account_file(
            # Google Cloud > 서비스 계정 > 서비스 계정 만들기 > 키 > 키 추가 > json 파일 다운로드
            'ultra-optics-433707-s2-b93cb4a7a435.json',
            scopes = ['https://www.googleapis.com/auth/spreadsheets']
        )
        self.service = discovery.build('sheets','v4', credentials=self.credentials)

    def read_spreadsheet(self, range_name) -> None:
        result = (
            self.service.spreadsheets()
            .values()
            .get(spreadsheetId=self.spreadsheet_id,
                 range=range_name
            )
            .execute()
        )
        # Get the values from the response
        values = result.get('values', [])

        # Print the values
        for row in values:
            print(row)