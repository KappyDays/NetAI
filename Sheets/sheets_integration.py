from googleapiclient import discovery
from google.oauth2 import service_account
import json

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
    
    def write_spreadsheet(self, range_name, values):
        result = (
            self.service.spreadsheets()
            .values()
            .update(
                spreadsheetId=self.spreadsheet_id,
                range=range_name,
                valueInputOption='RAW', # "USER_ENTERED"
                body=values
            )
            .execute()
        )
            
def main():
    googlesheetUtils = GooglesheetUtils()
    googlesheetUtils.read_spreadsheet('DataCenter!A1:B8')
    googlesheetUtils.write_spreadsheet('DataCenter!A4:B6', {'values': [['12', '23'], ['34', '45'], ['5', '6']]})

if __name__ == "__main__":
    main()