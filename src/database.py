from typing import List, Dict
import boto3

class Database:
    def __init__(self, local : bool = True) -> None:
        self.client = boto3.client('dynamodb', endpoint_url="http://localhost:8000")
        self.resource = boto3.resource('dynamodb', endpoint_url="http://localhost:8000")
        self.database = None
        self.BITMAP = 'bitmap'
        self.BLOCKSNAPSHOT = 'blocksnapshot'

    def set_name(self, name : str) -> None:
        self.database = name

    def list_tables(self) -> Dict:
        tables = self.client.list_tables().get("TableNames")
        keys = [i for i in range(len(tables))]
        return dict(zip(keys, tables))

    def item(self, bitmap : bool = False) -> Dict:
        table = self.resource.Table(f'{self.database}{self.BITMAP}') if bitmap else self.resource.Table(f'{self.database}{self.BLOCKSNAPSHOT}')
        response = table.scan()
        return response.get('Items')

    def item_by_key(self, key : Dict) -> Dict:
        table = self.resource.Table(f'{self.database}{self.BLOCKSNAPSHOT}')
        response = table.get_item(Key=key)
        return response.get('Item')

    def item_by_epoch(self, epoch : int) -> Dict:
        items = self.item()
        # TODO: Implement needed
        return {}

    def item_by_block(self, block : int) -> Dict:
        items = self.item()
        # TODO: Implement needed
        return {}

    def item_by_bitmap_epoch(self, epcoh : int) -> Dict:
        item = self.item(True)
        # TODO: Implement needed
        return {}


if __name__ == "__main__":
    db = Database()
    print(db)
