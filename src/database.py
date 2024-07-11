from typing import List, Dict
import boto3

class Database:
    def __init__(self, local : bool = True) -> None:
        self.client = boto3.client('dynamodb', endpoint_url="http://localhost:8000")
        self.resource = boto3.resource('dynamodb', endpoint_url="http://localhost:8000")
        self.database = None
        self.BITMAP = 'cloudEpochBitmapsTable'
        self.BLOCKSNAPSHOT = 'cloudBlockSnapshotStoreTable'

    def set_name(self, name : str) -> None:
        self.database = name

    def list_tables(self) -> List[Dict]:
        tables = self.client.list_tables().get("TableNames")
        keys = [i for i in range(len(tables))]
        return [dict(zip(keys, tables))]

    def item(self, bitmap : bool = False) -> List[Dict]:
        reference_table = f'{self.database}-{self.BITMAP}' if bitmap else f'{self.database}-{self.BLOCKSNAPSHOT}'
        data = list()
        try:
            table = self.resource.Table(reference_table)
            data = table.scan().get('Items')
        except:
            print(f"Item was not found on {reference_table}")
        return data

    def item_by_key(self, key : Dict) -> List[Dict]:
        table = self.resource.Table(f'{self.database}-{self.BLOCKSNAPSHOT}')
        response = table.get_item(Key=key)
        return response.get('Item')

    def item_by_epoch(self, epoch : int) -> List[Dict]:
        items = self.item()
        item_epoch = [item for item in items if int(item["key"].split(':')[0]) == epoch]
        return item_epoch

    def item_by_block(self, block : int) -> List[Dict]:
        items = self.item()
        item_block = [item for item in items if int(item["key"].split(':')[1]) == block]
        return item_block

    def item_by_bitmap_epoch(self, epcoh : int) -> List[Dict]:
        item = self.item(True)
        # TODO: Implement needed
        return []

if __name__ == "__main__":
    db = Database()
    print(db)
