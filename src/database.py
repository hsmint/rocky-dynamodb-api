from typing import List, Dict
from convert import int2bits
from boto3.dynamodb.conditions import Key
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

    def item(self, bitmap : bool = False, scan : str = "") -> List[Dict]:
        reference_table = f'{self.database}-{self.BITMAP}' if bitmap else f'{self.database}-{self.BLOCKSNAPSHOT}'
        data = list()
        try:
            table = self.resource.Table(reference_table)
            data = table.scan().get('Items')
        except:
            print(f"Item was not found on {reference_table}")
        return data

    def item_by_key(self, key : Dict) -> List[Dict]:
        try:
            table = self.resource.Table(f'{self.database}-{self.BLOCKSNAPSHOT}')
            item = [table.get_item(Key=key).get('Item')]
        except Exception as e:
            print(e)
            item = []
        return item

    def item_by_epoch(self, epoch : int) -> List[Dict]:
        try:
            table = self.resource.Table(f'{self.database}-{self.BLOCKSNAPSHOT}')
            item_epoch = table.get_item(FilterExpression=Key("key").begins_with(f'{epoch}:')).get('Items')
            print(item_epoch) # TODO : DEBUG CODE
        except:
            item_epoch = []
        return item_epoch

    def item_by_block(self, block : int) -> List[Dict]:
        try:
            table = self.resource.Table(f'{self.database}-{self.BLOCKSNAPSHOT}')
            epoch = int.from_bytes(table.get_item(Key={"key": "EpochCount"}).get('Item')["value"].value, "big")
            item_block = []
            for i in range(1, epoch + 1):
                response = table.get_item(Key={"key" : f'{i}:{block}'}).get('Item')
                if response is not None: item_block.append(response)
        except:
            item_block = []
        return item_block

    def item_by_bitmap_epoch(self, epoch : int) -> List[Dict]:
        item = []
        try:
            table = self.resource.Table(f"{self.database}-{self.BITMAP}")
            response = table.get_item(Key={"key": f"{epoch}-bitmap"}).get('Item', [])["value"].value
            bitmap = []
            for byte in response:
                bitmap += int2bits(byte)
            table = self.resource.Table(f"{self.database}-{self.BLOCKSNAPSHOT}")
            bitmap = [i for i, val in enumerate(bitmap) if val == 1]
            item = [table.get_item(Key={"key" : f"{epoch}:{block_id}"}).get("Item") for block_id in bitmap]
        except:
            pass
        return item
if __name__ == "__main__":
    db = Database()
    print(db)
