from typing import List, Dict
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

    def __key_match(self, key : str, epoch : int = -1, block = -1) -> bool:
        try:
            split_key = key.split(':')
            if epoch != -1 and block != -1:
                raise Exception("Both epoch and block are set")
            return (epoch != -1 and int(split_key[0]) == epoch) or (block != -1 and int(split_key[1]) == block)
        except:
            return False
        return False

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
            item_epoch = [table.get_item(FilterExpression=Key("key").begins_with(f'{epoch}:')).get('Item')]
            print(item_epoch) # TODO : DEBUG CODE
        except:
            item_epoch = []
        return item_epoch

    def item_by_block(self, block : int) -> List[Dict]:
        try:
            table = self.resource.Table(f'{self.database}-{self.BLOCKSNAPSHOT}')
            item_block = [table.get_item(Key=f'{i}:{block}').get('Item') for i in range(1000)]
        except:
            item_block = []
        return item_block

    def item_by_bitmap_epoch(self, epcoh : int) -> List[Dict]:
        item = self.item(bitmap = True)
        # TODO: Implement needed
        print(item) # TEST CODE
        return []

if __name__ == "__main__":
    db = Database()
    print(db)
