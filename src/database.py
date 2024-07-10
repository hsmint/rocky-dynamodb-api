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

    def list_tables(self):
        return self.client.list_tables()

    def item(self, bitmap : bool = False):
        table = self.resource.Table(f'{self.database}{self.BITMAP}') if bitmap else self.resource.Table(f'{self.database}{self.BLOCKSNAPSHOT}')
        response = table.scan()
        return response.get('Items')

    def item_by_key(self, key : dict):
        table = self.resource.Table(f'{self.database}{self.BLOCKSNAPSHOT}')
        response = table.get_item(Key=key)
        return response.get('Item')

    def item_by_epoch(self, epoch : int):
        items = self.item()
        # TODO: Implement needed

    def item_by_block(self, block : int):
        items = self.item()
        # TODO: Implement needed

    def item_by_bitmap_epoch(self, epcoh : int):
        item = self.item(True)
        # TODO: Implement needed


if __name__ == "__main__":
    db = Database("testinglocal")
    print(db)
