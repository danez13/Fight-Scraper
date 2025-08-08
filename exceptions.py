class EntityExistsError(Exception):
    def __init__(self, entity_type: str, entity_id: str):
        super().__init__(f"{entity_type} {entity_id} already exists.")
        self.entity_type = entity_type
        self.entity_id = entity_id
    