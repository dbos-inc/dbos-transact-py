from dbos import DBOS, DBOSConfiguredInstance


@DBOS.dbos_class(class_name="AnotherDBOSTestRegDup")
class DBOSTestRegDup(DBOSConfiguredInstance):
    """DBOSTestRegDup duplicates the name of a class defined dupname_classdefsa.py"""

    def __init__(self, instance_name: str) -> None:
        super().__init__(instance_name)
