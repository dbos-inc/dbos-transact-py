from dbos import DBOS, DBOSConfiguredInstance


@DBOS.dbos_class()
class DBOSTestRegDup(DBOSConfiguredInstance):
    """DBOSTestRegDup duplicates the name of a class defined dupname_classdefs1.py"""

    def __init__(self, instance_name: str) -> None:
        super().__init__(instance_name)
