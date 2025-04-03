from dbos import DBOS


@DBOS.dbos_class()
class DBOSTestRegDup:
    """DBOSTestRegDup duplicate the name of a class defined in test_classdecorators.py"""

    pass


@DBOS.dbos_class()
class DBOSTestClass:
    """DBOSTestClass duplicates the name of a class defined in classdefs.py

    The purpose is to test if two classes of the same name, in different
    packages, can be registration in DBOS.dbos_class().
    """

    @DBOS.dbos_class()
    class Inner:
        """Inner has same name as a nested class below.

        The purpose is to test if __qualname__ is properly used in registration.
        """

        pass


@DBOS.dbos_class()
class DBOSSendRecv:
    """DBOSSendRecv duplicates the name of a class defined in classdefs.py

    The purpose is to test if two classes of the same name, in different
    packages, can be registration in DBOS.dbos_class().
    """

    @DBOS.dbos_class()
    class Inner:
        """Inner has same name as a nested class above.

        The purpose is to test if __qualname__ is properly used in registration.
        """

        pass
