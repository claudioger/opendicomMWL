import sys
import os
from mwl import ModalityWorkList


def main(argv):
    if len(argv) == 8:
        try:
            aet = argv[1]
            port = int(argv[2])
            user_db = argv[3]
            password_db = argv[4]
            ip_db = argv[5]
            name_db = argv[6]
            debug = bool(int(argv[7]))
        except Exception as e:
            print("Parameters error, check README.md : ", str(e))
            exit(os.EX_CONFIG)

        mwl = ModalityWorkList(aet, user_db, password_db, ip_db, name_db, debug)
        if mwl.check_database_connection():
            mwl.execute(port)
        else:
            print("Database connection error, check README.md")
            exit(os.EX_CONFIG)
    else:
        print("Missing parameters, check README.md")
        exit(os.EX_CONFIG)


if __name__ == "__main__":
    main(sys.argv)
