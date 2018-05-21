import psycopg2

def clear_db():
    conn = psycopg2.connect(user="dbadmin", password="AdminPass123", host="localhost", dbname="projectdb18")
    #conn = psycopg2.connect(user="dbadmin", password="AdminPass123", host="localhost", dbname="testdb")

    # delete schemas
    for i in range(0, 500):
        try:
            conn.cursor().execute("DROP SCHEMA IF EXISTS \"" + str(i) + "\" CASCADE;")
            conn.commit()
            print("Deleted main schema",str(i))
        except Exception as e:
            conn.cursor().execute("ROLLBACK;")
            print(str(e))

        try:
            conn.cursor().execute("DROP SCHEMA IF EXISTS original_" + str(i) + " CASCADE;")
            conn.commit()
            print("Deleted original",str(i))
        except Exception as e:
            conn.cursor().execute("ROLLBACK;")
            print(str(e))


    # delete history
    conn.cursor().execute("DELETE FROM system.dataset_history WHERE TRUE;")
    conn.commit()
    print("Deleted history")

    # delete datasets
    conn.cursor().execute("DELETE FROM system.datasets WHERE TRUE;")
    conn.commit()
    print("Deleted datasets")

    # delete users
    conn.cursor().execute("DELETE FROM system.user_accounts WHERE TRUE;")
    conn.commit()
    print("Deleted users")

    # delete permissions
    conn.cursor().execute("DELETE FROM system.set_permissions WHERE TRUE;")
    conn.commit()
    print("Deleted permissions")
