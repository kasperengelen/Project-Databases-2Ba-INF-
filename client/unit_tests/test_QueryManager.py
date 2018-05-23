import unittest
import datetime
import psycopg2
import psycopg2.extras
from Model.DatabaseConfiguration import TestConnection
from Model.QueryManager import QueryManager

class TestQueryManager(unittest.TestCase):
    """Tests for the QueryManager class."""

    @classmethod
    def setUpClass(cls):
        """Prepare setup environment."""
        cls.db_conn = TestConnection().get_db()
        cls.engine = TestConnection().get_engine()
        cls.cur = cls.db_conn.cursor()
        cls.dict_cur = cls.db_conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

        cls.cur.execute("DELETE FROM SYSTEM.datasets WHERE TRUE;")
        cls.db_conn.commit()

        cls.cur.execute("DELETE FROM SYSTEM.user_accounts WHERE TRUE;");
        cls.db_conn.commit()

        cls.query_man = QueryManager(db_conn=cls.db_conn, engine=None)
    # ENDMETHOD

    @classmethod
    def tearDownClass(cls):
        """Clean up setup environment."""
        cls.cur.execute("DELETE FROM SYSTEM.datasets WHERE TRUE;")
        cls.db_conn.commit()

        cls.cur.execute("DELETE FROM SYSTEM.user_accounts WHERE TRUE;");
        cls.db_conn.commit()

        cls.db_conn.close()
        cls.engine.dispose()
    # ENDMETHOD

    def test_getUser(self):
        """Test select statements on system.user_accounts."""

        #### SETUP
        self.dict_cur.execute("INSERT INTO system.user_accounts(fname, lname, email, passwd) VALUES (%s, %s, %s, %s) RETURNING userid;", ['Peter', 'Selie', 'peter.selie@email.com', 'password123'])
        self.db_conn.commit()
        user1_id = self.dict_cur.fetchone()['userid']

        self.dict_cur.execute("INSERT INTO system.user_accounts(fname, lname, email, passwd, register_date) VALUES (%s, %s, %s, %s, %s) RETURNING userid;", ['Peter', 'Pieters', 'peter.selie2@email.com', 'password123', datetime.datetime(year=2015, month=10, day=5, hour=23, minute=20, second=59).isoformat()])
        self.db_conn.commit()
        user2_id = self.dict_cur.fetchone()['userid']

        self.dict_cur.execute("INSERT INTO system.user_accounts(fname, lname, email, passwd) VALUES (%s, %s, %s, %s) RETURNING userid;", ['Jan', 'Met de pet', 'jan.met.de@pet.com', 'abcdef123'])
        self.db_conn.commit()
        user3_id = self.dict_cur.fetchone()['userid']
        #### SETUP

        #### TEST
        res = self.query_man.getUser(fname="Peter")
        self.assertEqual(len(res), 2)
        self.assertEqual(res[0]['lname'], "Selie")

        res = self.query_man.getUser(userid=user1_id)
        self.assertEqual(len(res), 1)
        self.assertEqual(res[0]['lname'], "Selie")

        res = self.query_man.getUser(register_date=datetime.datetime(year=2015, month=10, day=5, hour=23, minute=20, second=59))
        self.assertEqual(len(res), 1)
        self.assertEqual(res[0]['userid'], user2_id)

        res = self.query_man.getUser()
        self.assertEqual(len(res), 3)
        #### TEST

        #### CLEAN
        self.cur.execute("DELETE FROM SYSTEM.set_permissions WHERE TRUE;")
        self.db_conn.commit()

        self.cur.execute("DELETE FROM SYSTEM.datasets WHERE TRUE;")
        self.db_conn.commit()

        self.cur.execute("DELETE FROM SYSTEM.user_accounts WHERE TRUE;");
        self.db_conn.commit()
        #### CLEAN
    # ENDTEST

    def test_getDataset(self):
        """Test select statements on system.datasets."""

        #### SETUP
        self.dict_cur.execute("INSERT INTO system.datasets(setname, description) VALUES (%s, %s) RETURNING setid;", ['A', 'Dataset A'])
        self.db_conn.commit()
        dataset1_id = self.dict_cur.fetchone()['setid']

        self.dict_cur.execute("INSERT INTO system.datasets(setname, description, creation_date) VALUES (%s, %s, %s) RETURNING setid;", ['B', 'Dataset B', datetime.datetime(year=20, month=12, day=10, hour=5, minute=59, second=59)])
        self.db_conn.commit()
        dataset2_id = self.dict_cur.fetchone()['setid']

        self.dict_cur.execute("INSERT INTO system.datasets(setname, description) VALUES (%s, %s) RETURNING setid;", ['C', 'Dataset C'])
        self.db_conn.commit()
        dataset3_id = self.dict_cur.fetchone()['setid']

        self.dict_cur.execute("INSERT INTO system.datasets(setname, description) VALUES (%s, %s) RETURNING setid;", ['B', 'Another B'])
        self.db_conn.commit()
        dataset4_id = self.dict_cur.fetchone()['setid']
        #### SETUP

        #### TEST
        res = self.query_man.getDataset(setname="B")
        self.assertEqual(len(res), 2)
        self.assertEqual(res[0]['creation_date'], datetime.datetime(year=20, month=12, day=10, hour=5, minute=59, second=59))

        res = self.query_man.getDataset(setid=dataset1_id)
        self.assertEqual(len(res), 1)
        self.assertEqual(res[0]['setname'], 'A')
        self.assertEqual(res[0]['description'], "Dataset A")

        res = self.query_man.getDataset(description="Dataset C")
        self.assertEqual(len(res), 1)
        self.assertEqual(res[0]['description'], "Dataset C")

        res = self.query_man.getDataset()
        self.assertEqual(len(res), 4)

        #### TEST

        #### CLEAN
        self.cur.execute("DELETE FROM SYSTEM.set_permissions WHERE TRUE;")
        self.db_conn.commit()

        self.cur.execute("DELETE FROM SYSTEM.datasets WHERE TRUE;")
        self.db_conn.commit()

        self.cur.execute("DELETE FROM SYSTEM.user_accounts WHERE TRUE;");
        self.db_conn.commit()
        #### CLEAN
    # ENDTEST

    def test_getPermission(self):
        """Test select statements on system.set_permissions."""

        #### SETUP
        # insert user
        self.dict_cur.execute("INSERT INTO system.user_accounts(fname,lname,email,passwd) VALUES (%s,%s,%s,%s) RETURNING userid;", ['Jan', 'Met de pet', 'email@adres.com', 'password123'])
        self.db_conn.commit()
        userid = self.dict_cur.fetchone()['userid']

        # insert dataset
        self.dict_cur.execute("INSERT INTO system.datasets(setname, description) VALUES (%s, %s) RETURNING setid;", ['Set A', 'Some dataset.'])
        self.db_conn.commit()
        setid = self.dict_cur.fetchone()['setid']

        # insert permission
        self.dict_cur.execute("INSERT INTO system.set_permissions(userid, setid, permission_type) VALUES (%s,%s,%s);", [userid, setid, 'admin'])
        self.db_conn.commit()
        #### SETUP

        #### TEST
        results = self.query_man.getPermission(userid=userid)
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]['setid'], setid)
        #### TEST

        #### CLEAN
        self.cur.execute("DELETE FROM SYSTEM.set_permissions WHERE TRUE;")
        self.db_conn.commit()

        self.cur.execute("DELETE FROM SYSTEM.datasets WHERE TRUE;")
        self.db_conn.commit()

        self.cur.execute("DELETE FROM SYSTEM.user_accounts WHERE TRUE;");
        self.db_conn.commit()
        #### CLEAN
    # ENDTEST

    def test_insertUser(self):
        """Test insert statements on system.user_accounts."""

        user1_id = self.query_man.insertUser(fname="Peter", lname="Selie", email="email@website.com", passwd="abc123", active=False, returning='userid')
        user2_admin = self.query_man.insertUser(fname="Jan", lname="Met de pet", email="some_email@website.com", passwd="password123", returning='admin')

        self.dict_cur.execute("SELECT * FROM system.user_accounts WHERE userid=%s;", [user1_id])
        res = self.dict_cur.fetchall()

        self.assertEqual(len(res), 1)

        userdata = res[0]

        self.assertEqual(userdata['userid'], user1_id)
        self.assertEqual(userdata['fname'], "Peter")
        self.assertEqual(userdata['lname'], "Selie")
        self.assertEqual(userdata['email'], "email@website.com")
        self.assertEqual(userdata['passwd'], "abc123")
        self.assertEqual(userdata['active'], False)

        self.assertFalse(user2_admin)

        #### CLEAN
        self.cur.execute("DELETE FROM SYSTEM.set_permissions WHERE TRUE;")
        self.db_conn.commit()

        self.cur.execute("DELETE FROM SYSTEM.datasets WHERE TRUE;")
        self.db_conn.commit()

        self.cur.execute("DELETE FROM SYSTEM.user_accounts WHERE TRUE;");
        self.db_conn.commit()
        #### CLEAN
    # ENDTEST

    def test_insertDataset(self):
        """Test insert statements on system.datasets."""

        setid_1 = self.query_man.insertDataset(setname="Set A", description="Some Dataset")
        setid_2 = self.query_man.insertDataset(setname="Set A", description="Another A")

        self.dict_cur.execute("SELECT * FROM system.datasets WHERE setname=%s;", ["Set A"])
        result = self.dict_cur.fetchall()

        self.assertEqual(len(result), 2)

        #### CLEAN
        self.cur.execute("DELETE FROM SYSTEM.set_permissions WHERE TRUE;")
        self.db_conn.commit()

        self.cur.execute("DELETE FROM SYSTEM.datasets WHERE TRUE;")
        self.db_conn.commit()

        self.cur.execute("DELETE FROM SYSTEM.user_accounts WHERE TRUE;");
        self.db_conn.commit()
        #### CLEAN
    # ENDTEST

    def test_insertPermission(self):
        """Test insert statements on system.set_permissions."""
        #### SETUP
        # insert user
        self.dict_cur.execute("INSERT INTO system.user_accounts(fname,lname,email,passwd) VALUES (%s,%s,%s,%s) RETURNING userid;", ['Jan', 'Met de pet', 'email@adres.com', 'password123'])
        self.db_conn.commit()
        userid = self.dict_cur.fetchone()['userid']

        # insert dataset
        self.dict_cur.execute("INSERT INTO system.datasets(setname, description) VALUES (%s, %s) RETURNING setid;", ['Set A', 'Some dataset.'])
        self.db_conn.commit()
        setid = self.dict_cur.fetchone()['setid']
        #### SETUP

        #### TEST
        self.query_man.insertPermission(userid=userid, setid=setid, permission_type='write')

        self.dict_cur.execute("SELECT * FROM system.set_permissions WHERE setid=%s AND userid=%s;", [setid, userid])
        result = self.dict_cur.fetchone()

        self.assertTrue(result is not None)
        self.assertTrue(result['permission_type'], 'write')
        #### TEST

        #### CLEAN
        self.cur.execute("DELETE FROM SYSTEM.set_permissions WHERE TRUE;")
        self.db_conn.commit()

        self.cur.execute("DELETE FROM SYSTEM.datasets WHERE TRUE;")
        self.db_conn.commit()

        self.cur.execute("DELETE FROM SYSTEM.user_accounts WHERE TRUE;");
        self.db_conn.commit()
        #### CLEAN


    # ENDTEST

    def test_deleteUser(self):
        """Test delete statements on system.user_accounts."""
        
        #### SETUP
        self.dict_cur.execute("INSERT INTO system.user_accounts(fname, lname, email, passwd) VALUES (%s, %s, %s, %s) RETURNING userid;", ['Peter', 'Selie', 'peter.selie@email.com', 'password123'])
        self.db_conn.commit()
        user1_id = self.dict_cur.fetchone()['userid']

        self.dict_cur.execute("INSERT INTO system.user_accounts(fname, lname, email, passwd) VALUES (%s, %s, %s, %s) RETURNING userid;", ['Peter', 'Pieters', 'peter.selie2@email.com', 'password123'])
        self.db_conn.commit()
        user2_id = self.dict_cur.fetchone()['userid']

        self.dict_cur.execute("INSERT INTO system.user_accounts(fname, lname, email, passwd) VALUES (%s, %s, %s, %s) RETURNING userid;", ['Jan', 'Met de pet', 'jan.met.de@pet.com', 'abcdef123'])
        self.db_conn.commit()
        user3_id = self.dict_cur.fetchone()['userid']
        #### SETUP

        #### TEST
        self.query_man.deleteUser(userid=user1_id)

        self.dict_cur.execute("SELECT * FROM system.user_accounts WHERE userid=%s;", [user1_id])
        result = self.dict_cur.fetchall()
        self.assertEqual(len(result), 0)

        self.dict_cur.execute("SELECT * FROM system.user_accounts WHERE userid=%s;", [user2_id])
        result = self.dict_cur.fetchall()
        self.assertEqual(len(result), 1)

        self.query_man.deleteUser(userid=user2_id)
        self.query_man.deleteUser(userid=user3_id)

        self.dict_cur.execute("SELECT * FROM system.user_accounts;")
        result = self.dict_cur.fetchall()
        self.assertEqual(len(result), 0)
        #### TEST

        #### CLEAN
        self.cur.execute("DELETE FROM SYSTEM.set_permissions WHERE TRUE;")
        self.db_conn.commit()

        self.cur.execute("DELETE FROM SYSTEM.datasets WHERE TRUE;")
        self.db_conn.commit()

        self.cur.execute("DELETE FROM SYSTEM.user_accounts WHERE TRUE;");
        self.db_conn.commit()
        #### CLEAN
    # ENDTEST

    def test_deleteDataset(self):
        """Test delete statements on system.datasets."""
        
        #### SETUP
        self.dict_cur.execute("INSERT INTO system.datasets(setname, description) VALUES (%s, %s) RETURNING setid;", ['A', 'Dataset A'])
        self.db_conn.commit()
        dataset1_id = self.dict_cur.fetchone()['setid']

        self.dict_cur.execute("INSERT INTO system.datasets(setname, description) VALUES (%s, %s) RETURNING setid;", ['B', 'Dataset B'])
        self.db_conn.commit()
        dataset2_id = self.dict_cur.fetchone()['setid']

        self.dict_cur.execute("INSERT INTO system.datasets(setname, description) VALUES (%s, %s) RETURNING setid;", ['C', 'Dataset C'])
        self.db_conn.commit()
        dataset3_id = self.dict_cur.fetchone()['setid']
        #### SETUP

        #### TEST
        self.query_man.deleteDataset(setid=dataset1_id)

        self.dict_cur.execute("SELECT * FROM system.datasets WHERE setid=%s;", [dataset1_id])
        res = self.dict_cur.fetchall()

        self.assertEqual(len(res), 0)

        self.dict_cur.execute("SELECT * FROM system.datasets;")
        res = self.dict_cur.fetchall()

        self.assertEqual(len(res), 2)

        self.query_man.deleteDataset(setid=dataset2_id)
        self.query_man.deleteDataset(setid=dataset3_id)

        self.dict_cur.execute("SELECT * FROM system.datasets;")
        res = self.dict_cur.fetchall()

        self.assertEqual(len(res), 0)
        #### TEST

        #### CLEAN
        self.cur.execute("DELETE FROM SYSTEM.set_permissions WHERE TRUE;")
        self.db_conn.commit()

        self.cur.execute("DELETE FROM SYSTEM.datasets WHERE TRUE;")
        self.db_conn.commit()

        self.cur.execute("DELETE FROM SYSTEM.user_accounts WHERE TRUE;");
        self.db_conn.commit()
        #### CLEAN
    # ENDTEST

    def test_deletePermission(self):
        """Test delete statements on system.set_permissions."""
        #### SETUP
        # insert user
        self.dict_cur.execute("INSERT INTO system.user_accounts(fname,lname,email,passwd) VALUES (%s,%s,%s,%s) RETURNING userid;", ['Jan', 'Met de pet', 'email@adres.com', 'password123'])
        self.db_conn.commit()
        userid_1 = self.dict_cur.fetchone()['userid']

        # insert user
        self.dict_cur.execute("INSERT INTO system.user_accounts(fname,lname,email,passwd) VALUES (%s,%s,%s,%s) RETURNING userid;", ['Peter', 'Selie', 'some_email@adres.com', 'abcdef123'])
        self.db_conn.commit()
        userid_2 = self.dict_cur.fetchone()['userid']

        # insert dataset
        self.dict_cur.execute("INSERT INTO system.datasets(setname, description) VALUES (%s, %s) RETURNING setid;", ['Set A', 'Some dataset.'])
        self.db_conn.commit()
        setid = self.dict_cur.fetchone()['setid']

        # insert permission
        self.dict_cur.execute("INSERT INTO system.set_permissions(userid, setid, permission_type) VALUES (%s,%s,%s);", [userid_1, setid, 'admin'])
        self.db_conn.commit()

        # insert permission
        self.dict_cur.execute("INSERT INTO system.set_permissions(userid, setid, permission_type) VALUES (%s,%s,%s);", [userid_2, setid, 'write'])
        self.db_conn.commit()
        #### SETUP

        #### TEST

        self.query_man.deletePermission(userid=userid_2, setid=setid)

        self.dict_cur.execute("SELECT * FROM system.set_permissions WHERE setid=%s AND userid=%s;", [setid, userid_2])
        results = self.dict_cur.fetchall()

        self.assertEqual(len(results), 0)

        #### TEST

        #### CLEAN
        self.cur.execute("DELETE FROM SYSTEM.set_permissions WHERE TRUE;")
        self.db_conn.commit()

        self.cur.execute("DELETE FROM SYSTEM.datasets WHERE TRUE;")
        self.db_conn.commit()

        self.cur.execute("DELETE FROM SYSTEM.user_accounts WHERE TRUE;");
        self.db_conn.commit()
        #### CLEAN
    # ENDTEST

    def test_updateUser(self):
        """Test update statements on system.user_accounts."""
        
        #### SETUP
        self.dict_cur.execute("INSERT INTO system.user_accounts(fname, lname, email, passwd) VALUES (%s, %s, %s, %s) RETURNING userid;", ['Peter', 'Selie', 'peter.selie@email.com', 'password123'])
        self.db_conn.commit()
        user1_id = self.dict_cur.fetchone()['userid']

        self.dict_cur.execute("INSERT INTO system.user_accounts(fname, lname, email, passwd, admin) VALUES (%s, %s, %s, %s, %s) RETURNING userid;", ['Jan', 'Met de pet', 'jan.met.de@pet.com', 'abcdef123', True])
        self.db_conn.commit()
        user2_id = self.dict_cur.fetchone()['userid']
        #### SETUP

        #### TEST
        self.dict_cur.execute("SELECT fname FROM system.user_accounts WHERE userid=%s;", [user1_id])
        res = self.dict_cur.fetchone()

        self.assertEqual(res['fname'], 'Peter')

        self.query_man.updateUser(reqs={'userid': user1_id}, sets={'fname': 'Pieter'})

        self.dict_cur.execute("SELECT fname FROM system.user_accounts WHERE userid=%s;", [user1_id])
        res = self.dict_cur.fetchone()

        self.assertEqual(res['fname'], 'Pieter')
        #### TEST 2
        self.dict_cur.execute("SELECT admin FROM system.user_accounts WHERE userid=%s;", [user2_id])
        res = self.dict_cur.fetchone()

        self.assertEqual(res['admin'], True)

        self.query_man.updateUser(reqs={'fname': "Jan"}, sets={'admin': False})

        self.dict_cur.execute("SELECT admin FROM system.user_accounts WHERE userid=%s;", [user2_id])
        res = self.dict_cur.fetchone()

        self.assertEqual(res['admin'], False)
        #### TEST

        #### CLEAN
        self.cur.execute("DELETE FROM SYSTEM.set_permissions WHERE TRUE;")
        self.db_conn.commit()

        self.cur.execute("DELETE FROM SYSTEM.datasets WHERE TRUE;")
        self.db_conn.commit()

        self.cur.execute("DELETE FROM SYSTEM.user_accounts WHERE TRUE;");
        self.db_conn.commit()
        #### CLEAN
    # ENDTEST

    def test_updateDataset(self):
        """Test update statements on system.datasets."""
        #### SETUP
        self.dict_cur.execute("INSERT INTO system.datasets(setname, description) VALUES (%s, %s) RETURNING setid;", ['A', 'Dataset A'])
        self.db_conn.commit()
        dataset1_id = self.dict_cur.fetchone()['setid']

        #### SETUP

        self.dict_cur.execute("SELECT setname FROM system.datasets WHERE setid=%s;", [dataset1_id])
        res = self.dict_cur.fetchone()

        self.assertEqual(res['setname'], "A")

        self.query_man.updateDataset(reqs={'setid': dataset1_id}, sets={'setname': 'ABC'})

        self.dict_cur.execute("SELECT setname FROM system.datasets WHERE setid=%s;", [dataset1_id])
        res = self.dict_cur.fetchone()

        self.assertEqual(res['setname'], "ABC")

        #### CLEAN
        self.cur.execute("DELETE FROM SYSTEM.set_permissions WHERE TRUE;")
        self.db_conn.commit()

        self.cur.execute("DELETE FROM SYSTEM.datasets WHERE TRUE;")
        self.db_conn.commit()

        self.cur.execute("DELETE FROM SYSTEM.user_accounts WHERE TRUE;");
        self.db_conn.commit()
        #### CLEAN
    # ENDTEST









