import unittest

class TestCase(unittest.TestCase):
    def test_1(self):
        # Mimir Test Case begins here

        from Project2.project import connect
        conn = connect("test.db")
        conn.execute("CREATE TABLE students (col1 INTEGER, col2 TEXT, col3 REAL);")
        conn.execute("INSERT INTO students VALUES (3, 'hi', 4.5);")
        conn.execute("INSERT INTO students VALUES (7842, 'string with spaces', 3.0);")
        conn.execute("INSERT INTO students VALUES (7, 'look a null', NULL);")
        result = conn.execute("SELECT col1, col2, col3 FROM students ORDER BY col1;")
        result_list = list(result)

        expected = [(3, 'hi', 4.5), (7, 'look a null', None), (7842, 'string with spaces', 3.0)]

        print("expected:",  expected)
        print("student: ",  result_list)
        assert expected == result_list

        # Mimir Test Case ends here

if __name__ == "__main__":
    unittest.main()